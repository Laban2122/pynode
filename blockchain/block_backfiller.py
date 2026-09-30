# Block Backfiller - Maintenance layer for ensuring block completeness
import asyncio
import time
import struct
import hashlib
from typing import List, Optional
from core.types import BMRequest


class BlockBackfiller:
    """
    Ensures blockchain completeness by backfilling missing blocks.
    
    Operates independently of BlockchainManager validation logic.
    Uses same communication queues as BM - NetworkManager routes all messages.
    
    Architecture:
    - Detects gaps by comparing HeaderChain (should exist) vs BlockChain (does exist)
    - Requests missing blocks via shared queue to NetworkManager
    - Polls periodically to check if gaps filled
    - No direct communication with BM - shares state through block storage
    """
    
    def __init__(self, out_queue: asyncio.Queue, header_chain, block_chain, start_height_config: int = -1, get_peer_count_func=None, backfill_done: asyncio.Event = None):
        """
        Args:
            out_queue: Queue to send block requests to NM (shared with BM)
            header_chain: Read-only access to HeaderChain (what should exist)
            block_chain: Read-only access to BlockChain (what does exist)
            start_height_config: Starting height for backfilling (-1=auto, 0+=explicit)
            get_peer_count_func: Callback to get current peer count
        """
        self.out_q = out_queue
        self.header_chain = header_chain  # Read-only reference
        self.block_chain = block_chain    # Read-only reference
        self.start_height_config = start_height_config
        self.get_peer_count_func = get_peer_count_func or (lambda: 0)
        
        # Backfiller state
        self.poll_interval = 300    # Check for gaps every 5 minutes
        
        # Statistics
        self.total_requested = 0

        # Event set when a full scan finds zero missing blocks and headers are current
        self.backfill_done = backfill_done
    
    def _is_headers_current(self) -> bool:
        """Return True if the tip header timestamp is within 2 hours of wall time.

        This distinguishes a header chain loaded from disk (potentially days old)
        from one that has actually caught up with the network.
        """
        tip_height = self.header_chain.get_height()
        if tip_height < 0 or tip_height >= len(self.header_chain.headers):
            return False
        tip_header = self.header_chain.headers[tip_height]
        if len(tip_header) < 72:
            return False
        tip_ts = struct.unpack('<I', tip_header[68:72])[0]
        return (time.time() - tip_ts) < 7200

    def _double_sha256(self, data: bytes) -> bytes:
        """Bitcoin double SHA256 hash"""
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()
    
    def _find_missing_blocks(self) -> List[bytes]:
        """
        Find all missing blocks by comparing headers vs stored blocks.
        
        Performs FULL SCAN every cycle to catch blocks that were:
        - Requested but never delivered
        - Lost due to peer disconnections
        - Stuck in backfill_pending state
        
        Returns:
            List of block hashes that have headers but no stored block
        """
        scan_start_time = time.time()
        missing = []
        
        # Get current header chain height
        header_height = self.header_chain.get_height()
        
        if header_height < 0:
            # No headers yet
            return missing
        
        # Determine start height
        if self.start_height_config >= 0:
            # Explicit start height from config
            start_height = self.start_height_config
        elif self.block_chain.get_block_count() > 0:
            # Auto-detect from first stored block
            start_height = self.block_chain.get_first_block_height()
        else:
            # No blocks yet, wait for next cycle
            return missing
        
        # End height: backfill up to tip (reorg handling deferred to Phase 4.3)
        end_height = header_height
        
        if end_height < start_height:
            # Not enough headers yet
            return missing
        
        # FULL SCAN: Always scan complete range (no incremental position tracking)
        # This ensures blocks stuck in pending state are rediscovered
        scan_start = start_height
        scan_count = end_height - scan_start + 1
        
        # Check each height from scan_start to end_height
        for height in range(scan_start, end_height + 1):
            # Get header at this height
            if height >= len(self.header_chain.headers):
                # Height beyond our headers
                break
            
            header = self.header_chain.headers[height]
            block_hash = self._double_sha256(header)
            
            # Check if we have the block (checks storage, not pending requests)
            if not self.block_chain.has_block(block_hash):
                missing.append(block_hash)
        
        # Measure and report scan performance. Guard the rate: a tiny scan range
        # (e.g. a single stored block near the tip) finishes within the OS timer
        # resolution, making scan_duration 0.0 — the rate is cosmetic only.
        scan_duration = time.time() - scan_start_time
        rate = (scan_count / scan_duration) if scan_duration > 0 else 0
        print(f"\n[Block Backfiller] 🔍 Full scan complete: {scan_count} heights checked in {scan_duration:.2f}s ({rate:.0f} heights/s), {len(missing)} missing")

        return missing
    
    async def _request_blocks(self, block_hashes: List[bytes]):
        """
        Send all missing blocks to NetworkManager for distribution.
        NM handles batching, peer selection, and retries.
        
        Args:
            block_hashes: Complete list of missing block hashes
        """
        if not block_hashes:
            return
        
        # Build items list for getdata (MSG_BLOCK type)
        items = []
        for block_hash in block_hashes:
            items.append({
                "type": 2,  # MSG_BLOCK
                "hash": block_hash
            })
        
        # Send getdata request to NetworkManager
        # NM will handle batching (16 per peer), distribution, and retries
        request = BMRequest(
            type="send_getdata",
            data={
                "items": items,
                "is_backfill": True
            }
        )
        
        await self.out_q.put(request)
        
        self.total_requested += len(block_hashes)
        
        # Log request summary with height range
        heights = []
        for block_hash in block_hashes:
            if block_hash in self.header_chain.index_by_hash:
                heights.append(self.header_chain.index_by_hash[block_hash])
        
        if heights:
            print(f"[Backfiller] 📤 Requested {len(block_hashes)} blocks (heights {min(heights)}-{max(heights)})")
        else:
            print(f"[Backfiller] 📤 Requested {len(block_hashes)} blocks")
    
    async def _backfill_cycle(self):
        """
        One iteration of the backfill process:
        1. Find all missing blocks
        2. Send complete list to NetworkManager
        
        NetworkManager handles:
        - Batching (16 blocks per peer)
        - Peer selection and distribution  
        - Timeout and retry logic
        
        Returns:
            Number of missing blocks found (0 if none)
        """
        # Find all missing blocks
        missing = self._find_missing_blocks()
        
        if not missing:
            return 0
        
        # Send all missing blocks to NetworkManager
        # NM will distribute across peers and handle retries
        await self._request_blocks(missing)
        return len(missing)
    
    async def _wait_for_startup_conditions(self):
        """
        Wait until node is ready for backfilling:
        - Headers synced (at least 1000 headers)
        - Enough peers connected (at least 5)
        - Blocks/headers exist (unless explicit start height configured)
        """
        print("[Backfiller] Starting block maintenance service")
        
        # Wait until headers have caught up with the network tip.
        # Checking height alone is not enough: headers.dat may already hold hundreds
        # of thousands of headers from a previous run, making the height check pass
        # immediately even though the node hasn't synced new blocks from peers yet.
        # The tip header timestamp tells us whether the chain is actually current.
        while not self._is_headers_current():
            h = self.header_chain.get_height()
            print(f"[Backfiller] 📜 Waiting for headers to catch up to network... ({h} headers, tip not yet current)")
            await asyncio.sleep(10)
        
        # Wait for enough peers
        from core.config import DESIRED_NOO_OUTBOUNDS
        min_peers = min(5, DESIRED_NOO_OUTBOUNDS)
        while self.get_peer_count_func() < min_peers:
            print(f"[Backfiller] 🔌 Waiting for peers... ({self.get_peer_count_func()}/{min_peers} connected)")
            await asyncio.sleep(10)
        
        # Wait for blocks/headers if auto-detect mode
        if self.start_height_config == -1:
            while self.block_chain.get_block_count() == 0:
                print("[Backfiller] 📦 No blocks stored yet, waiting for first block...")
                await asyncio.sleep(10)
        
        # Ready
        header_height = self.header_chain.get_height()
        peer_count = self.get_peer_count_func()
        block_count = self.block_chain.get_block_count()
        print(f"[Backfiller] ✅ Ready - {header_height} headers, {peer_count} peers, {block_count} blocks stored")
    
    async def run(self):
        """
        Main backfiller loop.
        
        Phase 1: Wait for startup conditions
        Phase 2: Continuous monitoring and gap filling
        """
        # Phase 1: Wait for startup conditions
        await self._wait_for_startup_conditions()
        
        # Phase 2: Continuous monitoring
        print(f"[Backfiller] 🔄 Starting continuous monitoring (every {self.poll_interval}s)")
        
        while True:
            # Initialize before the try so a cycle exception can't leave it
            # unbound — the sleep_time line below runs even on the except path,
            # and an UnboundLocalError there would kill the whole backfiller task.
            missing_count = 0
            try:
                missing_count = await self._backfill_cycle()

                # Heartbeat after each cycle
                if missing_count > 0:
                    print(f"[Backfiller] 🔧 Found {missing_count} missing blocks, requested from NetworkManager")
                elif self._is_headers_current():
                    print(f"[Backfiller] 💓 No missing blocks found, blockchain up to date")
                    if self.backfill_done and not self.backfill_done.is_set():
                        self.backfill_done.set()
                        print(f"[Backfiller] ✅ Backfill complete — UTXO build can proceed")
                else:
                    print(f"[Backfiller] 💓 No gaps in current range, headers still syncing — will recheck")
                    
            except Exception as e:
                print(f"[Backfiller] ❌ Error in backfill cycle: {e}")
                import traceback
                traceback.print_exc()
            
            # After an active backfill, rescan soon so we confirm completion
            # promptly once NM finishes delivering blocks (rather than waiting
            # the full 5-minute idle interval).
            sleep_time = 60 if missing_count > 0 else self.poll_interval
            await asyncio.sleep(sleep_time)
