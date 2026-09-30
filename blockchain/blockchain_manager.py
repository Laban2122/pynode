# BlockchainManager lives here
import asyncio
import struct
from io import BytesIO
from core.types import DataMessage, BMRequest
from core.helpers import read_varint, write_varint, parse_getheaders_message, encode_varint
from blockchain.headerchain import HeaderChain
from blockchain.blockchain import BlockChain
from blockchain.fork_handler import ForkHandler, ReorgDecision
from core.config import FORK_HANDLER_MAX_DEPTH, ENABLE_TX_VALIDATION


def _validate_one_sync(raw_hex: str, tx_index, block_chain) -> str:
    from blockchain.tx_validator import validate_txs
    return validate_txs([raw_hex], tx_index, block_chain)[0]['tag']


class BlockchainManager:
    """Pure Bitcoin logic - validates blocks, headers, transactions (peer-blind)"""
    
    def __init__(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue):
        self.in_q = in_queue
        self.out_q = out_queue
        
        # Initialize HeaderChain for managing headers
        self.header_chain = HeaderChain("headers.dat")
        print(f"[BM] HeaderChain initialized with {self.header_chain.get_height() + 1} headers")

        # Fail fast with a clear message if the chain loaded empty. The genesis is
        # always seeded on a fresh start, so an empty chain means headers.dat is
        # corrupt or belongs to a different chain than the configured genesis
        # (the common pitfall when repointing the node at an alternative chain).
        if self.header_chain.get_height() < 0:
            raise RuntimeError(
                "HeaderChain is empty after load. headers.dat does not start with the "
                "configured genesis (protocol.genesis_header). Delete/replace headers.dat "
                "for the target chain, or fix the configured genesis."
            )
        
        # Initialize BlockChain for storing full blocks
        self.block_chain = BlockChain("blocks.dat", "blocks.idx")
        print(f"[BM] BlockChain initialized with {self.block_chain.get_block_count()} blocks")
        
        # Initialize ForkHandler (Phase 4.3)
        # Use current chain tip as genesis for fork tracking
        tip_hash = self.header_chain.get_tip_hash()
        tip_height = self.header_chain.get_height()
        
        # Initialize with a block deep enough to handle forks
        # Use tip-100 as genesis to give fork handler context for earlier branches
        fork_genesis_height = max(0, tip_height - FORK_HANDLER_MAX_DEPTH)
        fork_genesis_hash = self.header_chain._get_header_hash(self.header_chain.headers[fork_genesis_height])
        
        self.fork_handler = ForkHandler(
            genesis_hash=fork_genesis_hash,
            genesis_height=fork_genesis_height,
            max_depth=FORK_HANDLER_MAX_DEPTH
        )
        print(f"[BM] ForkHandler initialized at height {fork_genesis_height} (tip: {tip_height})")
        
        # Prime fork handler with recent headers so it knows the mainchain
        print(f"[BM] Priming fork handler with {tip_height - fork_genesis_height} recent headers...")
        for height in range(fork_genesis_height + 1, tip_height + 1):
            header = self.header_chain.headers[height]
            # Feed to fork handler - should connect sequentially
            self.fork_handler.on_header(header)
        print(f"[BM] Fork handler primed with mainchain up to #{tip_height}")
        
        # Mempool (Phase 5.1)
        self.mempool = {}  # txid -> {'raw': bytes, 'timestamp': float, 'peer_id': int, 'tag': str|None}
        self.tx_index = None  # set by pynode.py after TxIndex is created
        self.val_valid   = 0
        self.val_partial = 0
        self.val_invalid = 0
        
        # Orphan block storage (Phase 4.3 enhancement)
        self.orphan_blocks = {}  # block_hash -> payload (max 100 blocks)

    async def _create_headers_response(self, getheaders_request: dict) -> bytes:
        """Create headers message response to a getheaders request"""
        locator_hashes = getheaders_request['locator_hashes']
        stop_hash = getheaders_request['stop_hash']
        
        # Find the best matching hash in our chain
        start_index = 0
        end_index = 0
        
        if not locator_hashes:
            headers_to_send = []
        else:
            first_hash = locator_hashes[0]
            if first_hash not in self.header_chain.index_by_hash:
                # We don't have their latest hash - send empty
                headers_to_send = []
                print(f"[BM] ⚠️  Peer's latest hash not in our chain")
            else:
                # Start from the block after their latest
                start_index = self.header_chain.index_by_hash[first_hash] + 1
                end_index = min(start_index + 2000, self.header_chain.get_height() + 1)
                
                # Check stop hash
                if stop_hash != b"\x00" * 32 and stop_hash in self.header_chain.index_by_hash:
                    stop_index = self.header_chain.index_by_hash[stop_hash]
                    if stop_index < end_index and stop_index >= start_index:
                        end_index = stop_index + 1  # Include the stop hash header
                
                headers_to_send = self.header_chain.headers[start_index:end_index]
        
        # Build headers message payload
        payload = encode_varint(len(headers_to_send))
        
        # Enhanced logging
        if headers_to_send:
            import hashlib
            first_header = headers_to_send[0]
            first_hash = hashlib.sha256(hashlib.sha256(first_header).digest()).digest()
            first_hash_hex = first_hash[::-1].hex()  # Reverse for display
            print(f"[BM] 📤 Sending {len(headers_to_send)} headers to peer - starting with {first_hash_hex[:6]}...{first_hash_hex[-6:]} - height {start_index}")
        else:
            print(f"[BM] 📤 Sending 0 headers to peer")
        
        for header in headers_to_send:
            payload += header  # 80-byte header
            payload += b"\x00"  # Transaction count (always 0 for headers message)
        
        return payload

    async def _process_getheaders(self, data_msg: DataMessage):
        """Process getheaders request from peer"""
        peer_id = data_msg.peer_id
        payload = data_msg.payload
        
        print(f"[BM] 📥 Received getheaders from peer {peer_id}")
        
        # Parse the request
        parsed_request = parse_getheaders_message(payload)
        
        # Create response
        response_payload = await self._create_headers_response(parsed_request)
        
        # Send response back via NetworkManager
        request = BMRequest(
            type="send_headers",
            data={
                "peer_id": peer_id,
                "payload": response_payload
            }
        )
        await self.out_q.put(request)

    async def _request_headers(self):
        """Request headers from NetworkManager using current chain tip"""
        locator = self.header_chain.get_locator()
        
        # Build getheaders payload
        stream = BytesIO()
        stream.write(struct.pack("<i", 70015))  # Protocol version
        write_varint(stream, len(locator))
        
        for h in locator:
            # Locator hashes are in LE (internal byte order)
            if isinstance(h, str):
                h = bytes.fromhex(h)
            stream.write(h)  # Keep LE format for wire protocol
        
        stream.write(b"\x00" * 32)  # Stop hash (all zeros = get up to 2000 headers)
        
        # Display info
        tip_hash = locator[0] if locator else b"\x00" * 32
        tip_hex = tip_hash[::-1].hex()
        tip_display = f"{tip_hex[:6]}.....{tip_hex[-6:]}"
        print(f"[BM] 📤 Requesting headers from tip: {tip_display}")
        
        # Send request to NetworkManager
        request = BMRequest(
            type="need_headers",
            data={
                "payload": stream.getvalue()
            }
        )
        await self.out_q.put(request)

    async def _execute_reorg(self, reorg: ReorgDecision):
        """
        Execute a chain reorganization (Phase 4.3).
        
        Steps:
        1. Call HeaderChain.reorg() with new_tail (truncates + appends)
        2. Emit reorg event to NetworkManager
        3. Backfiller will automatically detect missing blocks and download them
        """
        old_tip_hex = reorg.old_tip_hash[::-1].hex()
        new_tip_hex = reorg.new_tip_hash[::-1].hex()
        old_height = self.header_chain.get_height()
        
        print(f"[BM] 🔀 REORG EXECUTING: fork at height {reorg.fork_height}")
        print(f"     Old tip: {old_tip_hex[:6]}.....{old_tip_hex[-6:]} (height {old_height})")
        print(f"     New tip: {new_tip_hex[:6]}.....{new_tip_hex[-6:]}")
        print(f"     Replacing {old_height - reorg.fork_height} headers with {len(reorg.new_tail)} new headers")
        
        # Execute header reorg - this truncates headers.dat and appends new chain
        self.header_chain.reorg(reorg.new_tail)
        
        new_height = self.header_chain.get_height()
        new_tip = self.header_chain.get_tip_hash()
        print(f"[BM] ✅ REORG COMPLETE: new tip at height {new_height}")
        
        # Emit reorg event to NetworkManager
        # Backfiller will detect new hashes in HeaderChain and download blocks
        request = BMRequest(
            type="reorg_detected",
            data={
                "fork_height": reorg.fork_height,
                "old_tip": reorg.old_tip_hash[::-1].hex(),
                "new_tip": reorg.new_tip_hash[::-1].hex(),
                "headers_added": len(reorg.new_tail)
            }
        )
        await self.out_q.put(request)

    async def _process_headers(self, data_msg: DataMessage):
        """Process headers message from NetworkManager"""
        peer_id = data_msg.peer_id
        payload = data_msg.payload
        
        # Parse raw headers from payload (80 bytes each, LE/internal byte order)
        stream = BytesIO(payload)
        count = read_varint(stream)
        
        if count == 0:
            print(f"[BM] ⚠️  Peer {peer_id} sent zero headers")
            return
        
        print(f"[BM] 📋 Received {count} headers from peer {peer_id}")
        
        # Collect raw 80-byte headers
        headers = []
        for i in range(count):
            raw = stream.read(80)
            if len(raw) != 80:
                print(f"[BM] ⚠️  Incomplete header at index {i}")
                return
            
            headers.append(raw)
            
            # Read transaction count (always 0 in headers message)
            txn_count = read_varint(stream)
        
        # Phase 4.3: Feed headers to ForkHandler for reorg detection
        reorg_decision = None
        for header_bytes in headers:
            decision = self.fork_handler.on_header(header_bytes)
            if decision:
                reorg_decision = decision
                # Execute reorg immediately when detected
                await self._execute_reorg(reorg_decision)
        
        # Add headers to HeaderChain (linear mainchain)
        accepted_count = self.header_chain.add_headers(headers)
        
        # Check if any newly added headers have orphan blocks waiting
        if accepted_count > 0 and self.orphan_blocks:
            await self._process_orphan_blocks_cache()
        
        # Summary
        print(f"[BM] ✅ Accepted {accepted_count}/{count} headers")
        if accepted_count < count:
            print(f"[BM] ⚠️  {count - accepted_count} headers rejected (chain break or duplicate)")
        
        # Display current chain state with block number
        current_height = self.header_chain.get_height()
        tip_hash = self.header_chain.get_tip_hash()
        tip_hash_display = tip_hash[::-1].hex()
        print(f"[BM] 📊 Chain tip: Block #{current_height} ({tip_hash_display[:6]}.....{tip_hash_display[-6:]})")
        
        if accepted_count > 0 and count < 2000:
            print(f"[BM] 🆕 New block(s) received!")

    async def _store_block(self, block_hash: bytes, block_payload: bytes, peer_id: int):
        """Store full block to disk (Phase 4.2)"""
        try:
            # Check if we already have this block
            if self.block_chain.has_block(block_hash):
                print(f"[BM] ℹ️  Block already stored on disk")
                return
            
            # Get the height from header chain
            if block_hash in self.header_chain.index_by_hash:
                height = self.header_chain.index_by_hash[block_hash]
            else:
                print(f"[BM] ⚠️  Cannot store block: header not in chain")
                return
            
            # Store the block
            success = self.block_chain.add_block(block_payload, height=height)
            
            if success:
                if hasattr(self, 'tx_index') and self.tx_index:
                    self.tx_index.add_block(height, block_payload)
            else:
                print(f"[BM] ❌ Failed to store block to disk")
                
        except Exception as e:
            print(f"[BM] ❌ Error storing block: {e}")
            import traceback
            traceback.print_exc()

    async def _process_orphan_blocks_cache(self):
        """
        Check if any cached orphan blocks can now be stored (Phase 4.3 enhancement).
        Called after headers are added to HeaderChain.
        """
        if not self.orphan_blocks:
            return
        
        resolved_hashes = []
        
        # Check each orphan block to see if we now have its header
        for block_hash, payload in self.orphan_blocks.items():
            if block_hash in self.header_chain.index_by_hash:
                # Header now exists - we can store this block
                block_hash_hex = block_hash[::-1].hex()
                print(f"[BM] ✅ Orphan block resolved: {block_hash_hex[:6]}.....{block_hash_hex[-6:]}")
                
                # Store the block
                await self._store_block(block_hash, payload, peer_id=0)
                
                # Remove mempool transactions
                await self._remove_mempool_txs(payload)
                
                # Mark for removal
                resolved_hashes.append(block_hash)
        
        # Remove resolved orphans from cache
        for block_hash in resolved_hashes:
            del self.orphan_blocks[block_hash]
        
        if resolved_hashes:
            print(f"[BM] 🧹 Removed {len(resolved_hashes)} resolved orphan(s) from cache")
            print(f"[BM] 📊 Orphan cache: {len(self.orphan_blocks)} block(s) remaining")

    async def _process_block(self, data_msg: DataMessage):
        """Process block message from NetworkManager"""
        peer_id = data_msg.peer_id
        payload = data_msg.payload
        
        try:
            from core.helpers import parse_block_message
            import hashlib
            
            # Parse block
            block_data = parse_block_message(payload)
            header = block_data['header']
            tx_count = block_data['tx_count']
            
            # Calculate block hash (double SHA256 of header)
            block_hash = hashlib.sha256(hashlib.sha256(header).digest()).digest()
            block_hash_hex = block_hash[::-1].hex()  # Reverse for display
            
            # Check if we already have this BLOCK stored (not just header)
            if self.block_chain.has_block(block_hash):
                # print(f"[BM] ℹ️  Block {block_hash_hex[:6]}... already stored, ignoring")
                return

            # Feature 1: Timestamp for new block
            # import datetime
            # now = datetime.datetime.now()
            # utc_now = datetime.datetime.utcnow()
            # print(f"[BM] 🕒 New Block Received at Local: {now} | UTC: {utc_now}")

            # print(f"[BM] 📦 Received block from peer {peer_id}")
            # print(f"     Hash: {block_hash_hex[:6]}.....{block_hash_hex[-6:]}")
            # print(f"     Transactions: {tx_count}")
            pass
            
            # Extract previous block hash from header (bytes 4-36)
            prev_hash = header[4:36]
            
            # Check if block connects to our chain tip
            current_tip = self.header_chain.get_tip_hash()
            
            if prev_hash == current_tip:
                # Check if we already have this header
                if block_hash in self.header_chain.index_by_hash:
                    print(f"[BM] ℹ️  Header already in chain")
                    header_exists = True
                else:
                    # Add header to chain
                    added = self.header_chain.add_headers([header])
                    if added > 0:
                        # Keep ForkHandler in sync with HeaderChain tip
                        self.fork_handler.on_header(header)
                        
                        new_height = self.header_chain.get_height()
                        new_tip_display = block_hash[::-1].hex()
                        print(f"[BM] ✅ Block #{new_height} added to chain tip ({new_tip_display[:6]}.....{new_tip_display[-6:]})")
                        header_exists = False
                        
                        # Request relay of this block
                        request = BMRequest(
                            type="relay_block",
                            data={
                                "hash": block_hash,
                                "payload": payload
                            }
                        )
                        await self.out_q.put(request)
                    else:
                        print(f"[BM] ⚠️  Failed to add header to chain")
                        return
                
                # Store full block to disk (Phase 4.2)
                await self._store_block(block_hash, payload, peer_id)
                
                # Remove confirmed transactions from mempool (Phase 5.1)
                await self._remove_mempool_txs(payload)
                
            else:
                # Block doesn't connect to our tip - check if it's a backfill block
                prev_hash_hex = prev_hash[::-1].hex()
                tip_hash_hex = current_tip[::-1].hex()
                
                # Check if we have the header for this block (backfill case)
                if block_hash in self.header_chain.index_by_hash:
                    # print(f"[BM] 📚 Backfill block (header exists, below tip)")
                    # print(f"     Hash: {block_hash_hex[:6]}.....{block_hash_hex[-6:]}")
                    
                    # Store full block to disk
                    await self._store_block(block_hash, payload, peer_id)
                    
                    # Remove confirmed transactions from mempool (Phase 5.1)
                    await self._remove_mempool_txs(payload)
                elif prev_hash in self.header_chain.index_by_hash:
                    # FORK: parent is in our chain but block doesn't extend tip
                    fork_height = self.header_chain.index_by_hash[prev_hash]
                    tip_height = self.header_chain.get_height()
                    depth = tip_height - fork_height
                    print(f"[BM] 🔀 Fork block detected (branches from height {fork_height}, depth {depth})")
                    print(f"     Fork block: {block_hash_hex[:6]}.....{block_hash_hex[-6:]}")
                    print(f"     Fork parent: {prev_hash_hex[:6]}.....{prev_hash_hex[-6:]} (height {fork_height})")
                    print(f"     Main tip:    {tip_hash_hex[:6]}.....{tip_hash_hex[-6:]} (height {tip_height})")
                    
                    # Feed header to fork handler for work comparison
                    decision = self.fork_handler.on_header(header)
                    
                    if decision:
                        print(f"[BM] 🔀 Fork has more work — executing reorg")
                        await self._execute_reorg(decision)
                    else:
                        print(f"[BM] 📊 Fork tracked (less work than main chain, watching)")
                    
                    # Cache block payload for storage after potential reorg
                    if len(self.orphan_blocks) < 100:
                        self.orphan_blocks[block_hash] = payload
                else:
                    # TRUE ORPHAN: parent hash unknown — not in our chain at all
                    print(f"[BM] ⛔ ORPHAN block! Parent unknown — not a fork")
                    print(f"     Block:      {block_hash_hex[:6]}.....{block_hash_hex[-6:]}")
                    print(f"     Block prev: {prev_hash_hex[:6]}.....{prev_hash_hex[-6:]}")
                    print(f"     Our tip:    {tip_hash_hex[:6]}.....{tip_hash_hex[-6:]}")
                    
                    # Feed header to fork handler (may connect later via orphan resolution)
                    decision = self.fork_handler.on_header(header)
                    
                    if decision:
                        print(f"[BM] 🔀 Orphan resolved a fork — executing reorg")
                        await self._execute_reorg(decision)
                    
                    # Cache block payload for later resolution
                    if len(self.orphan_blocks) < 100:
                        self.orphan_blocks[block_hash] = payload
                        print(f"[BM] 💾 Cached orphan block ({len(self.orphan_blocks)} in memory)")
                    else:
                        print(f"[BM] ⚠️  Orphan cache full (100 blocks), discarding")
                    
                    # Request headers to discover the missing chain
                    await self._request_headers()
                
        except Exception as e:
            print(f"[BM] ❌ Error processing block: {e}")
            import traceback
            traceback.print_exc()

    async def _process_tx(self, data_msg: DataMessage):
        """Process transaction message from NetworkManager"""
        peer_id = data_msg.peer_id
        payload = data_msg.payload
        
        # Calculate txid
        from core.helpers import calculate_txid
        import time
        
        txid = calculate_txid(payload)
        
        # Check if already in mempool
        if txid in self.mempool:
            # Duplicate - silent ignore (very common during sync)
            return
        
        # Validate before storing so tag is ready
        tag = None
        if ENABLE_TX_VALIDATION and self.tx_index is not None and self.tx_index.ready:
            loop = asyncio.get_running_loop()
            tag = await loop.run_in_executor(
                None, _validate_one_sync, payload.hex(), self.tx_index, self.block_chain
            )
            if tag == 'valid':
                self.val_valid += 1
            elif tag == 'partial':
                self.val_partial += 1
            else:
                self.val_invalid += 1

        # Store in mempool
        self.mempool[txid] = {
            'raw': payload,
            'timestamp': time.time(),
            'peer_id': peer_id,
            'tag': tag,
        }

        # Log progress every 200 transactions
        if len(self.mempool) % 200 == 0:
            if ENABLE_TX_VALIDATION and self.tx_index is not None and self.tx_index.ready:
                print(f"[BM] Mempool: {len(self.mempool)} txs "
                      f"({self.val_valid} valid / {self.val_partial} partial / {self.val_invalid} invalid)")
            else:
                print(f"[BM] Mempool size: {len(self.mempool)} transactions")
        
        # Notify NetworkManager that TX is stored (for seen_txids tracking)
        request = BMRequest(
            type="tx_stored",
            data={"txid": txid}
        )
        await self.out_q.put(request)
    
    async def _process_getdata(self, data_msg: DataMessage):
        """Process getdata request - serve blocks we have or respond with notfound"""
        peer_id = data_msg.peer_id
        payload = data_msg.payload
        
        try:
            from io import BytesIO
            from core.helpers import read_varint
            import struct
            
            stream = BytesIO(payload)
            count = read_varint(stream)
            
            print(f"[BM] 📥 Received getdata from peer {peer_id}: {count} items")
            
            blocks_to_send = []
            not_found_items = []
            
            for i in range(count):
                inv_type = struct.unpack("<I", stream.read(4))[0]
                inv_hash = stream.read(32)
                
                # Check if it's a block request (MSG_BLOCK=2 or MSG_WITNESS_BLOCK=0x40000002)
                if inv_type == 2 or inv_type == 0x40000002:
                    # Check if we have this block
                    if self.block_chain.has_block(inv_hash):
                        block_data = self.block_chain.get_block(inv_hash)
                        if block_data:
                            blocks_to_send.append(block_data)
                            hash_hex = inv_hash[::-1].hex()
                            print(f"[BM]   Found block: {hash_hex[:6]}.....{hash_hex[-6:]}")
                        else:
                            not_found_items.append((inv_type, inv_hash))
                    else:
                        not_found_items.append((inv_type, inv_hash))
                        hash_hex = inv_hash[::-1].hex()
                        print(f"[BM]   Block not in storage: {hash_hex[:6]}.....{hash_hex[-6:]}")
                else:
                    # TX or other type - we don't serve these yet
                    not_found_items.append((inv_type, inv_hash))
            
            # Send blocks we have
            for block_data in blocks_to_send:
                request = BMRequest(
                    type="send_block",
                    data={
                        "peer_id": peer_id,
                        "payload": block_data
                    }
                )
                await self.out_q.put(request)
            
            # Send notfound for items we don't have
            if not_found_items:
                # Build notfound payload
                notfound_payload = bytes([len(not_found_items)])  # Count as varint
                for inv_type, inv_hash in not_found_items:
                    notfound_payload += struct.pack("<I", inv_type)
                    notfound_payload += inv_hash
                
                request = BMRequest(
                    type="send_notfound",
                    data={
                        "peer_id": peer_id,
                        "payload": notfound_payload
                    }
                )
                await self.out_q.put(request)
                print(f"[BM] 📤 Sending notfound for {len(not_found_items)} items")
            
            if blocks_to_send:
                print(f"[BM] 📤 Sending {len(blocks_to_send)} blocks to peer {peer_id}")
                
        except Exception as e:
            print(f"[BM] ❌ Error processing getdata: {e}")
            import traceback
            traceback.print_exc()

    async def _handle_data_messages(self):
        """Process data messages from NetworkManager"""
        print("[BM] Starting data message handler")
        print("[BM] Waiting for data messages from NetworkManager...")
        
        while True:
            try:
                data_msg = await self.in_q.get()
                # print(f"[BM] ⬅️  Received {data_msg.msg_type} message from NM")
                
                if not isinstance(data_msg, DataMessage):
                    print(f"[BM] ⚠️  Received non-DataMessage: {type(data_msg)}")
                    continue
                
                # Route to appropriate handler
                if data_msg.msg_type == "headers":
                    await self._process_headers(data_msg)
                elif data_msg.msg_type == "getheaders":
                    await self._process_getheaders(data_msg)
                elif data_msg.msg_type == "getdata":
                    await self._process_getdata(data_msg)
                elif data_msg.msg_type == "block":
                    await self._process_block(data_msg)
                elif data_msg.msg_type == "tx":
                    await self._process_tx(data_msg)
                else:
                    print(f"[BM] ❓ Unknown message type: {data_msg.msg_type}")
                
            except asyncio.CancelledError:
                print("[BM] Data message handler cancelled")
                raise
            except Exception as e:
                print(f"[BM] ❌ Error processing data message: {e}")
                import traceback
                traceback.print_exc()

    async def run(self):
        """Main BlockchainManager loop"""
        print("[BM] Starting BlockchainManager (Phase 4.1: HeaderChain integration)")
        print(f"[BM] Current chain height: {self.header_chain.get_height()}")
        
        # Start mempool cleanup task (Phase 5.1)
        cleanup_task = asyncio.create_task(self._mempool_cleanup_task())
        
        try:
            # Run data message handler
            await self._handle_data_messages()
        except asyncio.CancelledError:
            print("[BM] Shutting down...")
            cleanup_task.cancel()
            await asyncio.gather(cleanup_task, return_exceptions=True)
            raise
    
    async def _mempool_cleanup_task(self):
        """Background task to remove stale transactions (>  4h old)"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                import time
                now = time.time()
                stale_txids = [
                    txid for txid, data in self.mempool.items()
                    if now - data['timestamp'] > 14400  # 4 hours
                ]
                
                for txid in stale_txids:
                    tag = self.mempool[txid].get('tag')
                    if tag == 'valid':
                        self.val_valid -= 1
                    elif tag == 'partial':
                        self.val_partial -= 1
                    elif tag == 'invalid':
                        self.val_invalid -= 1
                    del self.mempool[txid]

                if stale_txids:
                    print(f"[BM] 🧹 Cleaned up {len(stale_txids)} stale transactions")
                    print(f"[BM] Mempool size: {len(self.mempool)} transactions")
                    
            except asyncio.CancelledError:
                print("[BM] Mempool cleanup task cancelled")
                raise
            except Exception as e:
                print(f"[BM] ⚠️  Mempool cleanup error: {e}")
    
    async def _remove_mempool_txs(self, block_payload: bytes):
        """Remove confirmed transactions from mempool after block arrival"""
        try:
            from core.helpers import parse_block_txs
            
            # Parse all txids from block
            txids = parse_block_txs(block_payload)
            
            # Remove from mempool
            removed_count = 0
            for txid in txids:
                if txid in self.mempool:
                    tag = self.mempool[txid].get('tag')
                    if tag == 'valid':
                        self.val_valid -= 1
                    elif tag == 'partial':
                        self.val_partial -= 1
                    elif tag == 'invalid':
                        self.val_invalid -= 1
                    del self.mempool[txid]
                    removed_count += 1
            
            if removed_count > 0:
                print(f"[BM] 🧹 Removed {removed_count} transactions from mempool, {len(self.mempool)} remaining")
                
        except Exception as e:
            print(f"[BM] ⚠️  Failed to remove block TXs from mempool: {e}")
