# ForkHandler - tracks competing chains and detects reorgs (Phase 4.3)
"""
Lightweight fork tracking using flat dictionaries.
Maintains multiple competing chain tips and detects when a better chain emerges.

Design Philosophy:
- Simple flat dictionaries for storage (memory efficient)
- Incremental work calculation (O(1) per header)
- Recursive orphan resolution (automatic chain building)
- Only tracks recent history (configurable depth)
"""

from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
import hashlib
import struct
import logging
from core.helpers import parse_compact_bits


@dataclass
class ReorgDecision:
    """Decision to reorganize the blockchain (Phase 4.3)"""
    fork_height: int
    old_tip_hash: bytes
    new_tip_hash: bytes
    new_tail: List[bytes]  # List of 80-byte headers to connect (not hashes!)


class ForkHandler:
    """
    Tracks competing blockchain forks and detects reorganizations.
    
    Core data structures (simplified approach):
    - parent: Dict[hash, hash]              # child -> parent relationship
    - nodes: Dict[hash, bytes]              # hash -> 80-byte header
    - height: Dict[hash, int]               # hash -> height (only if connected)
    - work: Dict[hash, int]                 # hash -> cumulative work
    - orphans_by_parent: Dict[hash, List[hash]]  # parent -> orphaned children
    
    Key invariants:
    - If hash in height, then hash is connected to mainchain
    - If hash not in height, then hash is orphan or not yet received
    - best_tip always points to highest-work chain tip
    """
    
    def __init__(self, genesis_hash: bytes, genesis_height: int, max_depth: int = 100):
        """
        Initialize fork handler.
        
        Args:
            genesis_hash: Hash of the genesis/starting block
            genesis_height: Height of genesis block
            max_depth: Maximum depth to track (older blocks pruned)
        """
        # Core storage (simplified flat dictionaries)
        self.parent: Dict[bytes, bytes] = {}
        self.nodes: Dict[bytes, bytes] = {}  # Store raw 80-byte headers
        self.height: Dict[bytes, int] = {}
        self.work: Dict[bytes, int] = {}
        
        # Orphan tracking
        self.orphans_by_parent: Dict[bytes, List[bytes]] = {}
        
        # Chain state
        self.best_tip: bytes = genesis_hash
        self.branch_tips: Set[bytes] = {genesis_hash}  # All competing tips
        
        # Config
        self.max_depth = max_depth
        
        # Initialize genesis
        self.nodes[genesis_hash] = b"\x00" * 80  # Placeholder for genesis (not actual header)
        self.height[genesis_hash] = genesis_height
        self.work[genesis_hash] = 0  # Genesis has zero work (no PoW before it)
        
        logging.info(f"[ForkHandler] Initialized with genesis {genesis_hash[::-1].hex()[:16]}... at height {genesis_height}")
    
    def on_header(self, header_bytes: bytes) -> Optional[ReorgDecision]:
        """
        Process incoming header.
        
        This is the main entry point. Returns ReorgDecision if reorg needed.
        
        Args:
            header_bytes: 80-byte serialized header
            
        Returns:
            ReorgDecision if reorg needed, None otherwise
        """
        # Parse header
        h = self._compute_hash(header_bytes)
        p = self._parse_prev_hash(header_bytes)
        
        # Skip if already processed
        if h in self.nodes:
            return None
        
        # Store header and parent relationship
        self.parent[h] = p
        self.nodes[h] = header_bytes
        
        # Case 1: Parent is known (has height assigned)
        if p in self.height:
            # Calculate height and work incrementally
            self.height[h] = self.height[p] + 1
            self.work[h] = self.work[p] + self._calculate_header_work(header_bytes)
            
            # Update branch tips
            self.branch_tips.add(h)
            # Remove parent from tips (it's no longer a tip, child is)
            self.branch_tips.discard(p)
            
            # Check if this creates a better chain
            reorg = self._maybe_update_best(h)
            
            # Resolve any orphans waiting for this block
            self._resolve_orphans(h)
            
            # Prune old blocks
            self._prune_old_blocks()
            
            return reorg
        
        # Case 2: Parent unknown -> orphan
        else:
            if p not in self.orphans_by_parent:
                self.orphans_by_parent[p] = []
            self.orphans_by_parent[p].append(h)
            
            orphan_count = sum(len(orphans) for orphans in self.orphans_by_parent.values())
            logging.debug(f"[ForkHandler] Orphan header {h[::-1].hex()[:16]}... waiting for parent {p[::-1].hex()[:16]}... (total orphans: {orphan_count})")
            
            return None
    
    def _maybe_update_best(self, h: bytes) -> Optional[ReorgDecision]:
        """
        Check if block h creates a better chain than current best.
        
        Uses lazy work calculation optimization:
        - Quick height check first (most forks are short)
        - Only compare work if heights are competitive
        
        Returns ReorgDecision if reorg needed.
        """
        best_height = self.height[self.best_tip]
        new_height = self.height[h]
        
        # Optimization: Skip if new chain is too short
        # (Can't have more work if 6+ blocks behind)
        if new_height < best_height - 6:
            return None
        
        # Compare cumulative work
        best_work = self.work[self.best_tip]
        new_work = self.work[h]
        
        # Check if this is actually a different chain (not just extending current best)
        if h == self.best_tip:
            return None  # Same block, no reorg
        
        # Check if this extends the current best chain (no reorg needed)
        if self.parent[h] == self.best_tip:
            # This is the next block on mainchain, just update tip
            self.best_tip = h
            return None
        
        if new_work > best_work or (new_work == best_work and new_height > best_height):
            # New chain wins! Prepare reorg
            logging.info(f"🔀 [ForkHandler] Reorg detected!")
            logging.info(f"   Old tip: {self.best_tip[::-1].hex()[:16]}... height={best_height} work={best_work}")
            logging.info(f"   New tip: {h[::-1].hex()[:16]}... height={new_height} work={new_work}")
            
            reorg = self._prepare_reorg(self.best_tip, h)
            
            # Update best tip
            self.best_tip = h
            
            return reorg
        
        return None
    
    def _prepare_reorg(self, old_tip: bytes, new_tip: bytes) -> ReorgDecision:
        """
        Prepare reorg decision by finding fork point and building new_tail.
        """
        # Find common ancestor (fork point)
        fork_point, old_chain, new_chain = self._find_fork_point(old_tip, new_tip)
        
        logging.info(f"   Fork point: {fork_point[::-1].hex()[:16]}... height={self.height[fork_point]}")
        logging.info(f"   Disconnect: {len(old_chain)} blocks")
        logging.info(f"   Connect: {len(new_chain)} blocks")
        
        # Build new_tail: convert hashes to 80-byte headers
        new_tail = [self.nodes[h] for h in new_chain]
    
        return ReorgDecision(
            fork_height=self.height[fork_point],
            old_tip_hash=old_tip,
            new_tip_hash=new_tip,
            new_tail=new_tail  # 80-byte headers, not hashes
        )
    
    def _find_fork_point(self, hash1: bytes, hash2: bytes) -> Tuple[bytes, List[bytes], List[bytes]]:
        """
        Find common ancestor of two chains.
        
        Returns:
            (fork_point_hash, blocks_from_hash1, blocks_from_hash2)
            
        Note: blocks_from_hash1/2 do NOT include the fork point itself,
        only the blocks that need to be disconnected/connected.
        """
        # Walk back both chains to same height
        height1 = self.height[hash1]
        height2 = self.height[hash2]
        
        chain1 = []
        chain2 = []
        
        h1 = hash1
        h2 = hash2
        
        # Bring taller chain down to same height
        while height1 > height2:
            chain1.append(h1)
            h1 = self.parent[h1]
            height1 -= 1
        
        while height2 > height1:
            chain2.append(h2)
            h2 = self.parent[h2]
            height2 -= 1
        
        # Now walk back together until we find common ancestor
        while h1 != h2:
            chain1.append(h1)
            chain2.append(h2)
            
            if h1 not in self.parent or h2 not in self.parent:
                # Reached genesis or missing block
                break
            
            h1 = self.parent[h1]
            h2 = self.parent[h2]
        
        # h1 == h2 == fork point
        # Reverse chain2 to get correct order (fork_point -> tip)
        chain2.reverse()
        
        return h1, chain1, chain2
    
    def _resolve_orphans(self, parent_hash: bytes):
        """
        Recursively resolve orphans when their parent arrives.
        
        This is called after adding a new block - checks if any orphans
        were waiting for this block as their parent.
        """
        if parent_hash not in self.orphans_by_parent:
            return
        
        orphan_hashes = self.orphans_by_parent.pop(parent_hash)
        
        logging.info(f"[ForkHandler] 🔗 Resolving {len(orphan_hashes)} orphan(s) for parent {parent_hash[::-1].hex()[:16]}...")
        
        for orphan_hash in orphan_hashes:
            # Get the orphan header
            orphan_header = self.nodes[orphan_hash]
            
            # Now parent is known, can assign height and work
            self.height[orphan_hash] = self.height[parent_hash] + 1
            self.work[orphan_hash] = self.work[parent_hash] + self._calculate_header_work(orphan_header)
            
            # Update branch tips: add new tip, but only remove parent if it's not the best tip
            self.branch_tips.add(orphan_hash)
            if parent_hash != self.best_tip:
                self.branch_tips.discard(parent_hash)
            
            # Check if this creates a better chain
            self._maybe_update_best(orphan_hash)
            
            # Recursively resolve orphans of this orphan
            self._resolve_orphans(orphan_hash)
            
            logging.debug(f"[ForkHandler] ✅ Orphan {orphan_hash[::-1].hex()[:16]}... connected at height {self.height[orphan_hash]}")
    
    def _prune_old_blocks(self):
        """
        Remove blocks older than max_depth to bound memory usage.
        
        Only prunes blocks that are deep in the mainchain and won't
        be needed for reorg detection.
        """
        best_height = self.height[self.best_tip]
        cutoff_height = best_height - self.max_depth
        
        if cutoff_height <= 0:
            return
        
        # Find blocks to prune (below cutoff height)
        to_prune = [h for h, height in self.height.items() if height < cutoff_height]
        
        if not to_prune:
            return
        
        # Remove from all data structures
        for h in to_prune:
            del self.nodes[h]
            del self.height[h]
            del self.work[h]
            
            # Remove from parent mapping
            if h in self.parent:
                del self.parent[h]
            
            # Remove from branch tips (shouldn't be there, but be safe)
            self.branch_tips.discard(h)
        
        logging.debug(f"[ForkHandler] Pruned {len(to_prune)} old blocks (cutoff height: {cutoff_height})")
    
    def _calculate_header_work(self, header_bytes: bytes) -> int:
        """
        Calculate work for a single header (inverse of difficulty).
        
        Work = 2^256 / (target + 1)
        
        For simplicity, we use: work ≈ max_target / target
        """
        # Parse bits field (bytes 72-76 in header)
        bits = struct.unpack("<I", header_bytes[72:76])[0]
        
        # Convert compact bits to target
        target = parse_compact_bits(bits)
        
        # Calculate work (inverse of target)
        # Use simplified formula: max_target / target
        max_target = 0x00000000FFFF0000000000000000000000000000000000000000000000000000
        
        if target == 0:
            return 0
        
        work = max_target // target
        
        return work
    
    def _compute_hash(self, header_bytes: bytes) -> bytes:
        """
        Compute double SHA256 hash of header.
        Returns hash in internal byte order (little-endian).
        """
        return hashlib.sha256(hashlib.sha256(header_bytes).digest()).digest()
    
    def _parse_prev_hash(self, header_bytes: bytes) -> bytes:
        """
        Extract prev_block hash from header.
        Returns hash in internal byte order (bytes 4-36).
        """
        return header_bytes[4:36]
    
    # ========== Query methods ==========
    
    def get_best_tip(self) -> bytes:
        """Return hash of current best chain tip"""
        return self.best_tip
    
    def get_height(self, block_hash: bytes) -> Optional[int]:
        """Return height of block, or None if not connected"""
        return self.height.get(block_hash)
    
    def get_work(self, block_hash: bytes) -> Optional[int]:
        """Return cumulative work of block, or None if not connected"""
        return self.work.get(block_hash)
    
    def get_branch_tips(self) -> List[Tuple[bytes, int, int]]:
        """
        Return all competing branch tips.
        
        Returns:
            List of (hash, height, work) tuples
        """
        tips = []
        for tip_hash in self.branch_tips:
            if tip_hash in self.height:  # Only include connected tips
                tips.append((tip_hash, self.height[tip_hash], self.work[tip_hash]))
        
        # Sort by work (descending)
        tips.sort(key=lambda x: x[2], reverse=True)
        
        return tips
    
    def get_orphan_count(self) -> int:
        """Return total number of orphaned headers"""
        return sum(len(orphans) for orphans in self.orphans_by_parent.values())
    
    def get_stats(self) -> dict:
        """Return statistics for monitoring/debugging"""
        return {
            "best_tip": self.best_tip[::-1].hex(),
            "best_height": self.height[self.best_tip],
            "best_work": self.work[self.best_tip],
            "total_headers": len(self.nodes),
            "connected_headers": len(self.height),
            "orphan_headers": self.get_orphan_count(),
            "branch_tips": len(self.branch_tips),
            "branch_details": [
                {
                    "hash": h[::-1].hex()[:16] + "...",
                    "height": height,
                    "work": work
                }
                for h, height, work in self.get_branch_tips()
            ]
        }
