import hashlib
import struct
import os
from io import BytesIO


class BlockChain:
    """Storage and retrieval of full Bitcoin blocks indexed by header hash"""
    
    def __init__(self, blocks_file="blocks.dat", index_file="blocks.idx"):
        self.blocks_file = blocks_file
        self.index_file = index_file
        
        # Index structure: block_hash -> {'offset': int, 'size': int, 'height': int}
        self.index_by_hash = {}
        self.index_by_height = {}  # height -> block_hash
        self.height = -1  # Highest block height stored
        
        self._load_index()
    
    def _load_index(self):
        """Load block index from index file"""
        if os.path.exists(self.index_file):
            try:
                with open(self.index_file, 'rb') as f:
                    data = f.read()
                    stream = BytesIO(data)
                    
                    # Read index entries
                    while True:
                        # Format: hash (32 bytes) + offset (8 bytes) + size (4 bytes) + height (4 bytes)
                        entry = stream.read(48)
                        if len(entry) < 48:
                            break
                        
                        block_hash = entry[0:32]
                        offset = struct.unpack("<Q", entry[32:40])[0]
                        size = struct.unpack("<I", entry[40:44])[0]
                        height = struct.unpack("<i", entry[44:48])[0]
                        
                        self.index_by_hash[block_hash] = {
                            'offset': offset,
                            'size': size,
                            'height': height
                        }
                        self.index_by_height[height] = block_hash
                        
                        if height > self.height:
                            self.height = height
                
                print(f"[BlockChain] Loaded index: {len(self.index_by_hash)} blocks, height={self.height}")
            except Exception as e:
                print(f"[BlockChain] Error loading index: {e}")
                self.index_by_hash = {}
                self.index_by_height = {}
                self.height = -1
        else:
            print(f"[BlockChain] No existing index file, starting fresh")
    
    def _save_index_entry(self, block_hash, offset, size, height):
        """Append a single index entry to the index file"""
        with open(self.index_file, 'ab') as f:
            # Format: hash (32 bytes) + offset (8 bytes) + size (4 bytes) + height (4 bytes)
            entry = block_hash
            entry += struct.pack("<Q", offset)
            entry += struct.pack("<I", size)
            entry += struct.pack("<i", height)
            f.write(entry)
    
    def _double_sha256(self, data):
        """Bitcoin uses double SHA256 for block hashing"""
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()
    
    def _get_block_hash(self, block_data):
        """Get the hash of a block from its header (first 80 bytes)"""
        if len(block_data) < 80:
            return None
        header = block_data[:80]
        return self._double_sha256(header)
    
    def add_block(self, block_data, height=None):
        """
        Add a block to storage
        Returns: True if added, False if already exists or error
        """
        if len(block_data) < 80:
            print(f"[BlockChain] Invalid block data: too short ({len(block_data)} bytes)")
            return False
        
        # Calculate block hash from header
        block_hash = self._get_block_hash(block_data)
        if not block_hash:
            return False
        
        # Check if already stored
        if block_hash in self.index_by_hash:
            return False  # Already have it
        
        # Determine height
        if height is None:
            height = self.height + 1
        
        # Get current file size (this will be the offset)
        if os.path.exists(self.blocks_file):
            offset = os.path.getsize(self.blocks_file)
        else:
            offset = 0
        
        size = len(block_data)
        
        # Append block to file
        try:
            with open(self.blocks_file, 'ab') as f:
                f.write(block_data)
            
            # Update index
            self.index_by_hash[block_hash] = {
                'offset': offset,
                'size': size,
                'height': height
            }
            self.index_by_height[height] = block_hash
            
            # Save index entry
            self._save_index_entry(block_hash, offset, size, height)
            
            # Update height
            if height > self.height:
                self.height = height
            
            return True
            
        except Exception as e:
            print(f"[BlockChain] Error writing block: {e}")
            return False
    
    def get_block(self, block_hash):
        """
        Retrieve a block by its hash
        Returns: block data (bytes) or None if not found
        """
        if block_hash not in self.index_by_hash:
            return None
        
        entry = self.index_by_hash[block_hash]
        offset = entry['offset']
        size = entry['size']
        
        try:
            with open(self.blocks_file, 'rb') as f:
                f.seek(offset)
                block_data = f.read(size)
                
                if len(block_data) != size:
                    print(f"[BlockChain] Warning: Read {len(block_data)} bytes, expected {size}")
                    return None
                
                return block_data
        except Exception as e:
            print(f"[BlockChain] Error reading block: {e}")
            return None
    
    def get_block_by_height(self, height):
        """
        Retrieve a block by its height
        Returns: block data (bytes) or None if not found
        """
        if height not in self.index_by_height:
            return None
        
        block_hash = self.index_by_height[height]
        return self.get_block(block_hash)
    
    def has_block(self, block_hash):
        """Check if we have a block"""
        return block_hash in self.index_by_hash
    
    def get_height(self):
        """Return highest block height stored"""
        return self.height
    
    def get_block_count(self):
        """Return total number of blocks stored"""
        return len(self.index_by_hash)
    
    def get_first_block_height(self):
        """Return the lowest block height stored"""
        if len(self.index_by_height) == 0:
            return None
        return min(self.index_by_height.keys())
    
    def get_blocks_range(self, start_height, end_height):
        """
        Get multiple blocks by height range
        Returns: list of (height, block_data) tuples
        """
        blocks = []
        for height in range(start_height, end_height + 1):
            if height in self.index_by_height:
                block_hash = self.index_by_height[height]
                block_data = self.get_block(block_hash)
                if block_data:
                    blocks.append((height, block_data))
        return blocks


if __name__ == "__main__":
    print("=" * 60)
    print("BlockChain Class Tests")
    print("=" * 60)
    
    # Test 1: Create new blockchain storage
    print("\nTest 1: Create new blockchain storage")
    bc = BlockChain("test_blocks.dat", "test_blocks.idx")
    print(f"Initial height: {bc.get_height()}")
    print(f"Initial block count: {bc.get_block_count()}")
    
    # Test 2: Add a fake block
    print("\nTest 2: Add a fake block")
    fake_header = bytes.fromhex(
        "0100000000000000000000000000000000000000000000000000000000000000"
        "000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a"
        "29ab5f49ffff001d1dac2b7c"
    )
    fake_tx_data = b"\x01" + b"\x00" * 100  # 1 tx + some dummy data
    fake_block = fake_header + fake_tx_data
    
    result = bc.add_block(fake_block, height=0)
    print(f"Add block result: {result}")
    print(f"New height: {bc.get_height()}")
    print(f"New block count: {bc.get_block_count()}")
    
    # Test 3: Retrieve the block
    print("\nTest 3: Retrieve the block")
    block_hash = bc._get_block_hash(fake_block)
    print(f"Block hash: {block_hash[::-1].hex()}")
    
    retrieved_block = bc.get_block(block_hash)
    if retrieved_block == fake_block:
        print("✅ Retrieved block matches original")
    else:
        print("❌ Retrieved block does not match")
    
    # Test 4: Retrieve by height
    print("\nTest 4: Retrieve by height")
    retrieved_by_height = bc.get_block_by_height(0)
    if retrieved_by_height == fake_block:
        print("✅ Retrieved by height matches original")
    else:
        print("❌ Retrieved by height does not match")
    
    # Test 5: Add another block and test persistence
    print("\nTest 5: Add another block and test persistence")
    fake_header2 = bytes.fromhex(
        "0200000000000000000000000000000000000000000000000000000000000000"
        "111111113ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a"
        "29ab5f49ffff001d1dac2b7d"
    )
    fake_block2 = fake_header2 + b"\x01" + b"\x11" * 150
    
    bc.add_block(fake_block2, height=1)
    print(f"Height after adding block 2: {bc.get_height()}")
    
    # Test 6: Reload from disk
    print("\nTest 6: Reload from disk")
    bc2 = BlockChain("test_blocks.dat", "test_blocks.idx")
    print(f"Reloaded height: {bc2.get_height()}")
    print(f"Reloaded block count: {bc2.get_block_count()}")
    
    retrieved_again = bc2.get_block_by_height(0)
    if retrieved_again == fake_block:
        print("✅ Block persisted correctly")
    else:
        print("❌ Block persistence failed")
    
    # Test 7: has_block check
    print("\nTest 7: has_block check")
    print(f"Has block 0: {bc.has_block(block_hash)}")
    print(f"Has random block: {bc.has_block(b'0' * 32)}")
    
    # Test 8: Get blocks range
    print("\nTest 8: Get blocks range")
    blocks_range = bc.get_blocks_range(0, 1)
    print(f"Retrieved {len(blocks_range)} blocks in range 0-1")
    
    print("\n" + "=" * 60)
    print("All tests completed")
    print("=" * 60)
    
    # Cleanup test files
    import os
    for f in ["test_blocks.dat", "test_blocks.idx"]:
        if os.path.exists(f):
            os.remove(f)
            print(f"Cleaned up {f}")
