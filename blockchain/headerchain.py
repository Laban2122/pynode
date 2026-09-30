import hashlib
import struct
import os

class HeaderChain:
    def __init__(self, headers_file="headers.dat"):
        self.headers_file = headers_file
        self.headers = []  # Keep temporarily for compatibility
        
        # New efficient data structures
        self.index_by_hash = {}  # hash -> {'height': int, 'header': bytes}
        self.buffer_threshold = 100  # Persist when this many headers ahead
        self.height = -1  # Start at -1, genesis will be height 0
        
        # Genesis block header — chain identity comes from config (single source
        # of truth). Override protocol.genesis_header to run an alternative chain.
        from core.config import GENESIS_HEADER
        self.genesis_header = GENESIS_HEADER
        
        self._load_headers_from_file()
    
    def _load_headers_from_file(self):
        """Load headers from file using add_headers() for consistency"""
        if os.path.exists(self.headers_file):
            with open(self.headers_file, 'rb') as f:
                data = f.read()
                if len(data) == 0:
                    # Empty file - start with genesis
                    self.add_headers([self.genesis_header])
                else:
                    # Split into 80-byte chunks and add them all
                    headers_to_add = []
                    for i in range(0, len(data), 80):
                        if i + 80 <= len(data):
                            headers_to_add.append(data[i:i+80])
                    
                    # Add all headers at once - this will validate the chain
                    added_count = self.add_headers(headers_to_add, _file_sync=True)
                    if added_count != len(headers_to_add):
                        print(f"Warning: Only {added_count} of {len(headers_to_add)} headers were valid")
        else:
            # No file - start with genesis
            self.add_headers([self.genesis_header])

    def _add_header_to_structures(self, header, height):
        header_hash = self._double_sha256(header)
        self.headers.append(header)
        self.index_by_hash[header_hash] = height  # Store actual height, not list index
        self.height = height


    def _append_header_to_file(self, header):
        """Append a single header to the file"""
        with open(self.headers_file, 'ab') as f:
            f.write(header)
    
    def _double_sha256(self, data):
        """Bitcoin uses double SHA256 for header hashing"""
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()
    
    def _get_header_hash(self, header):
        """Get the hash of a header"""
        return self._double_sha256(header)
    
    def add_headers(self, headers, _file_sync=False):
        """Add a list of headers, validating that each connects to the previous"""
        count = 0
        
        for header in headers:
            if len(header) != 80:
                continue  # Skip invalid headers
            
            # Handle empty chain case - first header should be genesis
            if not self.headers:
                if header == self.genesis_header:
                    # Valid genesis header
                    self._add_header_to_structures(header, 0)
                    # If reading from file do not write to file
                    if not _file_sync:
                        self._append_header_to_file(header)
                    count += 1
                    continue
                else:
                    print(f"Error: First header must be genesis, got: {header.hex()}")
                    break
            
            # Get the prev_hash from the new header (bytes 4-36)
            prev_hash = header[4:36]
            
            # Check if this header connects to our current tip
            current_tip_hash = self.get_tip_hash()
            if prev_hash == current_tip_hash:
                # Valid connection - add to chain
                new_height = self.height + 1
                self._add_header_to_structures(header, new_height)
                # If reading from file do not write to file
                if not _file_sync:
                    self._append_header_to_file(header)
                count += 1
            else:
                print(f"Header does not connect to the current tip: {self._get_header_hash(header)[::-1].hex()} ")   
                break  # Stop on first invalid header
        return count
    

    def get_height(self):
        """Return current blockchain height (number of headers - 1)"""
        return len(self.headers) - 1
    
    def get_tip_hash(self):
        return self._double_sha256(self.headers[-1])
    
    def get_locator(self):
        """Generate a block locator for efficient chain synchronization"""
        locator = []
        if not self.headers:
            return locator
        
        # Start from the tip and work backwards
        index = len(self.headers) - 1
        step = 1
        count = 0
        
        while index >= 0 and count < 32:
            # Add current header hash to locator
            locator.append(self._get_header_hash(self.headers[index]))
            count += 1
            
            # Move back by current step size
            index -= step
            
            # After first 10 entries, double the step size
            if count >= 10:
                step *= 2
        
        # Always include genesis hash if not already included
        genesis_hash = self._get_header_hash(self.headers[0])
        if locator and locator[-1] != genesis_hash:
            locator.append(genesis_hash)
        
        return locator

    def reorg(self, new_tail: list):
        """
        Reorganize chain to follow new fork.
        
        Args:
            new_tail: List of 80-byte headers from fork_point+1 to new tip
        
        Process:
            1. Find fork point (parent of first new_tail header)
            2. Truncate self.headers to fork point
            3. Truncate file to fork point
            4. Use add_headers() to append new_tail (validates and updates all structures)
        """
        if not new_tail:
            print("[HeaderChain] ⚠️  Empty new_tail, no reorg needed")
            return
        
        # Step 1: Find fork point
        # The first header in new_tail points to the fork point via prev_hash
        first_new_header = new_tail[0]
        fork_point_hash = first_new_header[4:36]  # prev_hash of first new header
        
        # Find fork point index in current chain
        fork_index = None
        for i, header in enumerate(self.headers):
            if self._double_sha256(header) == fork_point_hash:
                fork_index = i
                break
        
        if fork_index is None:
            print(f"[HeaderChain] ❌ Fork point not found in current chain")
            return
        
        fork_height = fork_index  # Height = index for 0-based
        print(f"[HeaderChain] 🔀 Executing reorg: fork at height {fork_height}")
        print(f"[HeaderChain]    Current tip: height {self.height}")
        print(f"[HeaderChain]    Discarding {len(self.headers) - fork_index - 1} headers")
        print(f"[HeaderChain]    Appending {len(new_tail)} new headers")
        
        # Step 2: Truncate in-memory chain to fork point
        self.headers = self.headers[:fork_index + 1]
        
        # Update height to fork point
        self.height = fork_height
        
        # Rebuild index_by_hash (only keep fork point and earlier)
        self.index_by_hash = {}
        for i, header in enumerate(self.headers):
            header_hash = self._double_sha256(header)
            self.index_by_hash[header_hash] = i
        
        # Step 3: Truncate file to fork point
        truncate_size = (fork_index + 1) * 80
        with open(self.headers_file, 'r+b') as f:
            f.truncate(truncate_size)
        
        print(f"[HeaderChain]    File truncated to {truncate_size} bytes")
        
        # Step 4: Append new_tail using add_headers()
        # This validates connections, updates index_by_hash, and writes to file
        added = self.add_headers(new_tail)
        
        if added != len(new_tail):
            print(f"[HeaderChain] ⚠️  Warning: Only added {added}/{len(new_tail)} headers from new tail")
        else:
            new_tip_hash = self.get_tip_hash()
            print(f"[HeaderChain] ✅ Reorg complete: new tip at height {self.height}")
            print(f"[HeaderChain]    New tip hash: {new_tip_hash[::-1].hex()[:16]}...")




if __name__ == "__main__":
    # Test 1 Test1 Test1: Instantiate object with testfile.dat and check genesis header
    h_chain = HeaderChain("testfile.dat")
    if len(h_chain.headers) > 0 and len(h_chain.headers[0]) == 80:
        print("Genesis header has 80 bytes")
        print("Genesis header hex:", h_chain.headers[0].hex())
    else:
        print("Error: Genesis header not 80 bytes")
    
    # Test 2: Add n fake headers that meets validation criteria
    n = 2 # Number of fake headers to add
    fake_headers = []
    n0=len(h_chain.headers)
    last_header_hash = h_chain._double_sha256(h_chain.headers[-1])
    for i in range(n):
        #print (f"Last header hash: \n{last_header_hash.hex()}")
        fake_header = b"\x00" * 4  # Version (4 bytes)
        fake_header += last_header_hash  # Previous block hash (32 bytes)
        fake_header += b"\x11" * 40  # Merkle root + timestamp + bits (40 bytes)
        fake_header += struct.pack("<I", i)  # Nonce (4 bytes)
        #print (f"This header: \n{fake_header[4:36].hex()}")
        fake_headers.append(fake_header)
        last_header_hash = h_chain._double_sha256(fake_header)

    h_chain.add_headers(fake_headers)
    # Check if headers were added
    if len(h_chain.headers) == n +n0:  # +1 for the genesis header
        print(f"Added {n} fake headers successfully.")
    else:
        print(f"Error: Expected {n + 1} headers, found {len(h_chain.headers)}")
    # compare file size and number of headers x 80
    expected_size = (n + n0) * 80
    actual_size = os.path.getsize(h_chain.headers_file)
    if actual_size == expected_size:
        print(f"File size is as expected: {expected_size} bytes")
    else:
        print(f"Error: Expected {expected_size} bytes, found {actual_size} bytes")

    # Test 3: Get locator an compare result with up to 32 newest header hashes
    locator = h_chain.get_locator()
    # Print the locator hashes with indexes 
    print("Locator hashes:")
    for i, hash in enumerate(locator):
        continue
        print(f"Index {i}: {hash.hex()}")   
    # print the last 20 hashes of self.headers
    print("Last 20 header hashes:")
    for i, header in enumerate(h_chain.headers[-1:-21:-1]):
        continue
        print(f"Header {len(h_chain.headers)  - i}: {h_chain._get_header_hash(header).hex()}")

    # Test4 - check validation of header connection in file
    print ("Test 4: Validate header connections in file")
    new_chain = HeaderChain("test_headers.dat")
    
    i = 0
    for h in new_chain.headers:
        if i <= 1:  # Only validate after the first header
            continue
        prev_header = new_chain.headers[i-1]
        if new_chain._get_header_hash(new_chain.headers[i]) != new_chain._double_sha256(new_chain.headers[i-1]):
            print(f"Header validation failed at position {i}")
            break
        
        i += 1
    else:
        print(f"All {len(new_chain.headers)} headers validated successfully")
        print(new_chain.headers[0][4:36].hex())
        print(new_chain.headers[1][4:36].hex())
        print(new_chain.headers[2][4:36][::-1].hex())
        print(new_chain.headers[3][4:36].hex())

    # Test5 - check file consistency
    print("Test 5: Check file consistency")
    if os.path.exists(new_chain.headers_file):
        with open(new_chain.headers_file, 'rb') as f:
            data = f.read()
            if len(data) == 0:
                print("File is empty, expected at least genesis header")
            else:
                # Check if file size matches number of headers * 80
                expected_size = len(new_chain.headers) * 80
                actual_size = len(data)
                if actual_size == expected_size:
                    print(f"File size is consistent: {actual_size} bytes")
                else:
                    print(f"Error: Expected {expected_size} bytes, found {actual_size} bytes")

                # check if all headers are chained by hash
                for i in range(1, len(new_chain.headers)):
                    prev_header = new_chain.headers[i-1]
                    current_header = new_chain.headers[i]
                    current_prev_hash = current_header[4:36]
                    if current_prev_hash != new_chain._double_sha256(prev_header):
                        print(f"Header validation failed at position {i}")
                        break
                else:
                    print("All headers in file are correctly chained by hash")
    else:
        print("Headers file does not exist")

    # Test 6: Check tip-hash consistency and file size relationship
    print("Test 6: Check header count vs file size consistency")
    test_chain = HeaderChain("test_headers.dat")
    
    # Get the number of headers using the new data structure
    n = len(test_chain.headers_by_height)  # Number of headers
    expected_file_size = n * 80
    
    if os.path.exists(test_chain.headers_file):
        actual_file_size = os.path.getsize(test_chain.headers_file)
        if actual_file_size == expected_file_size:
            print(f"✅ File size consistency test passed: {n} headers = {actual_file_size} bytes")
        else:
            print(f"❌ File size mismatch: {n} headers should be {expected_file_size} bytes, but file is {actual_file_size} bytes")
    else:
        print("❌ Headers file does not exist for test")
    
    # Also test that get_tip_hash() returns a valid hash
    tip_hash = test_chain.get_tip_hash()
    if tip_hash and len(tip_hash) == 32:
        print(f"✅ get_tip_hash() returned valid 32-byte hash: {tip_hash.hex()[:16]}...")
    else:
        print(f"❌ get_tip_hash() returned invalid result: {tip_hash}")