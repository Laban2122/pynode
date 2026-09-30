# Reusable parsing utilities for Bitcoin protocol messages
import struct
import socket
from io import BytesIO
from typing import List, Tuple, Dict, Any


def read_varint(stream: BytesIO) -> int:
    """Read variable-length integer from byte stream"""
    first_byte = stream.read(1)
    if not first_byte:
        raise ValueError("Empty varint")
    
    i = first_byte[0]
    if i < 0xfd:
        return i
    elif i == 0xfd:
        return int.from_bytes(stream.read(2), 'little')
    elif i == 0xfe:
        return int.from_bytes(stream.read(4), 'little')
    else:
        return int.from_bytes(stream.read(8), 'little')


def write_varint(stream: BytesIO, value: int):
    """Write variable-length integer to byte stream"""
    if value < 0xfd:
        stream.write(bytes([value]))
    elif value <= 0xffff:
        stream.write(b'\xfd')
        stream.write(struct.pack('<H', value))
    elif value <= 0xffffffff:
        stream.write(b'\xfe')
        stream.write(struct.pack('<I', value))
    else:
        stream.write(b'\xff')
        stream.write(struct.pack('<Q', value))


def read_varstring(stream: BytesIO) -> bytes:
    """Read variable-length string (length-prefixed bytes)"""
    length = read_varint(stream)
    return stream.read(length)


def parse_inv(payload: bytes) -> List[Dict[str, Any]]:
    """
    Parse 'inv' message payload.
    Returns list of inventory vectors: [{"type": int, "hash": bytes}, ...]
    
    Types: 1=MSG_TX, 2=MSG_BLOCK, 3=MSG_FILTERED_BLOCK, 4=MSG_CMPCT_BLOCK
    """
    stream = BytesIO(payload)
    count = read_varint(stream)
    
    inv_vectors = []
    for _ in range(count):
        inv_type = struct.unpack("<I", stream.read(4))[0]
        inv_hash = stream.read(32)
        
        inv_vectors.append({
            "type": inv_type,
            "hash": inv_hash
        })
    
    return inv_vectors


def parse_addr(payload: bytes) -> List[Dict[str, Any]]:
    """
    Parse 'addr' message payload.
    Returns list of addresses: [{"time": int, "services": int, "ip": str, "port": int}, ...]
    """
    stream = BytesIO(payload)
    count = read_varint(stream)
    
    addresses = []
    for _ in range(count):
        timestamp = struct.unpack("<I", stream.read(4))[0]
        services = struct.unpack("<Q", stream.read(8))[0]
        ip_bytes = stream.read(16)
        port = struct.unpack(">H", stream.read(2))[0]
        
        # Convert IPv6 bytes to string
        # Check if it's an IPv4-mapped IPv6 address
        if ip_bytes[:12] == b'\x00' * 10 + b'\xff\xff':
            # IPv4-mapped
            ip = socket.inet_ntop(socket.AF_INET, ip_bytes[12:16])
        else:
            # IPv6
            ip = socket.inet_ntop(socket.AF_INET6, ip_bytes)
        
        addresses.append({
            "time": timestamp,
            "services": services,
            "ip": ip,
            "port": port
        })
    
    return addresses


def parse_addrv2(payload: bytes) -> List[Dict[str, Any]]:
    """
    Parse 'addrv2' message payload (BIP 155).
    Returns list of addresses with extended network support.
    """
    stream = BytesIO(payload)
    count = read_varint(stream)
    
    addresses = []
    for _ in range(count):
        timestamp = struct.unpack("<I", stream.read(4))[0]
        
        # Services (compactSize)
        services = read_varint(stream)
        
        # Network ID
        network_id = stream.read(1)[0]
        
        # Address length and bytes
        addr_len = read_varint(stream)
        addr_bytes = stream.read(addr_len)
        
        # Port
        port = struct.unpack(">H", stream.read(2))[0]
        
        # Convert address based on network ID
        # 1 = IPv4, 2 = IPv6, 3 = TORv2, 4 = TORv3, 5 = I2P, 6 = CJDNS
        if network_id == 1:
            ip = socket.inet_ntop(socket.AF_INET, addr_bytes)
        elif network_id == 2:
            ip = socket.inet_ntop(socket.AF_INET6, addr_bytes)
        else:
            ip = addr_bytes.hex()  # For other networks, use hex representation
        
        addresses.append({
            "time": timestamp,
            "services": services,
            "network_id": network_id,
            "ip": ip,
            "port": port
        })
    
    return addresses


def parse_headers(payload: bytes) -> List[Dict[str, Any]]:
    """
    Parse 'headers' message payload.
    Returns list of block headers with basic fields.
    """
    stream = BytesIO(payload)
    count = read_varint(stream)
    
    headers = []
    for _ in range(count):
        version = struct.unpack("<I", stream.read(4))[0]
        prev_block = stream.read(32)
        merkle_root = stream.read(32)
        timestamp = struct.unpack("<I", stream.read(4))[0]
        bits = struct.unpack("<I", stream.read(4))[0]
        nonce = struct.unpack("<I", stream.read(4))[0]
        
        # Headers message includes transaction count (always 0)
        txn_count = read_varint(stream)
        
        headers.append({
            "version": version,
            "prev_block": prev_block,
            "merkle_root": merkle_root,
            "timestamp": timestamp,
            "bits": bits,
            "nonce": nonce
        })
    
    return headers


def inv_type_to_string(inv_type: int) -> str:
    """Convert inventory type number to human-readable string"""
    types = {
        0: "ERROR",
        1: "MSG_TX",
        2: "MSG_BLOCK",
        3: "MSG_FILTERED_BLOCK",
        4: "MSG_CMPCT_BLOCK",
        5: "MSG_WITNESS_BLOCK",
        6: "MSG_WITNESS_TX",
        7: "MSG_FILTERED_WITNESS_BLOCK"
    }
    return types.get(inv_type, f"UNKNOWN({inv_type})")


# ========== Phase 4.1: Header Chain Helpers ==========

def compute_block_hash(header_dict: Dict[str, Any]) -> bytes:
    """
    Compute block hash from header dictionary (from parse_headers).
    Returns hash in internal byte order.
    """
    import hashlib
    import struct
    
    # Serialize header (80 bytes)
    header_bytes = b""
    header_bytes += struct.pack("<I", header_dict["version"])
    header_bytes += header_dict["prev_block"]  # already in internal order
    header_bytes += header_dict["merkle_root"]
    header_bytes += struct.pack("<I", header_dict["timestamp"])
    header_bytes += struct.pack("<I", header_dict["bits"])
    header_bytes += struct.pack("<I", header_dict["nonce"])
    
    # Double SHA256
    return hashlib.sha256(hashlib.sha256(header_bytes).digest()).digest()


def parse_compact_bits(bits: int) -> int:
    """
    Parse compact difficulty target (bits field) into full 256-bit target.
    Returns target as integer.
    
    Compact format: 0xMMEEEEEE
    - MM = size (number of bytes)
    - EEEEEE = mantissa
    """
    size = bits >> 24
    mantissa = bits & 0x00ffffff
    
    if size <= 3:
        target = mantissa >> (8 * (3 - size))
    else:
        target = mantissa << (8 * (size - 3))
    
    return target


def hash_meets_target(hash_bytes: bytes, target: int) -> bool:
    """
    Check if block hash meets the difficulty target.
    
    Args:
        hash_bytes: Block hash in internal byte order (32 bytes)
        target: Difficulty target as integer
    
    Returns:
        True if hash <= target (valid PoW)
    """
    # Convert hash to integer (little-endian / internal byte order)
    hash_int = int.from_bytes(hash_bytes, 'little')
    
    return hash_int <= target


# ========== Phase 4.1f: getheaders handling ==========

def decode_varint(data: bytes) -> Tuple[int, int]:
    """
    Decode a varint and return (value, bytes_consumed)
    Used for parsing getheaders
    """
    first_byte = data[0]
    
    if first_byte < 0xfd:
        return first_byte, 1
    elif first_byte == 0xfd:
        return struct.unpack('<H', data[1:3])[0], 3
    elif first_byte == 0xfe:
        return struct.unpack('<I', data[1:5])[0], 5
    else:
        return struct.unpack('<Q', data[1:9])[0], 9


def encode_varint(value: int) -> bytes:
    """Encode an integer as a varint"""
    if value < 0xfd:
        return bytes([value])
    elif value <= 0xffff:
        return b'\xfd' + struct.pack('<H', value)
    elif value <= 0xffffffff:
        return b'\xfe' + struct.pack('<I', value)
    else:
        return b'\xff' + struct.pack('<Q', value)


def parse_getheaders_message(payload: bytes) -> Dict[str, Any]:
    """
    Parse a getheaders message payload
    
    Args:
        payload: Raw message payload (bytes) 
        
    Returns:
        dict with 'version', 'locator_hashes', 'stop_hash'
    """
    offset = 0
    
    # Parse version (4 bytes, little endian)
    version = struct.unpack("<I", payload[offset:offset+4])[0]
    offset += 4
    
    # Parse hash count (variable length integer)
    hash_count, varint_size = decode_varint(payload[offset:])
    offset += varint_size
    
    # Parse locator hashes (32 bytes each)
    locator_hashes = []
    for i in range(hash_count):
        hash_bytes = payload[offset:offset+32]
        locator_hashes.append(hash_bytes)
        offset += 32
    
    # Parse stop hash (32 bytes)
    stop_hash = payload[offset:offset+32]
    
    if locator_hashes:
        # Format hash as first 3 bytes and last 3 bytes: 000000.....e2b014
        full_hash = locator_hashes[0][::-1].hex()
        formatted_hash = f"{full_hash[:6]}.....{full_hash[-6:]}"
        print(f"       Locator hash count: {hash_count}, First hash: {formatted_hash}")
    
    return {
        'version': version,
        'locator_hashes': locator_hashes,
        'stop_hash': stop_hash
    }


def parse_inv_message(payload: bytes) -> List[Dict[str, Any]]:
    """
    Parse inv message to extract inventory items
    
    Returns list of items: [{'type': int, 'hash': bytes}, ...]
    Types: MSG_TX=1, MSG_BLOCK=2, MSG_WITNESS_BLOCK=0x40000002, etc.
    """
    stream = BytesIO(payload)
    count = read_varint(stream)
    
    items = []
    for _ in range(count):
        inv_type = struct.unpack('<I', stream.read(4))[0]  # 4-byte type
        inv_hash = stream.read(32)  # 32-byte hash (internal/LE order)
        
        items.append({
            'type': inv_type,
            'hash': inv_hash
        })
    
    return items


def create_getdata_message(items: List[Dict[str, Any]]) -> bytes:
    """
    Create getdata message payload from inventory items
    
    items: [{'type': int, 'hash': bytes}, ...]
    Returns: raw payload bytes
    """
    stream = BytesIO()
    
    # Write count
    write_varint(stream, len(items))
    
    # Write each item
    for item in items:
        stream.write(struct.pack('<I', item['type']))  # 4-byte type
        stream.write(item['hash'])  # 32-byte hash
    
    return stream.getvalue()


def parse_block_message(payload: bytes) -> Dict[str, Any]:
    """
    Parse block message
    
    Returns: {
        'header': bytes (80 bytes),
        'tx_count': int,
        'raw_txs': bytes (remaining payload after header)
    }
    """
    if len(payload) < 80:
        raise ValueError("Block message too short")
    
    header = payload[:80]
    stream = BytesIO(payload[80:])
    tx_count = read_varint(stream)
    
    # We don't parse transactions yet, just store the raw data
    raw_txs = stream.read()
    
    return {
        'header': header,
        'tx_count': tx_count,
        'raw_txs': raw_txs
    }


def calculate_txid(tx_bytes: bytes) -> str:
    """
    Calculate transaction ID (double SHA256 hash).
    Returns hex string in big-endian (display format).
    
    Note: For SegWit transactions, this should be called on the
    transaction data WITHOUT witness data (BIP141).
    """
    import hashlib
    hash1 = hashlib.sha256(tx_bytes).digest()
    hash2 = hashlib.sha256(hash1).digest()
    return hash2[::-1].hex()  # Reverse to big-endian for display


def parse_block_txs(block_payload: bytes) -> List[str]:
    """
    Parse block payload and return list of transaction IDs.
    Handles both legacy and SegWit transactions (BIP141).
    
    Returns: List of txids in hex (big-endian)
    """
    import hashlib
    
    try:
        stream = BytesIO(block_payload[80:])  # Skip 80-byte header
        tx_count = read_varint(stream)
        txids = []
        
        for _ in range(tx_count):
            # Save position before transaction
            start_pos = stream.tell()
            
            # Skip version (4 bytes)
            version = stream.read(4)
            
            # Check for SegWit marker (0x00 0x01)
            marker = stream.read(1)
            if marker == b'\x00':
                is_segwit = True
                flag = stream.read(1)
                if flag != b'\x01':
                    # Invalid SegWit flag
                    stream.seek(-2, 1)  # Rewind
                    is_segwit = False
            else:
                is_segwit = False
                stream.seek(-1, 1)  # Rewind
            
            # Read inputs
            in_count = read_varint(stream)
            for _ in range(in_count):
                stream.read(32)  # Previous txid
                stream.read(4)   # Previous vout
                script_len = read_varint(stream)
                stream.read(script_len)  # scriptSig
                stream.read(4)   # sequence
            
            # Read outputs
            out_count = read_varint(stream)
            for _ in range(out_count):
                stream.read(8)   # value (satoshis)
                script_len = read_varint(stream)
                stream.read(script_len)  # scriptPubKey
            
            # Save position before witness data
            before_witness = stream.tell()
            
            # Skip witness data if present
            if is_segwit:
                for _ in range(in_count):
                    witness_count = read_varint(stream)
                    for _ in range(witness_count):
                        witness_len = read_varint(stream)
                        stream.read(witness_len)
            
            # Read locktime
            locktime = stream.read(4)
            
            # Calculate txid (WITHOUT witness data for SegWit)
            end_pos = stream.tell()
            stream.seek(start_pos)
            
            if is_segwit:
                # Build TX without witness: version + inputs + outputs + locktime
                # seek past version(4) + marker(1) + flag(1) to reach input count
                stream.seek(start_pos + 6)
                tx_without_witness = BytesIO()
                tx_without_witness.write(version)

                # Re-read and write inputs
                in_count2 = read_varint(stream)
                write_varint(tx_without_witness, in_count2)
                for _ in range(in_count2):
                    prev_tx = stream.read(32)
                    prev_vout = stream.read(4)
                    script_len = read_varint(stream)
                    script_sig = stream.read(script_len)
                    sequence = stream.read(4)
                    tx_without_witness.write(prev_tx)
                    tx_without_witness.write(prev_vout)
                    write_varint(tx_without_witness, script_len)
                    tx_without_witness.write(script_sig)
                    tx_without_witness.write(sequence)
                
                # Re-read and write outputs
                out_count2 = read_varint(stream)
                write_varint(tx_without_witness, out_count2)
                for _ in range(out_count2):
                    value = stream.read(8)
                    script_len = read_varint(stream)
                    script_pubkey = stream.read(script_len)
                    tx_without_witness.write(value)
                    write_varint(tx_without_witness, script_len)
                    tx_without_witness.write(script_pubkey)
                
                tx_without_witness.write(locktime)  # locktime
                
                tx_data = tx_without_witness.getvalue()
                stream.seek(end_pos)  # Fast-forward past witness data
            else:
                # Legacy transaction - use full data
                tx_data = stream.read(end_pos - start_pos)
                stream.seek(start_pos)
                tx_data = stream.read(end_pos - start_pos)
            
            # Calculate txid
            tx_hash = hashlib.sha256(hashlib.sha256(tx_data).digest()).digest()
            txid = tx_hash[::-1].hex()  # Reverse to big-endian
            txids.append(txid)
        
        return txids
        
    except Exception as e:
        print(f"⚠️  Block TX parsing failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def parse_tx_full(stream: BytesIO) -> dict:
    """
    Parse one complete transaction from stream, returning inputs, outputs and txid.
    Handles legacy and segwit (BIP141). Advances stream past the full transaction.

    Returns:
        {txid, inputs: [{prev_txid, prev_vout}], outputs: [{value}], is_coinbase}
    """
    import hashlib

    start = stream.tell()
    version = stream.read(4)

    # Detect segwit marker
    b = stream.read(1)
    if b == b'\x00':
        flag = stream.read(1)
        is_segwit = (flag == b'\x01')
        if not is_segwit:
            stream.seek(-2, 1)
    else:
        is_segwit = False
        stream.seek(-1, 1)

    # Read inputs
    in_count = read_varint(stream)
    inputs = []
    for _ in range(in_count):
        prev_txid_le = stream.read(32)
        prev_vout = struct.unpack('<I', stream.read(4))[0]
        script_len = read_varint(stream)
        stream.read(script_len)  # scriptSig
        stream.read(4)           # sequence
        inputs.append({'prev_txid': prev_txid_le[::-1].hex(), 'prev_vout': prev_vout})

    # Read outputs
    out_count = read_varint(stream)
    outputs = []
    for _ in range(out_count):
        value = struct.unpack('<q', stream.read(8))[0]
        script_len = read_varint(stream)
        stream.read(script_len)  # scriptPubKey
        outputs.append({'value': value})

    # Skip witness data
    if is_segwit:
        for _ in range(in_count):
            for _ in range(read_varint(stream)):
                stream.read(read_varint(stream))

    locktime = stream.read(4)
    end = stream.tell()

    # Compute txid from non-witness serialization
    if is_segwit:
        # Seek past version(4) + marker(1) + flag(1) to re-read inputs/outputs cleanly
        stream.seek(start + 6)
        nw = BytesIO()
        nw.write(version)
        ic = read_varint(stream)
        write_varint(nw, ic)
        for _ in range(ic):
            nw.write(stream.read(32))
            nw.write(stream.read(4))
            sl = read_varint(stream)
            write_varint(nw, sl)
            nw.write(stream.read(sl))
            nw.write(stream.read(4))
        oc = read_varint(stream)
        write_varint(nw, oc)
        for _ in range(oc):
            nw.write(stream.read(8))
            sl = read_varint(stream)
            write_varint(nw, sl)
            nw.write(stream.read(sl))
        nw.write(locktime)
        tx_data = nw.getvalue()
        stream.seek(end)
    else:
        stream.seek(start)
        tx_data = stream.read(end - start)

    txid = hashlib.sha256(hashlib.sha256(tx_data).digest()).digest()[::-1].hex()

    is_coinbase = (
        len(inputs) == 1
        and inputs[0]['prev_txid'] == '0' * 64
        and inputs[0]['prev_vout'] == 0xffffffff
    )

    return {'txid': txid, 'inputs': inputs, 'outputs': outputs, 'is_coinbase': is_coinbase}


def get_tx_output(block_data: bytes, txid_hex: str, vout: int) -> int | None:
    """
    Scan a raw block for txid_hex and return the satoshi value at output index vout.
    Returns None if tx not found or vout out of range.
    """
    stream = BytesIO(block_data[80:])  # skip 80-byte header
    tx_count = read_varint(stream)
    for _ in range(tx_count):
        tx = parse_tx_full(stream)
        if tx['txid'] == txid_hex:
            return tx['outputs'][vout]['value'] if vout < len(tx['outputs']) else None
    return None

