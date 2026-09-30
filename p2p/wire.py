import struct
import hashlib
import asyncio
import socket


def dsha256(data: bytes) -> bytes:
    """Double SHA256 hash used throughout Bitcoin protocol"""
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


async def read_exact(reader, n: int) -> bytes:
    """Read exactly n bytes from async stream"""
    return await reader.readexactly(n)


def pack_net_addr(services: int, ip: str, port: int) -> bytes:
    """Pack network address for Bitcoin protocol (supports IPv4/IPv6)"""
    # Convert IP to bytes - handle both IPv4 and IPv6
    if ':' in ip:
        # IPv6
        ip_bytes = socket.inet_pton(socket.AF_INET6, ip)
    else:
        # IPv4 - convert to IPv6-mapped
        ipv4_bytes = socket.inet_pton(socket.AF_INET, ip)
        ip_bytes = b'\x00' * 10 + b'\xff\xff' + ipv4_bytes
    
    return struct.pack(">Q", services) + ip_bytes + struct.pack(">H", port)


def make_message(command: str, payload: bytes, magic: bytes = b"\xf9\xbe\xb4\xd9") -> bytes:
    """Create a Bitcoin protocol message (unified message creation)"""
    assert len(command) <= 12
    cmd = command.encode('ascii') + b"\x00" * (12 - len(command))
    length = struct.pack("<I", len(payload))
    checksum = dsha256(payload)[:4]
    return magic + cmd + length + checksum + payload
    
def read_varint(stream):
    i = stream.read(1)[0]
    if i < 0xfd:
        return i
    elif i == 0xfd:
        return int.from_bytes(stream.read(2), 'little')
    elif i == 0xfe:
        return int.from_bytes(stream.read(4), 'little')
    else:
        return int.from_bytes(stream.read(8), 'little')
    
def write_varint(stream, value: int):
    if value < 0xfd:
        stream.write(bytes([value]))
    elif value <= 0xffff:
        stream.write(b'\xfd' + value.to_bytes(2, 'little'))
    elif value <= 0xffffffff:
        stream.write(b'\xfe' + value.to_bytes(4, 'little'))
    else:
        stream.write(b'\xff' + value.to_bytes(8, 'little'))
    


    
def encode_varint(i):
    if i < 0xfd:
        return struct.pack("<B", i)
    elif i <= 0xffff:
        return b'\xfd' + struct.pack("<H", i)
    elif i <= 0xffffffff:
        return b'\xfe' + struct.pack("<I", i)
    else:
        return b'\xff' + struct.pack("<Q", i)
    
class CInv:
    def __init__(self, type_, hash_):
        self.type = type_  # e.g., MSG_TX (1) or MSG_BLOCK (2)
        self.hash = hash_  # 32-byte tx/block hash

    def serialize(self):
        return self.type.to_bytes(4, 'little') + self.hash[::-1]

    def __repr__(self):
        return f"CInv(type={self.type}, hash={self.hash.hex()[:8]}...)"
    
async def read_message(reader, magic: bytes = b"\xf9\xbe\xb4\xd9"):
    try:
        header = await read_exact(reader, 24)
        if not header:
            raise ConnectionError("Peer closed connection")

        msg_magic, command, length, checksum = struct.unpack("<4s12sI4s", header)
        
        if msg_magic != magic:
            raise ValueError(f"Bad magic: {msg_magic.hex()}")
            
        if length > 10_000_000:  # Sanity check
            raise ValueError(f"Suspicious length: {length}")
            
        payload = await read_exact(reader, length) if length > 0 else b""
        actual_checksum = dsha256(payload)[:4]

        if actual_checksum != checksum:
            raise ValueError("Checksum mismatch")

        command = command.rstrip(b'\x00').decode()
        return command, payload
    
    except asyncio.IncompleteReadError:
        raise ConnectionError("Peer closed connection (EOF)")
    


async def send_message(writer, command: str, payload: bytes, magic: bytes = b"\xf9\xbe\xb4\xd9"):
    """Send a Bitcoin protocol message (async version)"""
    msg = make_message(command, payload, magic)
    writer.write(msg)
    await writer.drain()

# create_message function replaced by make_message above

if __name__ == "__main__":
    # test read_varint()
    from io import BytesIO
    payload = b'\x02' + b'\x01\x00\x00\x00' + b'\xaa'*32 + b'\x02\x00\x00\x00' + b'\xbb'*32
    stream = BytesIO(payload)
    print("Testing read_varint function: should read 2")
    count = read_varint(stream)  # reads 1 byte → value = 2
    print(f"Count: {count}")  # Should print 2