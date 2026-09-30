"""
RPC Server for Pynode
Provides HTTP JSON-RPC interface for node control.

Usage:
    curl -X POST http://localhost:8888 -d '{"method": "addnode", "params": ["192.168.1.100:8333", "onetry"]}'
"""

import asyncio
import json
from typing import Dict, Any, Optional
from aiohttp import web

# UTXO scanner for wallet queries
from rpc.utxo_scanner import UTXOScanner

# Bind address comes from config.json (rpc.host / rpc.port)
from core.config import RPC_HOST, RPC_PORT


class RPCServer:
    """
    HTTP JSON-RPC server for node control.
    Routes commands to NetworkManager via queue.
    Read-only queries can access HeaderChain directly.
    """
    
    def __init__(self, rpc_to_nm: asyncio.Queue, port: int = RPC_PORT, host: str = RPC_HOST,
                 header_chain: Any = None, block_chain: Any = None):
        self.rpc_to_nm = rpc_to_nm
        self.port = port
        self.host = host
        self.header_chain = header_chain  # Read-only access for queries
        self.block_chain = block_chain    # Read-only access for UTXO queries

        # Initialize UTXO scanner if block_chain available
        if block_chain:
            self.utxo_scanner = UTXOScanner(block_chain)
        else:
            self.utxo_scanner = None

        # Per-address locks to prevent concurrent scans for the same address
        self._scan_locks: dict = {}
        
        self.app = web.Application()
        self._setup_routes()
    
    def _setup_routes(self):
        """Configure HTTP routes"""
        self.app.router.add_post('/', self._handle_rpc)
        self.app.router.add_get('/', self._handle_info)
    
    async def _handle_info(self, request: web.Request) -> web.Response:
        """GET / - Return server info"""
        info = {
            "server": "Pynode RPC",
            "version": "0.2",
            "methods": [
                "addnode", "getblockheader", "getblockhash", "getblockchaininfo",
                "getnetworkinfo", "getblockcount", "scantxoutset", "flushutxocache", "sendrawtransaction"
            ]
        }
        return web.json_response(info)
    
    async def _handle_rpc(self, request: web.Request) -> web.Response:
        """POST / - Handle JSON-RPC request"""
        try:
            # Parse JSON body
            try:
                data = await request.json()
            except json.JSONDecodeError:
                return self._error_response(-32700, "Parse error")
            
            # Extract method and params
            method = data.get("method")
            params = data.get("params", [])
            req_id = data.get("id")
            
            if not method:
                return self._error_response(-32600, "Invalid request: missing method", req_id)
            
            # Dispatch to handler
            result = await self._dispatch(method, params)
            
            if "error" in result:
                return self._error_response(result["error"]["code"], result["error"]["message"], req_id)
            
            return self._success_response(result.get("result"), req_id)
            
        except Exception as e:
            print(f"[RPC] ❌ Error handling request: {e}")
            return self._error_response(-32603, f"Internal error: {str(e)}")
    
    async def _dispatch(self, method: str, params: list) -> Dict[str, Any]:
        """Dispatch RPC method to appropriate handler"""
        handlers = {
            "addnode": self._handle_addnode,
            "getblockheader": self._handle_getblockheader,
            "getblockhash": self._handle_getblockhash,
            "getblockchaininfo": self._handle_getblockchaininfo,
            "getnetworkinfo": self._handle_getnetworkinfo,
            # Wallet methods:
            "getblockcount": self._handle_getblockcount,
            "scantxoutset": self._handle_scantxoutset,
            "flushutxocache": self._handle_flushutxocache,
            "sendrawtransaction": self._handle_sendrawtransaction,
        }
        
        handler = handlers.get(method)
        if not handler:
            return {"error": {"code": -32601, "message": f"Method not found: {method}"}}
        
        return await handler(params)
    
    async def _handle_addnode(self, params: list) -> Dict[str, Any]:
        """
        Handle addnode RPC command.
        
        params: ["node", "command"]
            node: IP:port string (e.g., "192.168.1.100:8333")
            command: "add" | "remove" | "onetry"
        
        Bitcoin Core compatible.
        """
        # Validate params
        if len(params) < 2:
            return {"error": {"code": -1, "message": "addnode requires 2 parameters: node and command"}}
        
        node = params[0]
        action = params[1].lower()
        
        if action not in ["add", "remove", "onetry"]:
            return {"error": {"code": -1, "message": f"Invalid command: {action}. Use add, remove, or onetry"}}
        
        # Parse node address (handle IPv6 with brackets)
        try:
            if node.startswith("["):
                # IPv6 format: [ipv6]:port
                if "]:" in node:
                    host = node[1:node.index("]")]
                    port = int(node[node.index("]:")+2:])
                else:
                    # No port, just [ipv6]
                    host = node[1:-1] if node.endswith("]") else node[1:]
                    port = 8333
            elif ":" in node:
                # IPv4:port or hostname:port
                host, port_str = node.rsplit(":", 1)
                port = int(port_str)
            else:
                host = node
                port = 8333  # Default Bitcoin port
        except ValueError:
            return {"error": {"code": -1, "message": f"Invalid node address: {node}"}}
        
        # Create RPC command message
        rpc_command = {
            "type": "rpc_addnode",
            "host": host,
            "port": port,
            "action": action
        }
        
        # Fire and forget - put on queue
        await self.rpc_to_nm.put(rpc_command)
        
        print(f"[RPC] 📤 addnode {host}:{port} {action}")
        
        # Return success (fire-and-forget, no result confirmation)
        return {"result": None}
    
    async def _handle_getblockheader(self, params: list) -> Dict[str, Any]:
        """
        Handle getblockheader RPC command.
        
        params: [blockhash, verbose]
            blockhash: Block hash (hex string, required)
            verbose: Boolean (optional, ignored - always returns JSON)
        
        Returns formatted header fields.
        Bitcoin Core compatible.
        """
        import struct
        import hashlib
        from datetime import datetime
        
        # Check if header_chain is available
        if not self.header_chain:
            return {"error": {"code": -1, "message": "HeaderChain not available"}}
        
        # Validate params
        if len(params) < 1:
            return {"error": {"code": -1, "message": "getblockheader requires blockhash parameter"}}
        
        blockhash_hex = params[0]
        # Second parameter (verbose) is ignored - we always return JSON
        
        # Parse blockhash (hex string, display order = big-endian)
        try:
            blockhash = bytes.fromhex(blockhash_hex)
            if len(blockhash) != 32:
                return {"error": {"code": -1, "message": "Invalid blockhash: must be 32 bytes (64 hex chars)"}}
            # Reverse to internal format (little-endian)
            blockhash_internal = blockhash[::-1]
        except ValueError:
            return {"error": {"code": -1, "message": "Invalid blockhash: not valid hex"}}
        
        # Search for header by hash
        header = None
        height = None
        for h in range(len(self.header_chain.headers)):
            candidate = self.header_chain.headers[h]
            candidate_hash = self.header_chain._get_header_hash(candidate)
            if candidate_hash == blockhash_internal:
                header = candidate
                height = h
                break
        
        if header is None or height is None:
            return {"error": {"code": -5, "message": "Block not found"}}
        
        # Get header from found height
        # header = self.header_chain.headers[height]
        
        # Parse header fields (80 bytes)
        version = struct.unpack('<I', header[0:4])[0]
        prev_hash = header[4:36][::-1].hex()  # Reverse for display
        merkle_root = header[36:68][::-1].hex()  # Reverse for display
        timestamp = struct.unpack('<I', header[68:72])[0]
        bits = struct.unpack('<I', header[72:76])[0]
        nonce = struct.unpack('<I', header[76:80])[0]
        
        # Calculate block hash
        block_hash = hashlib.sha256(hashlib.sha256(header).digest()).digest()[::-1].hex()
        
        # Format timestamp
        time_str = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S UTC')
        
        # Decode bits to target
        exponent = bits >> 24
        mantissa = bits & 0x007fffff
        target = mantissa * (2 ** (8 * (exponent - 3)))
        
        result = {
            "height": height,
            "hash": blockhash_hex,
            "version": version,
            "versionHex": hex(version),
            "previousblockhash": prev_hash if height > 0 else "0" * 64,
            "merkleroot": merkle_root,
            "time": timestamp,
            "timeStr": time_str,
            "bits": hex(bits),
            "nonce": nonce,
            "difficulty": self._bits_to_difficulty(bits)
        }
        
        print(f"[RPC] 📄 getblockheader {blockhash_hex[:16]}... -> height {height}")
        
        return {"result": result}
    
    def _bits_to_difficulty(self, bits: int) -> float:
        """Convert bits to difficulty"""
        # Genesis block bits for difficulty 1
        genesis_bits = 0x1d00ffff
        
        # Calculate targets
        exp = bits >> 24
        mant = bits & 0x007fffff
        target = mant * (2 ** (8 * (exp - 3)))
        
        exp_genesis = genesis_bits >> 24
        mant_genesis = genesis_bits & 0x007fffff
        target_genesis = mant_genesis * (2 ** (8 * (exp_genesis - 3)))
        
        if target == 0:
            return 0.0
        
        return target_genesis / target
    
    async def _handle_getnetworkinfo(self, params: list) -> Dict[str, Any]:
        """
        Handle getnetworkinfo RPC command.
        
        Returns network information including local addresses and listening port.
        """
        import socket
        from core.config import LISTEN_PORT, LISTEN_HOST, ENABLE_INBOUND, PROTOCOL_VERSION, USER_AGENT
        
        # Get local IP addresses
        local_addresses = []
        
        try:
            # Get hostname
            hostname = socket.gethostname()
            
            # Get all IP addresses for this host
            addr_info = socket.getaddrinfo(hostname, None)
            seen = set()
            
            for family, _, _, _, sockaddr in addr_info:
                ip: str = sockaddr[0]
                if ip not in seen and not ip.startswith('127.') and ip != '::1':
                    seen.add(ip)
                    addr_type = "ipv6" if family == socket.AF_INET6 else "ipv4"
                    local_addresses.append({
                        "address": ip,
                        "port": LISTEN_PORT,
                        "type": addr_type
                    })
            
            # Also try to get public IP
            try:
                # Connect to external server to determine outbound IP
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                outbound_ip = s.getsockname()[0]
                s.close()
                if outbound_ip not in seen:
                    local_addresses.insert(0, {
                        "address": outbound_ip,
                        "port": LISTEN_PORT,
                        "type": "ipv4",
                        "note": "outbound"
                    })
            except Exception:
                pass
                
        except Exception as e:
            local_addresses.append({"error": str(e)})
        
        # Get chain height
        chain_height = -1
        if self.header_chain:
            chain_height = self.header_chain.get_height()
        
        result = {
            "version": PROTOCOL_VERSION,
            "subversion": USER_AGENT,
            "listening": ENABLE_INBOUND,
            "listenport": LISTEN_PORT,
            "listenhost": LISTEN_HOST,
            "localaddresses": local_addresses,
            "headers": chain_height,
        }
        
        print(f"[RPC] 📡 getnetworkinfo - {len(local_addresses)} local addresses, port {LISTEN_PORT}")
        
        return {"result": result}
    
    # ─────────────────────────────────────────────────────────────
    # Wallet RPC Methods
    # ─────────────────────────────────────────────────────────────
    
    async def _handle_getblockhash(self, params: list) -> Dict[str, Any]:
        """
        Handle getblockhash RPC command.
        
        Returns hash of block in best-block-chain at height provided.
        
        params: [height]
            height: Block height (numeric, required)
        
        Returns: Block hash as hex string
        Bitcoin Core compatible.
        """
        if not self.header_chain:
            return {"error": {"code": -1, "message": "HeaderChain not available"}}
        
        # Validate params
        if len(params) < 1:
            return {"error": {"code": -1, "message": "getblockhash requires height parameter"}}
        
        # Parse height
        try:
            height = int(params[0])
        except (ValueError, TypeError):
            return {"error": {"code": -1, "message": f"Invalid height: {params[0]}"}}
        
        # Get chain height
        chain_height = self.header_chain.get_height()
        
        # Check bounds
        if height < 0 or height > chain_height:
            return {"error": {"code": -8, "message": f"Block height out of range (0-{chain_height})"}}
        
        # Get header at height
        header = self.header_chain.headers[height]
        
        # Calculate hash using HeaderChain's method
        block_hash = self.header_chain._get_header_hash(header)
        
        # Reverse for display format (Bitcoin Core compatible)
        block_hash_hex = block_hash[::-1].hex()
        
        print(f"[RPC] 🔍 getblockhash {height} -> {block_hash_hex[:16]}...")
        
        return {"result": block_hash_hex}
    
    async def _handle_getblockchaininfo(self, params: list) -> Dict[str, Any]:
        """
        Handle getblockchaininfo RPC command.
        
        Returns information about the current state of the blockchain.
        Bitcoin Core compatible (subset).
        """
        import struct
        
        if not self.header_chain:
            return {"error": {"code": -1, "message": "HeaderChain not available"}}
        
        # Get heights
        headers_height = self.header_chain.get_height()
        blocks_height = self.block_chain.get_height() if self.block_chain else 0
        
        # Get tip header and hash
        tip_header = self.header_chain.headers[-1]
        tip_hash = self.header_chain._get_header_hash(tip_header)
        bestblockhash = tip_hash[::-1].hex()  # Reverse for display
        
        # Extract bits from tip header and calculate difficulty
        bits = struct.unpack('<I', tip_header[72:76])[0]
        difficulty = self._bits_to_difficulty(bits)
        
        # Determine pruned status
        pruned = False
        pruneheight = None
        if self.block_chain:
            first_height = self.block_chain.get_first_block_height()
            if first_height is not None and first_height > 0:
                pruned = True
                pruneheight = first_height
        
        # Build result
        result = {
            "chain": "main",
            "blocks": blocks_height,
            "headers": headers_height,
            "bestblockhash": bestblockhash,
            "difficulty": difficulty,
            "pruned": pruned
        }
        
        # Add pruneheight only if pruned
        if pruned and pruneheight is not None:
            result["pruneheight"] = pruneheight
        
        print(f"[RPC] ⛓️  getblockchaininfo -> blocks: {blocks_height}, headers: {headers_height}, pruned: {pruned}")
        
        return {"result": result}
    
    async def _handle_getblockcount(self, params: list) -> Dict[str, Any]:
        """
        Handle getblockcount RPC command.
        
        Returns current blockchain height.
        Bitcoin Core compatible.
        """
        if not self.header_chain:
            return {"error": {"code": -1, "message": "HeaderChain not available"}}
        
        height = self.header_chain.get_height()
        print(f"[RPC] 📊 getblockcount -> {height}")
        
        return {"result": height}
    
    async def _handle_scantxoutset(self, params: list) -> Dict[str, Any]:
        """
        Handle scantxoutset RPC command.
        
        Scans the UTXO set for outputs matching a descriptor/address.
        
        params: ["start", [descriptors]]
            descriptors: List of output descriptors, e.g., ["addr(bc1q...)"]
        
        Bitcoin Core compatible (subset).
        """
        import struct  # Import at function level to avoid scoping issues
        
        if not self.utxo_scanner:
            return {"error": {"code": -1, "message": "UTXO scanner not available (no block storage)"}}
        
        # Parse params
        if len(params) < 2:
            return {"error": {"code": -1, "message": "scantxoutset requires action and descriptors"}}
        
        action = params[0]
        descriptors = params[1]
        
        if action != "start":
            return {"error": {"code": -1, "message": f"Unsupported action: {action}. Use 'start'"}}
        
        if not isinstance(descriptors, list) or len(descriptors) == 0:
            return {"error": {"code": -1, "message": "descriptors must be a non-empty list"}}
        
        # Extract address from descriptor
        # Format: "addr(bc1q...)" or just "bc1q..."
        descriptor = descriptors[0]
        if descriptor.startswith("addr(") and descriptor.endswith(")"):
            address = descriptor[5:-1]
        else:
            address = descriptor
        
        # Run scan (may take time for large blockchains)
        print(f"[RPC] 🔍 scantxoutset for {address[:20]}...")
        
        try:
            # Per-address lock: only one scan at a time; queued requests get the cached result
            if address not in self._scan_locks:
                self._scan_locks[address] = asyncio.Lock()

            import asyncio as _asyncio
            loop = _asyncio.get_event_loop()
            async with self._scan_locks[address]:
                utxos = await loop.run_in_executor(
                    None,
                    self.utxo_scanner.get_utxos_for_rpc,
                    address
                )
            
            total_amount = sum(u["amount"] for u in utxos)
            
            # Get current block height and time for freshness info
            current_height = self.block_chain.get_height()
            lastblock_timestamp = 0  #
            firstblock_timestamp = 0  
            try:
                # Get timestamp from latest block header
                header_data = self.header_chain.headers[current_height]
                if header_data and len(header_data) >= 80:
                    # Timestamp is at offset 68 (4 bytes, little-endian)
                    lastblock_timestamp = struct.unpack('<I', header_data[68:72])[0]
            except Exception:
                pass
            
            # Get first block timestamp for pruning info
            
            try:
                first_height = self.block_chain.get_first_block_height()
                if first_height is not None:
                    first_block_data = self.block_chain.get_block_by_height(first_height)
                    if first_block_data and len(first_block_data) >= 80:
                        # Extract timestamp from header (bytes 68-72)
                        firstblock_timestamp = struct.unpack('<I', first_block_data[68:72])[0]
            except Exception as e:
                print(f"[RPC] Warning: Could not get first block timestamp: {e}")

            result = {
                "success": True,
                "txouts": len(utxos),
                "total_amount": total_amount,
                "height": current_height,
                "lastblock_timestamp": lastblock_timestamp,
                "firstblock_timestamp": firstblock_timestamp,
                "unspents": utxos
            }
            
            print(f"[RPC] ✅ scantxoutset found {len(utxos)} UTXOs, total {total_amount:.8f} BTC")
            
            return {"result": result}
            
        except Exception as e:
            print(f"[RPC] ❌ scantxoutset error: {e}")
            return {"error": {"code": -1, "message": str(e)}}
    
    async def _handle_flushutxocache(self, params: list) -> Dict[str, Any]:
        if not self.utxo_scanner:
            return {"error": {"code": -1, "message": "UTXO scanner not available"}}

        count = len(self.utxo_scanner.last_scanned)
        self.utxo_scanner.utxos.clear()
        self.utxo_scanner.last_scanned.clear()
        self._scan_locks.clear()

        print(f"[RPC] 🗑️  flushutxocache — cleared {count} cached address(es)")
        return {"result": {"flushed": count}}

    async def _handle_sendrawtransaction(self, params: list) -> Dict[str, Any]:
        """
        Handle sendrawtransaction RPC command.
        
        Broadcasts a signed raw transaction to the network.
        
        params: [hex_tx, (maxfeerate)]
            hex_tx: Serialized transaction in hex
            maxfeerate: Optional max fee rate (ignored for now)
        
        Returns: txid on success
        Bitcoin Core compatible.
        """
        if len(params) < 1:
            return {"error": {"code": -1, "message": "sendrawtransaction requires hex transaction"}}
        
        hex_tx = params[0]
        
        # Validate hex
        try:
            tx_bytes = bytes.fromhex(hex_tx)
        except ValueError:
            return {"error": {"code": -1, "message": "Invalid hex encoding"}}
        
        if len(tx_bytes) < 60:
            return {"error": {"code": -1, "message": "Transaction too short"}}
        
        # Calculate txid (double SHA256 of non-witness serialization)
        import hashlib
        
        # Check if this is a SegWit transaction (marker byte 0x00, flag byte 0x01 after version)
        # Structure: version(4) + marker(1) + flag(1) + inputs... + outputs... + witness... + locktime(4)
        is_segwit = len(tx_bytes) > 6 and tx_bytes[4] == 0x00 and tx_bytes[5] == 0x01
        
        if is_segwit:
            # SegWit: txid is hash of non-witness serialization
            # We need to reconstruct: version + inputs + outputs + locktime (skip marker/flag/witness)
            txid_bytes = self._get_segwit_txid_bytes(tx_bytes)
            txid = hashlib.sha256(hashlib.sha256(txid_bytes).digest()).digest()
        else:
            # Non-SegWit: txid is hash of full transaction
            txid = hashlib.sha256(hashlib.sha256(tx_bytes).digest()).digest()
        txid_hex = txid[::-1].hex()  # Reverse for display
        
        # Send to NetworkManager for broadcast
        rpc_command = {
            "type": "rpc_sendrawtransaction",
            "tx_hex": hex_tx,
            "txid": txid_hex
        }
        
        await self.rpc_to_nm.put(rpc_command)
        
        print(f"[RPC] 📤 sendrawtransaction {txid_hex[:16]}...")
        
        # Return txid immediately (fire-and-forget broadcast)
        return {"result": txid_hex}
    
    def _success_response(self, result: Any, req_id: Any = None) -> web.Response:
        """Build JSON-RPC success response"""
        response = {
            "result": result,
            "error": None,
            "id": req_id
        }
        return web.json_response(response)
    
    def _error_response(self, code: int, message: str, req_id: Any = None) -> web.Response:
        """Build JSON-RPC error response"""
        response = {
            "result": None,
            "error": {
                "code": code,
                "message": message
            },
            "id": req_id
        }
        return web.json_response(response, status=200)  # JSON-RPC returns 200 even for errors
    
    def _get_segwit_txid_bytes(self, tx_bytes: bytes) -> bytes:
        """
        Extract non-witness serialization from SegWit transaction for txid calculation.
        
        SegWit format: version(4) + marker(1) + flag(1) + inputs + outputs + witness + locktime(4)
        Non-witness:   version(4) + inputs + outputs + locktime(4)
        """
        from io import BytesIO
        import struct
        
        stream = BytesIO(tx_bytes)
        
        # Read version (4 bytes)
        version = stream.read(4)
        
        # Skip marker (0x00) and flag (0x01)
        stream.read(2)
        
        # Read varint for input count
        def read_varint(s):
            n = s.read(1)[0]
            if n < 0xfd:
                return n
            elif n == 0xfd:
                return struct.unpack('<H', s.read(2))[0]
            elif n == 0xfe:
                return struct.unpack('<I', s.read(4))[0]
            else:
                return struct.unpack('<Q', s.read(8))[0]
        
        def varint_bytes(n):
            if n < 0xfd:
                return bytes([n])
            elif n <= 0xffff:
                return b'\xfd' + struct.pack('<H', n)
            elif n <= 0xffffffff:
                return b'\xfe' + struct.pack('<I', n)
            else:
                return b'\xff' + struct.pack('<Q', n)
        
        # Count position before reading inputs
        input_count = read_varint(stream)
        
        # Read all inputs (each: txid(32) + vout(4) + script_len(varint) + script + sequence(4))
        inputs_data = varint_bytes(input_count)
        for _ in range(input_count):
            txid = stream.read(32)
            vout = stream.read(4)
            script_len = read_varint(stream)
            script = stream.read(script_len)
            sequence = stream.read(4)
            inputs_data += txid + vout + varint_bytes(script_len) + script + sequence
        
        # Read output count and outputs
        output_count = read_varint(stream)
        outputs_data = varint_bytes(output_count)
        for _ in range(output_count):
            value = stream.read(8)
            script_len = read_varint(stream)
            script = stream.read(script_len)
            outputs_data += value + varint_bytes(script_len) + script
        
        # Skip witness data (one stack per input)
        for _ in range(input_count):
            stack_items = read_varint(stream)
            for _ in range(stack_items):
                item_len = read_varint(stream)
                stream.read(item_len)
        
        # Read locktime (last 4 bytes)
        locktime = stream.read(4)
        
        # Reconstruct non-witness serialization
        return version + inputs_data + outputs_data + locktime
        
    async def _watchdog_loop(self, restart_event: asyncio.Event) -> None:
        """
        Self-ping every 30 s. After 2 consecutive failures, signals restart_event.
        Backup guard: fires if the exception handler misses the accept failure.
        """
        INTERVAL    = 60   # seconds between pings
        TIMEOUT     = 8    # seconds to wait for a response
        MAX_FAIL    = 2    # consecutive failures before restart

        await asyncio.sleep(INTERVAL)   # let the server settle before first ping
        failures = 0

        while not restart_event.is_set():
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port), timeout=TIMEOUT
                )
                body = b'{"method":"getblockcount","params":[]}'
                writer.write(
                    f"POST / HTTP/1.1\r\nHost: {self.host}:{self.port}\r\n"
                    f"Content-Type: application/json\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    f"Connection: close\r\n\r\n".encode() + body
                )
                await writer.drain()
                data = await asyncio.wait_for(reader.read(256), timeout=TIMEOUT)
                writer.close()
                await writer.wait_closed()
                if not data:
                    raise ConnectionError("empty response")
                failures = 0

            except asyncio.CancelledError:
                raise
            except Exception as e:
                failures += 1
                print(f"[RPC] ⚠️  Watchdog ping failed ({failures}/{MAX_FAIL}): {e}")
                if failures >= MAX_FAIL:
                    print(f"[RPC] 🔁 Watchdog triggering restart after {failures} consecutive failures")
                    restart_event.set()
                    return

            await asyncio.sleep(INTERVAL)

    async def run(self):
        """
        Run the RPC server.

        Two-layer defence against the Windows ProactorEventLoop / WinError 64 freeze:

        Fix 1 — exception handler: catches OSError(WinError 64) from asyncio's
          accept_coro before it silently kills the accept transport, and signals
          an immediate restart.

        Fix 2 — watchdog: self-pings every 30 s; restarts after 2 consecutive
          failures in case the exception handler misses a future variant of the bug.
        """
        loop = asyncio.get_running_loop()
        restart_count = 0

        # ── Fix 1: event-loop exception handler ───────────────────────────────
        # asyncio creates an internal accept_coro task whose exception is never
        # retrieved. WinError 64 (ERROR_NETNAME_DELETED) kills the AcceptEx
        # operation and stops all future accepts. We intercept it here.
        restart_event = asyncio.Event()
        _original_exc_handler = loop.get_exception_handler()

        def _exc_handler(loop, context):
            exc = context.get('exception')
            msg = context.get('message', '')
            future = context.get('future')
            if isinstance(exc, OSError) and getattr(exc, 'winerror', None) == 64:
                future_str = str(future) if future else ''
                if 'accept' in msg.lower() or 'accept_coro' in future_str:
                    print(f"[RPC] ⚠️  WinError 64 on accept transport — triggering restart")
                    restart_event.set()
                    return
            if _original_exc_handler:
                _original_exc_handler(loop, context)
            else:
                loop.default_exception_handler(context)

        loop.set_exception_handler(_exc_handler)

        # ── Restart loop ───────────────────────────────────────────────────────
        while True:
            restart_event.clear()

            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, self.host, self.port)

            try:
                await site.start()
            except OSError as e:
                print(f"[RPC] ❌ Failed to start: {e} (port {self.port} may be in use)")
                await runner.cleanup()
                await asyncio.sleep(5)
                continue

            if restart_count == 0:
                print(f"[RPC] 🎧 Listening on http://{self.host}:{self.port}")
            else:
                print(f"[RPC] ✅ Restarted (#{restart_count}) — listening on http://{self.host}:{self.port}")

            # ── Fix 2: watchdog runs alongside the server ──────────────────────
            watchdog = asyncio.create_task(
                self._watchdog_loop(restart_event), name="rpc-watchdog"
            )

            try:
                await restart_event.wait()   # blocks until Fix 1 or Fix 2 fires
            except asyncio.CancelledError:
                # Node is shutting down — clean up and exit
                watchdog.cancel()
                await asyncio.gather(watchdog, return_exceptions=True)
                print("[RPC] Server shutting down...")
                await runner.cleanup()
                raise

            watchdog.cancel()
            await asyncio.gather(watchdog, return_exceptions=True)

            restart_count += 1
            print(f"[RPC] 🔄 Tearing down and restarting (#{restart_count})...")
            await runner.cleanup()
            await asyncio.sleep(1)   # brief pause before rebinding the port
