"""
UTXO Scanner - Scans blocks to find UTXOs for a specific address

Used by RPC server for wallet UTXO queries.
Supports caching to avoid rescanning on restart.

Design:
- Scan blocks from blocks.dat
- Parse transactions to find outputs matching address
- Track spent outputs by checking inputs
- Cache results to JSON file
"""

import os
import json
import struct
import hashlib
from io import BytesIO
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict

import bech32


@dataclass
class UTXO:
    """Unspent transaction output"""
    txid: str           # Hex, big-endian display format
    vout: int           # Output index
    amount: int         # Satoshis
    height: int         # Block height
    scriptpubkey: str   # Hex


class UTXOScanner:
    """
    Scans blockchain for UTXOs belonging to a specific address.
    
    Uses incremental scanning - only scans new blocks since last scan.
    """
    
    def __init__(self, block_chain):
        """
        Args:
            block_chain: BlockChain instance for reading blocks
        """
        self.block_chain = block_chain
        
        # UTXO storage: {address: {(txid, vout): UTXO}}
        self.utxos: Dict[str, Dict[Tuple[str, int], UTXO]] = {}
        
        # Last scanned height per address
        self.last_scanned: Dict[str, int] = {}

    
    # ─────────────────────────────────────────────────────────────
    # Block Parsing
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def _read_varint(stream: BytesIO) -> int:
        """Read Bitcoin varint"""
        first = struct.unpack('<B', stream.read(1))[0]
        if first < 0xfd:
            return first
        elif first == 0xfd:
            return struct.unpack('<H', stream.read(2))[0]
        elif first == 0xfe:
            return struct.unpack('<I', stream.read(4))[0]
        else:
            return struct.unpack('<Q', stream.read(8))[0]
    
    @staticmethod
    def _double_sha256(data: bytes) -> bytes:
        """Bitcoin double SHA256"""
        return hashlib.sha256(hashlib.sha256(data).digest()).digest()
    
    def _parse_transaction(self, stream: BytesIO) -> Tuple[str, List[dict], List[dict]]:
        """
        Parse a single transaction from stream.
        
        Returns:
            (txid, inputs, outputs)
            - txid: hex string (big-endian display format)
            - inputs: list of {txid, vout}
            - outputs: list of {amount, scriptpubkey}
        """
        start_pos = stream.tell()
        
        # Version (4 bytes)
        version = struct.unpack('<I', stream.read(4))[0]
        
        # Check for SegWit marker
        marker_pos = stream.tell()
        marker = stream.read(2)
        
        is_segwit = (marker == b'\x00\x01')
        if not is_segwit:
            # Not SegWit, rewind
            stream.seek(marker_pos)
        
        # Input count
        input_count = self._read_varint(stream)
        
        inputs = []
        for _ in range(input_count):
            prev_txid = stream.read(32)[::-1].hex()  # Reverse for display
            prev_vout = struct.unpack('<I', stream.read(4))[0]
            script_len = self._read_varint(stream)
            stream.read(script_len)  # Skip scriptSig
            stream.read(4)  # Sequence
            
            inputs.append({
                "txid": prev_txid,
                "vout": prev_vout
            })
        
        # Output count
        output_count = self._read_varint(stream)
        
        outputs = []
        for i in range(output_count):
            amount = struct.unpack('<Q', stream.read(8))[0]
            script_len = self._read_varint(stream)
            scriptpubkey = stream.read(script_len)
            
            outputs.append({
                "amount": amount,
                "scriptpubkey": scriptpubkey.hex(),
                "vout": i
            })
        
        # Skip witness data if SegWit
        if is_segwit:
            for _ in range(input_count):
                witness_count = self._read_varint(stream)
                for _ in range(witness_count):
                    item_len = self._read_varint(stream)
                    stream.read(item_len)
        
        # Locktime (4 bytes)
        locktime = struct.unpack('<I', stream.read(4))[0]
        
        end_pos = stream.tell()
        
        # Calculate txid (hash of non-witness serialization)
        stream.seek(start_pos)
        if is_segwit:
            # For SegWit, txid excludes witness
            # Reconstruct non-witness serialization
            tx_data = struct.pack('<I', version)
            
            # Skip marker/flag, re-read inputs/outputs
            stream.seek(marker_pos + 2)  # After marker/flag
            
            input_count = self._read_varint(stream)
            tx_data += self._encode_varint(input_count)
            
            for _ in range(input_count):
                tx_data += stream.read(32)  # prev_txid
                tx_data += stream.read(4)   # prev_vout
                script_len = self._read_varint(stream)
                tx_data += self._encode_varint(script_len)
                tx_data += stream.read(script_len)
                tx_data += stream.read(4)   # sequence
            
            output_count = self._read_varint(stream)
            tx_data += self._encode_varint(output_count)
            
            for _ in range(output_count):
                tx_data += stream.read(8)   # amount
                script_len = self._read_varint(stream)
                tx_data += self._encode_varint(script_len)
                tx_data += stream.read(script_len)
            
            # Skip witness, add locktime
            for _ in range(input_count):
                witness_count = self._read_varint(stream)
                for _ in range(witness_count):
                    item_len = self._read_varint(stream)
                    stream.read(item_len)
            
            tx_data += struct.pack('<I', locktime)
            txid = self._double_sha256(tx_data)[::-1].hex()
        else:
            # Non-SegWit: hash entire transaction
            tx_bytes = stream.read(end_pos - start_pos)
            stream.seek(start_pos)
            tx_bytes = stream.read(end_pos - start_pos)
            txid = self._double_sha256(tx_bytes)[::-1].hex()
        
        stream.seek(end_pos)
        return txid, inputs, outputs
    
    @staticmethod
    def _encode_varint(n: int) -> bytes:
        """Encode integer as Bitcoin varint"""
        if n < 0xfd:
            return struct.pack('<B', n)
        elif n <= 0xffff:
            return struct.pack('<BH', 0xfd, n)
        elif n <= 0xffffffff:
            return struct.pack('<BI', 0xfe, n)
        else:
            return struct.pack('<BQ', 0xff, n)
    
    def _parse_block_transactions(self, block_data: bytes) -> List[Tuple[str, List[dict], List[dict]]]:
        """
        Parse all transactions in a block.
        
        Returns:
            List of (txid, inputs, outputs) tuples
        """
        stream = BytesIO(block_data)
        
        # Skip 80-byte header
        stream.read(80)
        
        # Transaction count
        tx_count = self._read_varint(stream)
        
        transactions = []
        for _ in range(tx_count):
            try:
                txid, inputs, outputs = self._parse_transaction(stream)
                transactions.append((txid, inputs, outputs))
            except Exception as e:
                print(f"[UTXOScanner] Error parsing transaction: {e}")
                break
        
        return transactions
    
    # ─────────────────────────────────────────────────────────────
    # Address Matching
    # ─────────────────────────────────────────────────────────────
    
    @staticmethod
    def _address_to_scriptpubkey(address: str) -> Optional[str]:
        """
        Convert Bitcoin address to scriptPubKey hex.
        
        Supports:
        - bech32 P2WPKH (bc1q...)
        - bech32 P2WSH (bc1q... 62 chars)
        """
        if address.startswith("bc1") or address.startswith("tb1"):
            # Bech32/Bech32m
            hrp = "bc" if address.startswith("bc") else "tb"
            decoded = bech32.bech32_decode(address)
            
            if decoded[0] is None:
                # Try bech32m
                decoded = bech32.bech32_decode(address)
                if decoded[0] is None:
                    return None
            
            witness_version = decoded[1][0]
            witness_program = bytes(bech32.convertbits(decoded[1][1:], 5, 8, False))
            
            if len(witness_program) == 20:
                # P2WPKH: OP_0 <20-byte-hash>
                return f"0014{witness_program.hex()}"
            elif len(witness_program) == 32:
                # P2WSH: OP_0 <32-byte-hash>
                return f"0020{witness_program.hex()}"
            else:
                return None
        
        # TODO: Add support for legacy addresses (1..., 3...)
        return None
    
    # ─────────────────────────────────────────────────────────────
    # Scanning
    # ─────────────────────────────────────────────────────────────
    
    def scan_address(self, address: str, force_rescan: bool = False) -> List[UTXO]:
        """
        Scan blockchain for UTXOs belonging to address.
        
        Uses incremental scanning - only scans new blocks since last scan.
        OPTIMIZED: Only parses blocks that contain the target scriptPubKey.
        
        Args:
            address: Bitcoin address (bech32)
            force_rescan: If True, rescan from beginning
            
        Returns:
            List of UTXOs for this address
        """
        # Convert address to scriptPubKey for matching
        target_spk = self._address_to_scriptpubkey(address)
        if not target_spk:
            print(f"[UTXOScanner] Unsupported address format: {address}")
            return []
        
        target_spk_bytes = bytes.fromhex(target_spk)
        
        # Initialize storage for this address if needed
        if address not in self.utxos:
            self.utxos[address] = {}
        
        # Determine scan range
        first_block = self.block_chain.get_first_block_height() or 0
        
        is_first_scan = address not in self.last_scanned
        
        if force_rescan:
            start_height = first_block
        elif is_first_scan:
            # First scan for this address - start from first available block (or min_height)
            start_height = first_block
        else:
            start_height = self.last_scanned[address] + 1
        
        end_height = self.block_chain.get_height()
        
        if start_height > end_height:
            # Already up to date
            return list(self.utxos[address].values())
        
        # Show helpful message for first-time scan
        if is_first_scan:
            blocks_to_scan = end_height - start_height + 1
            print(f"[UTXOScanner] First scan for {address[:16]}... (scanning {blocks_to_scan:,} blocks)")
        else:
            print(f"[UTXOScanner] Scanning blocks {start_height} to {end_height} for {address[:16]}...")
        
        # Track spent outputs (to remove from UTXOs)
        spent = set()  # {(txid, vout), ...}
        
        # Scan each block - FAST: only parse blocks containing our scriptPubKey
        blocks_scanned = 0
        blocks_with_match = 0
        
        for height in range(start_height, end_height + 1):
            block_data = self.block_chain.get_block_by_height(height)
            if not block_data:
                continue
            
            blocks_scanned += 1
            
            # OPTIMIZATION: Quick check if block is relevant
            # A block is relevant if:
            # 1. It contains our scriptPubKey (receiving to us), OR
            # 2. It contains any of our UTXO txids (spending from us)
            has_potential_match = target_spk_bytes in block_data
            
            # Also check if any of our UTXOs might be spent in this block
            has_potential_spend = False
            if self.utxos[address]:
                for (utxo_txid, _) in self.utxos[address].keys():
                    # Convert txid to bytes (little-endian as stored in inputs)
                    utxo_txid_bytes = bytes.fromhex(utxo_txid)[::-1]
                    if utxo_txid_bytes in block_data:
                        has_potential_spend = True
                        break
            
            if has_potential_match or has_potential_spend:
                blocks_with_match += 1
                transactions = self._parse_block_transactions(block_data)
                
                for txid, inputs, outputs in transactions:
                    # Check inputs (mark as spent) - only if we have UTXOs to track
                    if self.utxos[address]:
                        for inp in inputs:
                            spent.add((inp["txid"], inp["vout"]))
                    
                    # Check outputs (add as UTXO if matches address)
                    for out in outputs:
                        if out["scriptpubkey"] == target_spk:
                            key = (txid, out["vout"])
                            self.utxos[address][key] = UTXO(
                                txid=txid,
                                vout=out["vout"],
                                amount=out["amount"],
                                height=height,
                                scriptpubkey=out["scriptpubkey"]
                            )
            
            if blocks_scanned % 5000 == 0:
                print(f"[UTXOScanner]   Scanned {blocks_scanned} blocks ({blocks_with_match} with matches)...")
        
        # Remove spent outputs
        for key in spent:
            if key in self.utxos[address]:
                del self.utxos[address][key]
        
        # Update last scanned height
        self.last_scanned[address] = end_height
        
        result = list(self.utxos[address].values())
        print(f"[UTXOScanner] Found {len(result)} UTXOs for {address[:16]}...")
        
        return result
    
    def get_balance(self, address: str) -> int:
        """
        Get total balance for address in satoshis.
        
        Scans if needed, otherwise returns cached result.
        """
        utxos = self.scan_address(address)
        return sum(u.amount for u in utxos)
    
    def get_utxos_for_rpc(self, address: str) -> List[dict]:
        """
        Get UTXOs in RPC-compatible format.
        
        Returns format matching Bitcoin Core's scantxoutset.
        """
        utxos = self.scan_address(address)
        
        current_height = self.block_chain.get_height()
        
        result = []
        for u in utxos:
            result.append({
                "txid": u.txid,
                "vout": u.vout,
                "amount": u.amount / 100_000_000,  # Convert to BTC
                "satoshis": u.amount,
                "scriptPubKey": u.scriptpubkey,
                "height": u.height,
                "confirmations": current_height - u.height + 1 if u.height > 0 else 0
            })
        
        return result
