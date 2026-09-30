"""
Standalone transaction validation test.

Usage:
    python test_tx_validation.py

Reads blocks from the node's data directory (read-only). Safe to run while
pynode is live. Defaults to the current directory; override with the
PYNODE_DATA_DIR environment variable or by editing RUNTIME_DIR below.

To test your own transactions:
  1. Go to mempool.space and find your transaction
  2. Click the transaction, then "Details" -> "Raw Tx" (or append /hex to the URL)
  3. Copy the raw hex and paste it into MANUAL_TX_HEX below
"""
import sys
import os
import time
from io import BytesIO

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(__file__))

# ── Configuration ──────────────────────────────────────────────────────────────
RUNTIME_DIR = os.environ.get("PYNODE_DATA_DIR", ".")

# Paste raw tx hex from mempool.space here. Leave empty to use auto-sampled txs.
MANUAL_TX_HEX = [
    # "0200000001abcdef...",
]
# ──────────────────────────────────────────────────────────────────────────────

from blockchain.blockchain import BlockChain
from blockchain.headerchain import HeaderChain
from blockchain.tx_index import TxIndex
from blockchain.tx_validator import validate_txs, TAG_VALID, TAG_PARTIAL, TAG_INVALID
from core.helpers import parse_tx_full, read_varint


BLOCKS_FILE  = os.path.join(RUNTIME_DIR, "blocks.dat")
INDEX_FILE   = os.path.join(RUNTIME_DIR, "blocks.idx")
HEADERS_FILE = os.path.join(RUNTIME_DIR, "headers.dat")


def load_block_chain() -> BlockChain:
    print(f"Loading block index from {RUNTIME_DIR} ...")
    bc = BlockChain(blocks_file=BLOCKS_FILE, index_file=INDEX_FILE)
    if bc.get_block_count() == 0:
        print("ERROR: No blocks found. Check RUNTIME_DIR path.")
        sys.exit(1)
    return bc


def load_header_chain() -> HeaderChain:
    hc = HeaderChain(headers_file=HEADERS_FILE)
    print(f"[HeaderChain] Loaded {hc.get_height() + 1:,} headers (tip={hc.get_height()})")
    return hc


CACHE_FILE = os.path.join(RUNTIME_DIR, "txindex.cache")


def build_tx_index(block_chain: BlockChain, header_chain: HeaderChain) -> TxIndex:
    """Load TxIndex from cache or build from scratch — no asyncio needed."""
    idx = TxIndex(block_chain, header_chain, backfill_done=None,
                  cache_file=CACHE_FILE)

    t0 = time.time()
    if idx._try_load_cache():
        idx._fill_live_from_disk()
        print(f"TxIndex ready in {time.time() - t0:.1f}s (mapped from cache)\n")
    else:
        print("Building TxIndex (full scan of stored blocks)...")
        idx._build()
        print(f"TxIndex ready in {time.time() - t0:.1f}s (cache saved for next run)\n")

    idx.ready = True
    return idx


def _first_non_coinbase(block_data: bytes, height: int, label: str) -> dict | None:
    """Extract the first non-coinbase tx from a raw block. Returns None if not found."""
    stream   = BytesIO(block_data[80:])
    tx_count = read_varint(stream)
    for _ in range(tx_count):
        tx_start = stream.tell()
        tx       = parse_tx_full(stream)
        tx_end   = stream.tell()
        if not tx['is_coinbase']:
            return {
                'txid':    tx['txid'],
                'height':  height,
                'raw_hex': block_data[80 + tx_start: 80 + tx_end].hex(),
                'source':  label,
            }
    return None


def sample_txs_from_blocks(block_chain: BlockChain, count: int = 3) -> list[dict]:
    """Pick non-coinbase txs from the middle of the stored range (inputs likely in-range)."""
    samples = []
    heights = sorted(block_chain.index_by_height.keys())
    start   = len(heights) // 2

    for height in heights[start:]:
        if len(samples) >= count:
            break
        block_data = block_chain.get_block_by_height(height)
        if not block_data:
            continue
        s = _first_non_coinbase(block_data, height, f'auto (block {height})')
        if s:
            samples.append(s)

    return samples


def sample_early_tx(block_chain: BlockChain) -> dict | None:
    """Pick the first non-coinbase tx from the earliest stored block.
    Its inputs almost certainly reference pre-range blocks → expect PARTIAL tag."""
    heights    = sorted(block_chain.index_by_height.keys())
    first_h    = heights[0] if heights else None
    if first_h is None:
        return None
    block_data = block_chain.get_block_by_height(first_h)
    if not block_data:
        return None
    return _first_non_coinbase(
        block_data, first_h,
        f'early block {first_h} (expect PARTIAL — inputs pre-range)'
    )


def _tag_icon(tag: str) -> str:
    return {'valid': 'OK', 'partial': 'PARTIAL', 'invalid': 'INVALID'}.get(tag, '?')


def print_result(r: dict, source: str, elapsed_ms: float):
    icon = _tag_icon(r['tag'])
    print(f"  TXID   : {r['txid']}")
    print(f"  Source : {source}")
    print(f"  Tag    : {r['tag'].upper()} {icon}")
    print(f"  Time   : {elapsed_ms:.1f} ms")

    if r['detail'] == 'coinbase':
        print(f"  Type   : coinbase (no inputs to validate)")
    else:
        print(f"  Inputs : {r['inputs_resolved']} / {r['inputs_total']} resolved")
        if r['inputs_sum_sat'] is not None:
            print(f"  In     : {r['inputs_sum_sat']:,} sat  ({r['inputs_sum_sat']/1e8:.8f} BTC)")
        print(f"  Out    : {r['outputs_sum_sat']:,} sat  ({r['outputs_sum_sat']/1e8:.8f} BTC)")
        if r['fee_sat'] is not None:
            print(f"  Fee    : {r['fee_sat']:,} sat")
        if r['detail']:
            print(f"  Note   : {r['detail']}")
    print()


def main():
    print("=" * 60)
    print("Pynode - Transaction Validation Test")
    print("=" * 60)
    print()

    block_chain  = load_block_chain()
    header_chain = load_header_chain()
    tx_index     = build_tx_index(block_chain, header_chain)

    # Build test set: manual + auto-sampled
    test_txs = []

    if MANUAL_TX_HEX:
        for raw_hex in MANUAL_TX_HEX:
            test_txs.append({'raw_hex': raw_hex, 'source': 'manual (mempool.space)'})
        print(f"Manual transactions: {len(MANUAL_TX_HEX)}")
    else:
        print("No manual transactions — auto-sampling from stored blocks.")

    auto_samples = sample_txs_from_blocks(block_chain, count=3)
    for s in auto_samples:
        test_txs.append({'raw_hex': s['raw_hex'], 'source': s['source']})
    print(f"Auto-sampled transactions: {len(auto_samples)}")

    early = sample_early_tx(block_chain)
    if early:
        test_txs.append({'raw_hex': early['raw_hex'], 'source': early['source']})
        print(f"Early block transaction: block {early['height']}")
    print()

    if not test_txs:
        print("No transactions to test.")
        return

    print("-" * 60)
    print("Results")
    print("-" * 60)
    print()

    counts = {TAG_VALID: 0, TAG_PARTIAL: 0, TAG_INVALID: 0}
    for t in test_txs:
        t0 = time.time()
        r  = validate_txs([t['raw_hex']], tx_index, block_chain)[0]
        elapsed_ms = (time.time() - t0) * 1000
        print_result(r, t['source'], elapsed_ms)
        if r['tag'] in counts:
            counts[r['tag']] += 1

    print("-" * 60)
    print(f"Summary: {counts[TAG_VALID]} valid, "
          f"{counts[TAG_PARTIAL]} partial, "
          f"{counts[TAG_INVALID]} invalid  "
          f"(total {len(test_txs)})")
    print("-" * 60)


if __name__ == "__main__":
    main()
