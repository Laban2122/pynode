import asyncio
import os
import sys

# Disable bytecode cache to avoid stale imports during development
sys.dont_write_bytecode = True

from network.network_manager import NetworkManager
from blockchain.blockchain_manager import BlockchainManager
from blockchain.block_backfiller import BlockBackfiller
from blockchain.tx_index import TxIndex
from rpc.rpc_server import RPCServer
from core.logging import setup_global_logging, enable_console_filter
from core.config import DOWNLOAD_BLOCKS_FROM_HEIGHT

# Version info
VERSION = "0.9"
BUILD_DATE = "dev"


def report_block_storage(block_chain):
    blocks_file = block_chain.blocks_file

    if os.path.isfile(blocks_file):
        print(f"[MAIN] Found block file: {os.path.abspath(blocks_file)}")
        oldest_height = block_chain.get_first_block_height()
        youngest_height = block_chain.get_height()
        if oldest_height is None:
            print("[MAIN] Block file contains no indexed blocks")
        else:
            print(
                f"[MAIN] Stored block range: height {oldest_height:,} to "
                f"{youngest_height:,} ({block_chain.get_block_count():,} blocks)"
            )
        return

    print(f"[MAIN] Block file not found: {os.path.abspath(blocks_file)}")
    if DOWNLOAD_BLOCKS_FROM_HEIGHT >= 0:
        print(
            "[MAIN] Configured to download blocks starting at height "
            f"{DOWNLOAD_BLOCKS_FROM_HEIGHT:,}"
        )
    else:
        print(
            "[MAIN] Automatic mode: block storage starts with the first current "
            "block received"
        )


async def main():
    # Setup logging to file and console
    setup_global_logging("logs/pynode.log")

    # Enable console filtering to suppress ping/pong noise
    enable_console_filter(suppress_keywords=['ping', 'pong'], debug=False)

    # Banner
    print("="*60)
    print(f"Pynode v{VERSION} - Bitcoin Node | Build: {BUILD_DATE}")
    print("="*60)

    mode = sys.argv[1] if len(sys.argv) > 1 else "standard"
    print(f"Mode: {mode}")
    print()

    # Create queues for inter-subsystem communication
    bm_in_q = asyncio.Queue()
    bm_out_q = asyncio.Queue()
    rpc_to_nm = asyncio.Queue()  # RPC → NetworkManager commands

    # Initialize subsystems
    bm = BlockchainManager(bm_in_q, bm_out_q)
    report_block_storage(bm.block_chain)

    # Get current chain height for handshake
    start_height = bm.header_chain.get_height()
    print(f"[MAIN] Initial chain height: {start_height}")

    nm = NetworkManager(bm_in_q, bm_out_q, mode=mode, start_height=start_height, rpc_queue=rpc_to_nm, header_chain=bm.header_chain)

    # Event set by backfiller when it confirms zero missing blocks with a current header tip
    backfill_done = asyncio.Event()

    # In offline mode there are no peers — skip the backfill gate so TxIndex
    # builds immediately from whatever blocks are already on disk.
    if mode == "offline" and bm.block_chain.get_block_count() > 0:
        print(f"[MAIN] Offline mode — {bm.block_chain.get_block_count():,} blocks on disk, skipping backfill wait")
        backfill_done.set()

    # Initialize block backfiller (maintenance layer)
    backfiller = BlockBackfiller(
        out_queue=bm_out_q,
        header_chain=bm.header_chain,
        block_chain=bm.block_chain,
        start_height_config=DOWNLOAD_BLOCKS_FROM_HEIGHT,
        get_peer_count_func=lambda: len(nm.peerlinks),
        backfill_done=backfill_done,
    )

    # Transaction index: built in memory after backfill, enables fast tx lookup for validation
    # Cache lives alongside blocks.dat so subsequent startups skip the full block scan
    _cache_file = os.path.join(
        os.path.dirname(bm.block_chain.blocks_file) or '.', 'txindex.cache'
    )
    tx_index = TxIndex(bm.block_chain, bm.header_chain, backfill_done, cache_file=_cache_file)
    bm.tx_index = tx_index

    rpc = RPCServer(rpc_to_nm, header_chain=bm.header_chain, block_chain=bm.block_chain)

    try:
        await asyncio.gather(
            bm.run(),
            nm.run(),
            rpc.run(),
            backfiller.run(),
            tx_index.run(),
        )
    except KeyboardInterrupt:
        print("\n[MAIN] Shutting down...")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[MAIN] Interrupted")
