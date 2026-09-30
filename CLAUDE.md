# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the node

```bash
python pynode.py              # standard mode (multiple peers)
python pynode.py test         # single peer
python pynode.py offline      # no peers, RPC still available on port 8888
```

Standalone transaction validation test (safe to run while node is live — read-only):
```bash
python test_tx_validation.py
```

RPC call example:
```bash
curl -X POST http://localhost:8888 -H "Content-Type: application/json" -d '{"method":"getblockcount","params":[]}'
```

## Configuration

All settings live in `config.json` (required, single source of truth — see `config.info` for the reference). `core/config.py` is only the loader/exporter; it has no defaults, and a missing file or key is a fatal startup error. Key settings:

- `DOWNLOAD_BLOCKS_FROM_HEIGHT` — block backfill start height (-1 = auto-detect from first stored block)
- `ENABLE_TX_VALIDATION` — toggle mempool tx validation (one-line on/off)
- `DESIRED_NOO_OUTBOUNDS`, `MAX_PEERS`, `ENABLE_INBOUND`, `LISTEN_PORT`
- `FORK_HANDLER_MAX_DEPTH`

## Storage

Runtime data lives in the working directory the node is launched from. Launch from a dedicated folder outside the project (e.g. a sibling `pynode_runtime\`) to keep data files out of the repo:
- `headers.dat` — 80 bytes/header, sequential, append-only
- `blocks.dat` + `blocks.idx` — blocks indexed by hash (`hash(32) + offset(8) + size(4) + height(4)`)
- `txindex.cache` — binary TxIndex cache (magic `TXIC` + version + build_through + data)

Log: `logs/pynode.log` (also in runtime folder when node is running live).

## Architecture: strict 3-layer design

All cross-layer communication is **asyncio queues only** — never direct method calls.

```
PeerLink (p2p/peer_link.py)
  ↕ per-peer queue (PeerEvent)
NetworkManager (network/network_manager.py)
  ↕ bm_in_q (DataMessage) / bm_out_q (BMRequest)
BlockchainManager (blockchain/blockchain_manager.py)
```

**PeerLink** — TCP socket, Bitcoin P2P framing, handshake, ping/pong. No Bitcoin logic.

**NetworkManager** — Peer lifecycle, inventory routing, backfill distribution, peer selection. Only layer that knows about peers. Also handles `BlockBackfiller` requests from the shared `bm_out_q`.

**BlockchainManager** — Peer-blind Bitcoin logic: header chain, block storage, fork/reorg detection, mempool, tx validation. Never knows which peer anything came from.

**RPCServer** (`rpc/rpc_server.py`) — HTTP JSON-RPC on port 8888. Reads HeaderChain/BlockChain directly; sends commands to NM via `rpc_to_nm` queue.

**BlockBackfiller** (`blockchain/block_backfiller.py`) — Periodic gap scan (every 5 min), requests missing blocks by putting `BMRequest(type="need_blocks")` into `bm_out_q` (shared with BM).

**TxIndex** (`blockchain/tx_index.py`) — Memory-mapped sorted cache file (36 bytes/entry: txid_le + height uint32 LE after a 17-byte header), binary search O(log N) against the map — no resident RAM copy. Built after backfill completes via chunked external sort (~8M-entry sorted runs merged straight into the cache file; ~1 GB peak RAM). Uses `header_chain` as canonical source of truth — never indexes orphan blocks. Cache maps instantly at startup; full rebuild of the 840k+ range (~400M txs) takes roughly 45 min.

**tx_validator** (`blockchain/tx_validator.py`) — Tags transactions `valid`/`partial`/`invalid`. `partial` means some inputs reference blocks outside our stored range — expected for a partial node.

## Key conventions

**Byte order:** Internal storage is little-endian (LE). Display/logging uses big-endian (BE). Txids in TxIndex are stored LE; convert with `bytes.fromhex(txid_hex)[::-1]`.

**Segwit txid:** Must use non-witness serialization (skip marker + flag + witness data). The stream must seek to `start_pos + 6` (not `start_pos`) before re-reading for the second pass.

**Import discipline:** Never import NM from BM, never import BM from PL. Shared code goes in `core/`. `sys.dont_write_bytecode = True` is set in entry points to avoid stale `.pyc` files.

**Log prefixes:** `[PL{id}]` PeerLink, `[NM]` NetworkManager, `[BM]` BlockchainManager, `[RPC]` RPCServer, `[TxIndex]`, `[Backfiller]`.

**Logging:** `core/logging.py` replaces `sys.stdout` with a `TeeLogger` that writes to both console and file. Never use `sys.__stdout__` directly — it bypasses the tee and causes double output.

## Log filtering (PowerShell)

```powershell
# <runtime folder>\logs\filter.ps1
Get-Content -Path "pynode.log" -Wait |
    Select-String -Pattern "backfill|utxo|rpc|mempool|txindex|error|warn" |
    ForEach-Object {
        Write-Host $_.Line
        $_.Line | Add-Content "filtered.log"
    }
```
