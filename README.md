# Pynode

**Pynode** is an experimental Bitcoin node implementation in Python focused on **clean architecture**, **strict separation of concerns**, and **queue-based async design**.

Its distinguishing feature is that it is a **partial node**: it syncs the full header chain but only stores blocks from a configurable height onward. You are connected to the real Bitcoin mainnet and serving RPC queries within minutes — no multi-day initial block download, no 600+ GB of disk.

This project is intended for **research / education / prototyping** and as a **network portal** for other systems — not as a production replacement for Bitcoin Core.

> **Status: experimental, not actively maintained.** The original author has moved on to other projects. Issues and PRs are reviewed occasionally. The code is published as a hackable, readable codebase for people who want to learn Bitcoin internals or build on top of it. Expect breaking changes; if you want a battle-tested full node, use **Bitcoin Core**.

---

## What this project is (and is not)

### ✅ Goals
- A readable Python implementation of Bitcoin P2P networking + chain processing primitives
- Strict layering so components remain testable and evolvable
- Practical tooling: header sync, block backfill, transaction index, basic RPC surface for external projects
- A codebase you can modify without cascading errors

### ❌ Non-goals
- Production-grade security hardening
- Full consensus parity with Bitcoin Core
- End-user wallet application development (a companion `wallet_server` project lives in a separate repository)

---

## Quickstart

### 1) Requirements
- Python 3.10+
- ~a few GB of disk (depends on how far back you set the block download height)

```bash
git clone <this-repo>
cd pynode
pip install -r requirements.txt
```

### 2) Set up a runtime folder

The node reads `config.json` from — and writes all data files to — the **working directory it is launched from**. Keep that outside the repo:

```bash
mkdir ../pynode_runtime
cp config.json ../pynode_runtime/
cd ../pynode_runtime
```

Edit `config.json` to taste. The most important setting is `blockchain.download_blocks_from_height` — the height blocks are stored from (`-1` = start from whatever the current tip is when you first connect). Every setting is documented in `config.info`.

### 3) Run the node

```bash
python ../pynode/pynode.py           # standard mode (multiple peers)
python ../pynode/pynode.py test      # single-peer mode
python ../pynode/pynode.py offline   # no peers; RPC still available
```

The node syncs headers first, then backfills blocks from your configured height. Progress is logged to console and `logs/pynode.log`.

Mode notes:
- `offline` disables all outbound/inbound peer connections, but local RPC still runs.
- `test` uses one initial sync peer, then normal connection maintenance continues.
- Initial peers come from `peer_cache.json` if present, otherwise `network.start_peers` in config, plus DNS seeding.

### 4) RPC

Pynode serves JSON-RPC over HTTP on port **8888** (localhost by default):

```bash
curl -X POST http://localhost:8888 \
  -H "Content-Type: application/json" \
  -d '{"method":"getblockcount","params":[]}'
```

Available methods: `getblockcount`, `getblockhash`, `getblockheader`, `getblockchaininfo`, `getnetworkinfo`, `addnode`, `scantxoutset`, `flushutxocache`, `sendrawtransaction`. See `rpc/rpc_server.py` for parameters.

---

## Architecture (3-layer design)

Pynode is organized around a strict 3-layer design; all cross-layer communication is **asyncio queues only** (no direct calls):

- **PeerLink (PL)** — *wire + socket only*
  - TCP connection, Bitcoin P2P framing, handshake, ping/pong, basic metrics
  - Emits standardized events upward; contains no Bitcoin logic
  - Location: `p2p/peer_link.py`

- **NetworkManager (NM)** — *peer-aware routing + scheduling*
  - Owns PeerLink lifecycle (inbound/outbound), peer metadata, inventory tracking
  - Parses network-level messages (`inv`, `addr`, etc.)
  - Routes `headers`/`block`/`tx` data to BM; distributes backfill requests across peers
  - Only layer that knows about peers
  - Location: `network/network_manager.py`

- **BlockchainManager (BM)** — *peer-blind Bitcoin logic*
  - Header chain, fork handling/reorg decisions, block storage, mempool, tx validation
  - Never knows which peer anything came from
  - Location: `blockchain/blockchain_manager.py`

Supporting services: **BlockBackfiller** (periodic gap scan + block requests), **TxIndex** (memory-mapped txid→height index over the stored block range), **RPCServer**.

For a detailed overview, see `docs/ARCHITECTURE.md`.

---

## Key features

- **Headers-first sync** with full header chain from genesis
- **Partial block storage** from a configurable height — operational in minutes
- **Block backfilling** — detects and fills gaps in the stored range automatically
- **Fork handling / reorg detection** (chainwork-based)
- **Transaction index** — mmap-backed binary-search lookup over hundreds of millions of txs with negligible RAM
- **Mempool with tx validation** — txs tagged `valid`/`partial`/`invalid` (`partial` = inputs reference blocks outside the stored range, expected for a partial node)
- **Local JSON-RPC server** for node control and queries

---

## Repository layout

```
pynode/
├── pynode.py                  # Entry point, queue wiring
├── config.json                # Runtime configuration (single source of truth)
├── config.info                # Reference documentation for every config key
├── p2p/
│   ├── peer_link.py           # PeerLink (socket + framing)
│   └── wire.py                # Wire helpers
├── network/
│   └── network_manager.py     # NetworkManager (peer routing + scheduling)
├── blockchain/
│   ├── blockchain_manager.py  # BlockchainManager (peer-blind chain logic)
│   ├── headerchain.py         # HeaderChain (headers-first)
│   ├── blockchain.py          # Block storage (blocks.dat + hash index)
│   ├── fork_handler.py        # Fork/reorg detection
│   ├── block_backfiller.py    # Backfill service
│   ├── tx_index.py            # mmap txid→height index
│   └── tx_validator.py        # Mempool tx validation
├── rpc/
│   ├── rpc_server.py          # Local JSON-RPC server
│   └── utxo_scanner.py        # UTXO scanning (scantxoutset)
├── core/                      # Shared: types, config loader, helpers, logging
├── tools/                     # Standalone utilities (DNS seed probe, ...)
├── docs/                      # Architecture, backfilling, fork handling, TODO
├── test_tx_validation.py      # Standalone validation test (safe on a live node)
└── stress_rpc.py              # RPC robustness stress test
```

---

## Documentation

- Architecture overview: `docs/ARCHITECTURE.md`
- Block backfilling: `docs/BACKFILLING.md`
- Fork handling: `docs/FORKHANDLING.md`
- Roadmap / TODOs: `docs/TODO.md`
- Config reference: `config.info`
- Agent/contributor conventions: `CLAUDE.md`

---

## Contributing

Contributions are welcome, especially:
- tests (unit + integration)
- protocol correctness audits
- performance improvements (backfill, parsing)
- better docs + diagrams
- items from `docs/TODO.md` (a local UTXO set is the big one)

### Design rules (important)
This repo enforces strict boundaries:
- PeerLink **must not** implement Bitcoin logic.
- BlockchainManager **must not** know about peers/sockets.
- Cross-layer communication is **queue-based** only.

Please read `docs/ARCHITECTURE.md` and `CLAUDE.md` before large changes.

---

## Security notes / disclaimer

- This is experimental software with **no security hardening**. It has not been audited.
- The RPC server has **no authentication**. Keep it bound to localhost (the default) and never expose port 8888 to the internet.
- Inbound P2P connections are enabled by default (`network.enable_inbound`); disable them in `config.json` if you don't want to accept connections from the network.
- **Do not use this software to secure funds you cannot afford to lose.** If you experiment with real transactions, use small amounts — better yet, adapt the config for testnet/signet first.

---

## License

MIT — see `LICENSE.txt`.
