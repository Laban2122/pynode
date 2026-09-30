# Block Backfilling

## Purpose

Backfilling keeps local block storage complete when headers exist but some blocks are missing.

Typical cases:
- Restart after downtime
- Joining network late with existing headers
- Partial/corrupt block store recovery

This document covers backfilling behavior only. General node architecture is documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Scope And Ownership

Backfill responsibilities are split across existing layers:
- BlockBackfiller: detect missing blocks and request them
- NetworkManager: distribute requests across peers, track pending requests, retry timeouts
- BlockchainManager: validate/store received blocks

Communication remains queue-based; no cross-layer direct calls are introduced.

---

## High-Level Flow

1. Node starts and services are wired in [pynode.py](pynode.py).
2. BlockBackfiller waits for startup conditions.
3. Every poll cycle, BlockBackfiller scans for missing blocks.
4. Missing hashes are sent to NetworkManager as a backfill request.
5. NetworkManager sends `getdata` in batches to available peers.
6. Incoming blocks are forwarded to BlockchainManager for normal validation/storage.
7. Pending entries are cleared as blocks arrive; timeouts are retried.
8. Loop repeats on next poll cycle.

---

## Startup Conditions

BlockBackfiller waits for:
- Header chain minimum progress (`height >= 1000`)
- Minimum peer availability (`>= 5` peers)

If `DOWNLOAD_BLOCKS_FROM_HEIGHT = -1` and no blocks exist yet, backfill waits until a valid start point is available.

---

## Gap Detection Logic

Backfill scan range:
- Start: configured start height, or first stored block when auto mode is used
- End: `tip_height - 10`

Why `tip - 10`:
- Leaves a small safety window near tip
- Avoids repeatedly backfilling very recent chain-edge data

Each scan checks whether the block hash at each height exists in block storage. Missing hashes are accumulated and requested.

---

## Request Distribution

NetworkManager handles request strategy:
- Split into 16-block batches
- Round-robin across available peers
- Track pending requests per block hash
- Track per-peer pending load

When blocks arrive:
- Pending entry is removed
- Peer is marked responsive
- Progress counter increments

Timeout handling:
- Monitor loop runs every 10 seconds
- Pending entries older than 60 seconds are retried
- Retries prefer responsive peers with lower current load

No artificial global timeout is used; backfill continues while progress is possible.

---

## Key Runtime State

Important NetworkManager state used by backfill:
- `backfill_active`
- `backfill_pending`
- `backfill_responsive_peers`
- `backfill_peer_load`
- `backfill_blocks_received`

Important BlockBackfiller state:
- `poll_interval` (default 300s)
- Scan strategy: FULL SCAN every cycle (not incremental)

---

## Configuration

Backfill configuration lives in [core/config.py](core/config.py):

```python
DOWNLOAD_BLOCKS_FROM_HEIGHT = -1
```

Meaning:
- `-1`: auto mode (resume from existing local block range when possible)
- `0+`: explicit start height

Practical examples:
- `0`: attempt complete history backfill
- `900000`: only recent-chain backfill

---

## Integration In pynode.py

Backfiller runs as a concurrent task with BM/NM/RPC:

```python
await asyncio.gather(
    bm.run(),
    nm.run(),
    rpc_server.run(),
    backfiller.run(),
)
```

Backfiller is initialized with:
- BM output queue (request path)
- HeaderChain and BlockChain references (gap scan)
- `DOWNLOAD_BLOCKS_FROM_HEIGHT`
- Peer-count callback

---

## Operational Notes

- Backfilling is idempotent at the request layer: duplicates are tolerated and filtered by storage checks.
- BlockchainManager should treat already-stored blocks as duplicates, not fatal errors.
- Peer churn is expected; monitor logic adapts by redistributing pending work.
- New peers joining mid-session can be added to rotation.

---

## Troubleshooting

### Backfiller does not start

Check:
1. Header height has reached startup threshold.
2. At least 5 peers are connected.
3. Start height is resolvable (`DOWNLOAD_BLOCKS_FROM_HEIGHT` not blocked by empty store in auto mode).

Actions:
1. Set `DOWNLOAD_BLOCKS_FROM_HEIGHT = 0` (or a concrete height).
2. Wait for more peers.
3. Verify node sync progress/logs.

### Blocks not arriving

Check NM logs for:
- No available peers
- Timed out blocks with no responsive peers

Actions:
1. Increase connected peers.
2. Wait for retry cycle.
3. Confirm peer connectivity/health.

### Backfill ends but gaps remain

Common reasons:
1. Heights below configured start are intentionally skipped.
2. Last 10 blocks are excluded by design.
3. Additional gaps will be picked up next poll cycle.

### Excessive logging during large backfills

Current progress logging is periodic. If needed, reduce log frequency in code for long runs.

---

## Validation Checklist

Use this quick checklist after changes:

1. Full backfill run
- Remove `blocks.dat` and `blocks.idx` (keep headers)
- Start node with `python pynode.py`
- Confirm sustained `getdata` activity and growing block store

2. Partial backfill
- Set `DOWNLOAD_BLOCKS_FROM_HEIGHT = 900000`
- Start node
- Confirm early heights are skipped and recent range fills

3. Restart behavior
- Start node, allow backfill progress, stop, restart
- Confirm scan resumes from configured/derived range and continues

4. Peer churn
- Start with minimum peers, then add/remove peers
- Confirm pending work is redistributed and progress continues

---

## Known Limitations

- Full scan every cycle (not incremental) trades CPU for reliability
- Backfill window excludes latest 10 blocks by design
- Performance depends on peer quality and connectivity

Why full scan:
- Catches blocks stuck in pending state (requested but never delivered)
- Rediscovers blocks lost to peer disconnections
- Simple and reliable - no complex state tracking needed
- At ~25,000 heights, scan completes in <1 second

Potential future improvements:
- Adaptive progress logging thresholds
- Latency-aware peer selection
- Incremental scanning with timeout-aware retry logic

---

## Related Documents

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/TODO.md](docs/TODO.md)
- [core/config.py](core/config.py)
- [blockchain/block_backfiller.py](blockchain/block_backfiller.py)
- [network/network_manager.py](network/network_manager.py)
- [blockchain/blockchain_manager.py](blockchain/blockchain_manager.py)
