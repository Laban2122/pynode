# Pynode TODO

This file tracks active and upcoming work only.
Completed tasks are intentionally not listed here.

## Most Critical TODOs

### 1. Create A UTXO Database
Priority: Critical
Status: Planned

Goal:
- Build and maintain a local UTXO set owned by Pynode.

Why this is critical:
- Required for deterministic local validation.
- Removes dependency on external RPC for core validation decisions.
- Enables reliable transaction policy and relay gating.

Minimum scope:
- Persistent key-value store for outpoints `(txid, vout)`.
- UTXO value, script, height, and spent status tracking.
- Atomic apply/revert support for reorg safety.
- Startup consistency checks and recovery path.

Suggested implementation notes:
- Store backend: sqlite, LevelDB, or RocksDB.
- Write path driven by accepted blocks in BlockchainManager.
- Reorg hooks integrated with header reorg/block convergence flow.

Acceptance criteria:
- Node can rebuild UTXO DB from stored blocks.
- UTXO DB remains consistent after restart.
- UTXO DB remains correct after chain reorg handling.
- Query latency is acceptable for per-tx validation.

---

### 2. Validate Incoming Transactions Before Relay
Priority: Critical
Status: Planned

Goal:
- Validate incoming transactions against local UTXO DB before relaying.
- Relay only transactions that pass validation and policy checks.

Why this is critical:
- Prevents propagation of invalid transactions.
- Reduces network noise and local mempool pollution.
- Establishes clear relay trust boundary in node behavior.

Required checks (minimum):
- Inputs exist in UTXO DB.
- Inputs are unspent.
- Basic transaction structure and amount sanity.
- Fee is non-negative and above configured minimum policy.
- No double-spend against current mempool view.

Relay policy rule:
- If validation fails, do not relay.
- If validation succeeds, accept to mempool and relay inventory.

Integration points:
- Receive tx in [blockchain/blockchain_manager.py](blockchain/blockchain_manager.py).
- Use UTXO DB service for input verification.
- Relay decision routed through [network/network_manager.py](network/network_manager.py).

Acceptance criteria:
- Invalid txs are rejected and never relayed.
- Valid txs are relayed normally.
- Conflicting txs are handled by explicit policy (reject/replace).
- Validation outcome is logged with concise reason codes.

---

## Supporting Work (After Critical)

### 3. Mempool Policy Completion
Priority: High
Status: Planned

Scope:
- Memory limits and eviction.
- RBF policy handling.
- Ancestor/descendant tracking.

### 4. Peer Misbehavior Scoring For Invalid TXs
Priority: High
Status: Planned

Scope:
- Score peers that repeatedly send invalid transactions.
- Apply disconnect/ban thresholds.

### 5. Backfill/UTXO Coordination
Priority: High
Status: Planned

Scope:
- Ensure UTXO DB updates stay correct during backfill.
- Ensure UTXO correctness when historical gaps are filled.

### 6. Address Relay Improvements
Priority: Medium | Status: Planned | Relay addr/addrv2 with deduplication, rate limits, and basic peer hygiene.

---

## Known Issues

### 7. Backfill Re-Requests Blocks Already Downloaded But Not Yet Applied
Priority: Medium
Status: Fix implemented 2026-07-18 (NM ignores rescans while session delivers;
delivered-block dedup window on session start). Verified against the d523635
deadlock fix and 66d25e5 peer-rotation fix — stall path with ban/rotation kept
intact. Pending live verification on next node restart.

Problem:
- The backfiller's periodic full scan checks `block_chain.has_block()` (persisted storage only).
  Blocks that are downloaded and queued but not yet applied still count as "missing",
  so each scan re-requests them from peers — duplicate downloads and wasted bandwidth.
- Observed: download counter reached 2,731 remaining, but the next scan re-queued
  11,875 "missing" and restarted the counter.

Fix direction:
- Have the scan (or NetworkManager) exclude in-flight/received-pending-apply hashes,
  e.g. check the apply queue alongside storage before re-requesting.

### 8. Misleading "Backfill complete: all blocks received" Log Message
Priority: Low
Status: Fix implemented 2026-07-18 (reworded to "session complete"; the
Backfiller's "No missing blocks found" heartbeat is the true completion signal).

Problem:
- `[NM] ✅ Backfill complete: all blocks received` is printed per request batch,
  while thousands of blocks are still missing overall. Reads as overall completion.

Fix direction:
- Reword to reflect batch scope (e.g. "batch complete: N blocks received"), and log
  a true completion line only when a full scan finds 0 missing.

---

### 9. Per-Peer Timeout for Stale Backfill Windows
Priority: Low
Status: Planned (observed 2026-07-18, cost only 96 blocks)

Problem:
- With windowed dispensing, a zero-delivery peer holds its 16-block
  in-flight window until the session ends: the session-level stall detector
  never fires while any other peer delivers, so those blocks wait out the
  whole session before being re-requested.

Fix direction:
- In the monitor loop, requeue blocks pending on one peer for >120s and
  ban that peer, without touching the rest of the session.

---

### 10. TxIndex Full Rebuild Exhausted System Memory (2026-07-19 incident)
Priority: High
Status: Fix implemented 2026-07-20 (chunked external sort + mmap lookup).
Pending live verification on next node restart (full rebuild expected,
~45 min, ~1 GB peak RAM).

Problem:
- The full-range rebuild (840000+, ~409M txs) collected every 36-byte entry
  as a separate Python object in one list: ~30 GB of RAM on a 31 GB machine,
  then a single GIL-holding sort that froze the event loop for hours
  (all peers reset) and swap-thrashed the whole desktop.
- The finished index was also held resident (~15 GB) and fully re-loaded
  from cache at each startup.

Fix:
- Build: sort ~8M-entry chunks, spill to run files, stream-merge into the
  cache file (tmp + atomic replace). Peak RAM ~1 GB, event loop breathes
  between chunks.
- Lookup: cache file is memory-mapped; binary search runs against the map.
  No resident copy, instant startup. Lookup volume is low by design.

---

## Architecture Rule Reminder

Hybrid tasks requiring both network context and blockchain intent belong in NetworkManager.

---

Last Updated: 2026-07-20