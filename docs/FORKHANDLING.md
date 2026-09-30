# Fork And Orphan Handling In Pynode

## What This Is

This document explains how Pynode handles:
- Forks: when two valid header/block branches compete
- Orphans: when data arrives before its parent is known

It is written for operators and contributors who want to understand behavior during reorgs and out-of-order network data.

---

## Purpose Of Fork Handling

Bitcoin nodes must follow the branch with the most cumulative proof-of-work.
Pynode's fork pipeline exists to:
- Detect competing branches early
- Switch to a stronger branch when needed
- Keep header storage consistent on disk
- Recover missing blocks after a branch switch

Main components involved:
- [blockchain/fork_handler.py](blockchain/fork_handler.py)
- [blockchain/blockchain_manager.py](blockchain/blockchain_manager.py)
- [blockchain/headerchain.py](blockchain/headerchain.py)
- [network/network_manager.py](network/network_manager.py) (indirectly, via backfill and routing)

---

## When Fork Logic Is Triggered

Fork logic is triggered in these cases:

1. Incoming headers from peers
- In [blockchain/blockchain_manager.py](blockchain/blockchain_manager.py), `_process_headers` feeds each header to `ForkHandler.on_header(...)`.
- If a stronger chain is detected, a reorg decision is produced immediately.

2. Incoming blocks that do not extend the current tip
- In [blockchain/blockchain_manager.py](blockchain/blockchain_manager.py), `_process_block` checks whether `prev_hash == current_tip`.
- If not, and parent exists in the known chain, the block is treated as a fork branch candidate.

3. Incoming blocks with unknown parent
- Also in `_process_block`, if parent is not known at all, the block is treated as an orphan.
- This triggers a header request so the missing chain context can be discovered.

---

## How Forks Are Dealt With

### Step 1: Competing Header Is Seen

`ForkHandler` stores header and parent relation.
If the parent is connected, it computes:
- New header height
- New cumulative work

### Step 2: Chain Comparison

`ForkHandler` compares challenger branch vs current best:
- Uses cumulative work as primary criterion
- Uses height as tie-breaker

If challenger is stronger, it creates a `ReorgDecision` with:
- Fork point height
- Old tip hash
- New tip hash
- `new_tail` headers to append after truncation

### Step 3: Reorg Execution

`BlockchainManager._execute_reorg(...)` calls `HeaderChain.reorg(new_tail)`.
`HeaderChain.reorg(...)`:
- Finds fork point
- Truncates in-memory headers to fork point
- Truncates `headers.dat` to the same point
- Appends and validates the new branch tail

### Step 4: Block Recovery After Header Reorg

Header reorg updates header state first.
Full blocks for the new branch may still be missing.
Backfiller and normal block flow recover these gaps over time.

---

## How Orphans Are Dealt With

Pynode handles two orphan forms:

1. Header orphan
- A header whose parent is unknown to `ForkHandler`.
- Stored in `orphans_by_parent` until parent arrives.
- Automatically resolved when parent later appears.

2. Block orphan
- A block payload whose parent/header context is not yet known in `HeaderChain`.
- Stored in `BlockchainManager.orphan_blocks` (bounded cache).
- Headers are requested from peers to fill the missing chain context.

When new headers are later accepted:
- `_process_orphan_blocks_cache` checks cached orphan block hashes.
- If header now exists, block is stored and removed from cache.

---

## Practical Behavior You Should Expect

Normal fork race (no reorg):
- Competing branch is tracked
- Main chain stays unchanged
- Fork blocks may be cached pending outcome

Winning fork (reorg):
- Header chain switches to stronger branch
- Old header tail is discarded from tip back to fork point
- New header tail is appended
- Missing new-branch blocks are downloaded afterward

True orphan storm:
- Orphan cache grows until parents are discovered
- Node keeps requesting headers to resolve missing context
- If orphan cache limit is reached, newest excess orphan blocks are dropped

---

## Important Limits And Design Choices

Recent-depth tracking:
- `ForkHandler` tracks a limited recent window (`FORK_HANDLER_MAX_DEPTH`).
- This bounds memory but reduces visibility into very deep historical branch competition.

Header-first correctness:
- Branch selection is driven by header work, not by block arrival order.
- This is intentional and aligns with headers-first sync behavior.

Deferred block convergence:
- Reorg is applied at header layer first.
- Full block store catches up via normal block receipt and backfilling.

---

## Operator Notes

If you suspect fork issues, check logs for:
- Fork detected / reorg executed messages from BlockchainManager and ForkHandler
- Orphan block cache growth
- Repeated header requests with little progress

Useful files to inspect:
- [headers.dat](headers.dat)
- [blocks.dat](blocks.dat)
- [blocks.idx](blocks.idx)
- [logs/pynode.log](logs/pynode.log)

---

## Summary

Pynode's fork and orphan handling is built around a clear sequence:
1. Detect competing headers
2. Select best work branch
3. Reorg headers safely on disk
4. Resolve and backfill blocks until storage converges

This gives robust behavior for normal network forks and out-of-order data while keeping module responsibilities separated.