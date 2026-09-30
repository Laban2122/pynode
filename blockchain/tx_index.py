import asyncio
import glob
import hashlib
import heapq
import mmap
import os
import struct
import time

from core.helpers import parse_block_txs


def _block_hash(header_80: bytes) -> bytes:
    return hashlib.sha256(hashlib.sha256(header_80).digest()).digest()


class TxIndex:
    """
    Transaction index: txid → block height.

    Enables fast lookup of which block contains a given transaction,
    needed for validating transaction inputs without scanning all blocks.

    Structure:
      cache file — 17-byte header + flat sorted records, 36 bytes per entry:
                   txid_le (32 bytes, little-endian) + height (4 bytes, uint32 LE).
                   Memory-mapped read-only; binary search runs against the map,
                   so the index costs almost no resident RAM regardless of size.
      _live      — dict[bytes32 → int] for blocks received after the initial build.
                   Checked first on every lookup (O(1)).

    Lookup is O(1) for recent blocks, O(log N) for historical blocks (~29 probes
    for 400M entries; the hot pages stay in the OS page cache). We don't build
    for large lookup volumes, so mapped-file search is plenty.

    Build runs in a thread pool after backfill_done is set and uses a chunked
    external sort: entries are collected ~8M at a time, each chunk is sorted and
    spilled to a run file next to the cache, and the runs are stream-merged
    straight into the cache file. Peak RAM stays around 1 GB even for 400M+
    transactions. (A whole-range in-memory sort needs ~30 GB of object overhead
    and holds the GIL for hours — it froze the node and the desktop on
    2026-07-19.)

    Canonical safety: _build() and _fill_live_from_disk() use header_chain as the
    source of truth for which block hash is canonical at each height, so
    orphan/fork blocks stored in blocks.dat are never indexed.

    TODO: _build() reads header_chain.headers from a thread while the event loop
    may write to it during a reorg. A future fix should snapshot the headers list
    at build start.

    Cache: the merge writes the cache file directly (tmp + atomic replace), so a
    finished build is already persisted. Cache is invalidated when the stored
    block range changes.
    """

    _ENTRY         = 36          # bytes per record
    _CACHE_MAGIC   = b'TXIC'     # 4-byte file magic
    _CACHE_VER     = b'\x01'     # 1-byte format version
    _HEADER_LEN    = 17          # magic(4) + ver(1) + build_through(4) + data_len(8)
    _CHUNK_ENTRIES = 8_000_000   # entries per sorted run (~288 MB payload)

    def __init__(self, block_chain, header_chain, backfill_done: asyncio.Event,
                 cache_file: str = None):
        self.block_chain   = block_chain
        self.header_chain  = header_chain
        self.backfill_done = backfill_done
        self.cache_file    = cache_file

        self._mmap          = None   # read-only map of the cache file
        self._cache_fh      = None   # file handle backing the map
        self._count         = 0      # entries in the mapped cache
        self._live: dict[bytes, int] = {}
        self._build_through = -1
        self.ready          = False

    # ── Lifecycle ────────────────────────────────────────────────────────────

    async def run(self):
        print("[TxIndex] Waiting for backfill to complete...")
        await self.backfill_done.wait()

        loop = asyncio.get_running_loop()

        if self._try_load_cache():
            await loop.run_in_executor(None, self._fill_live_from_disk)
        else:
            print("[TxIndex] Building transaction index...")
            await loop.run_in_executor(None, self._build)

        self.ready = True
        print(f"[TxIndex] Ready: {self._count:,} transactions indexed, "
              f"{len(self._live):,} live entries")

    # ── Build (runs in thread) ────────────────────────────────────────────────

    def _build(self):
        if not self.cache_file:
            print("[TxIndex] No cache file configured — index disabled")
            return

        t0 = time.time()

        first = self.block_chain.get_first_block_height()
        last  = min(self.block_chain.get_height(),
                    self.header_chain.get_height())

        if first is None or last < 0:
            print("[TxIndex] No blocks stored — index is empty")
            return

        total = last - first + 1
        print(f"[TxIndex] Scanning {total:,} blocks ({first}-{last})...")

        # Clean up run files left behind by a crashed build
        for stale in glob.glob(glob.escape(self.cache_file) + ".run*"):
            os.remove(stale)

        runs: list[str] = []
        entries: list[bytes] = []
        n_total   = 0
        processed = 0

        def spill():
            nonlocal entries, n_total
            if not entries:
                return
            entries.sort()
            path = f"{self.cache_file}.run{len(runs)}"
            with open(path, 'wb') as rf:
                rf.writelines(entries)
            runs.append(path)
            n_total += len(entries)
            entries = []

        for height in range(first, last + 1):
            if height >= len(self.header_chain.headers):
                break
            canonical_hash = _block_hash(self.header_chain.headers[height])
            block_data = self.block_chain.get_block(canonical_hash)
            if not block_data:
                continue  # gap not yet filled — skip

            height_b = struct.pack('<I', height)
            for txid_hex in parse_block_txs(block_data):
                entries.append(bytes.fromhex(txid_hex)[::-1] + height_b)

            if len(entries) >= self._CHUNK_ENTRIES:
                spill()

            processed += 1
            if processed % 5000 == 0:
                elapsed = time.time() - t0
                print(f"[TxIndex]   {processed:,}/{total:,} blocks, "
                      f"{n_total + len(entries):,} txs ({elapsed:.0f}s)...")

        spill()

        print(f"[TxIndex] Merging {len(runs)} sorted run(s) ({n_total:,} entries)...")
        self._merge_runs(runs, n_total, last)
        for path in runs:
            os.remove(path)

        self._map_cache()

        elapsed = time.time() - t0
        print(f"[TxIndex] Build complete: {self._count:,} transactions in {elapsed:.1f}s")

    def _merge_runs(self, runs, n_total, build_through):
        """Stream-merge sorted run files into the cache file (tmp + atomic replace)."""
        es = self._ENTRY

        def reader(path):
            with open(path, 'rb') as f:
                while True:
                    buf = f.read(es * 8192)
                    if not buf:
                        return
                    for i in range(0, len(buf), es):
                        yield buf[i:i + es]

        t0     = time.time()
        merged = 0
        tmp    = self.cache_file + ".tmp"
        with open(tmp, 'wb', buffering=1 << 20) as out:
            out.write(self._CACHE_MAGIC)
            out.write(self._CACHE_VER)
            out.write(struct.pack('<I', build_through))
            out.write(struct.pack('<Q', n_total * es))
            for rec in heapq.merge(*(reader(p) for p in runs)):
                out.write(rec)
                merged += 1
                if merged % 50_000_000 == 0:
                    print(f"[TxIndex]   merged {merged:,}/{n_total:,} entries "
                          f"({time.time() - t0:.0f}s)...")
        os.replace(tmp, self.cache_file)
        size_mb = os.path.getsize(self.cache_file) / 1024 / 1024
        print(f"[TxIndex] Cache saved: {self.cache_file} ({size_mb:.0f} MB)")

    def _fill_live_from_disk(self):
        """Add canonical blocks above _build_through into _live (runs after cache load)."""
        last = min(self.block_chain.get_height(), self.header_chain.get_height())
        if last <= self._build_through:
            return
        count = last - self._build_through
        print(f"[TxIndex] Catching up {count} new block(s) into live index...")
        for height in range(self._build_through + 1, last + 1):
            if height >= len(self.header_chain.headers):
                break
            canonical_hash = _block_hash(self.header_chain.headers[height])
            block_data = self.block_chain.get_block(canonical_hash)
            if not block_data:
                continue
            for txid_hex in parse_block_txs(block_data):
                self._live[bytes.fromhex(txid_hex)[::-1]] = height

    # ── Cache ────────────────────────────────────────────────────────────────

    def _try_load_cache(self) -> bool:
        if not self.cache_file or not os.path.exists(self.cache_file):
            return False
        try:
            with open(self.cache_file, 'rb') as f:
                if f.read(4) != self._CACHE_MAGIC:
                    print("[TxIndex] Cache magic mismatch — rebuilding")
                    return False
                if f.read(1) != self._CACHE_VER:
                    print("[TxIndex] Cache version mismatch — rebuilding")
                    return False
                build_through = struct.unpack('<I', f.read(4))[0]
                data_len      = struct.unpack('<Q', f.read(8))[0]

            file_size = os.path.getsize(self.cache_file)
            if file_size != self._HEADER_LEN + data_len or data_len % self._ENTRY:
                print("[TxIndex] Cache truncated — rebuilding")
                return False

            first = self.block_chain.get_first_block_height()
            last  = self.block_chain.get_height()
            if first is None or build_through < first or build_through > last:
                print(f"[TxIndex] Cache out of range (cached={build_through}, "
                      f"stored={first}-{last}) — rebuilding")
                return False

            self._map_cache()
            print(f"[TxIndex] Mapped cache: {self._count:,} transactions "
                  f"(built through height {self._build_through})")
            return True

        except Exception as e:
            print(f"[TxIndex] Cache load error: {e} — rebuilding")
            return False

    def _map_cache(self):
        """Memory-map the cache file read-only and parse its header."""
        if self._mmap is not None:
            self._mmap.close()
            self._cache_fh.close()
            self._mmap = None

        self._cache_fh = open(self.cache_file, 'rb')
        self._mmap = mmap.mmap(self._cache_fh.fileno(), 0, access=mmap.ACCESS_READ)
        self._build_through = struct.unpack('<I', self._mmap[5:9])[0]
        data_len            = struct.unpack('<Q', self._mmap[9:17])[0]
        self._count         = data_len // self._ENTRY

    # ── Public API ───────────────────────────────────────────────────────────

    def add_block(self, height: int, block_data: bytes):
        """Register all transactions from a newly stored canonical block.

        Called by BlockchainManager after every successful block store.
        Blocks at or below _build_through are already in the cache; those
        above go into _live for O(1) lookup.
        """
        if height <= self._build_through:
            return
        for txid_hex in parse_block_txs(block_data):
            self._live[bytes.fromhex(txid_hex)[::-1]] = height

    def get_block_height(self, txid_le: bytes) -> int | None:
        """Return the block height containing txid_le (little-endian bytes)."""
        h = self._live.get(txid_le)
        if h is not None:
            return h
        return self._search(txid_le)

    def get_block_height_from_hex(self, txid_hex: str) -> int | None:
        """Convenience wrapper — accepts display-format (big-endian) hex txid."""
        return self.get_block_height(bytes.fromhex(txid_hex)[::-1])

    # ── Binary search ────────────────────────────────────────────────────────

    def _search(self, txid_le: bytes) -> int | None:
        if self._count == 0:
            return None

        es   = self._ENTRY
        base = self._HEADER_LEN
        mm   = self._mmap
        lo   = 0
        hi   = self._count - 1

        while lo <= hi:
            mid    = (lo + hi) >> 1
            offset = base + mid * es
            entry  = mm[offset:offset + 32]

            if entry == txid_le:
                return struct.unpack('<I', mm[offset + 32:offset + 36])[0]
            elif entry < txid_le:
                lo = mid + 1
            else:
                hi = mid - 1

        return None
