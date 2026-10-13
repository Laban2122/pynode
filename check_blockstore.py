"""Read-only consistency check for blocks.dat / blocks.idx.

Run this before starting Pynode on a store that has been through a crash.
It touches nothing - it only reports what BlockChain's loader would do:

  clean              - index and data file agree, nothing to repair
  truncate dat       - orphaned block bytes with no index entry; the loader
                       removes them (the 939008-939078 misalignment case)
  truncate idx       - a partial index entry; the loader removes it
  REFUSE             - the data file is shorter than the index claims, which
                       cannot be repaired here; the loader raises instead of
                       serving blocks from the wrong offsets

With --verify it also re-hashes every indexed block's header and reports any
entry whose bytes do not hash to the hash recorded in the index. That is the
check that catches misalignment which happens to leave the file lengths
plausible.

Usage:
    python check_blockstore.py [--dat blocks.dat] [--idx blocks.idx] [--verify]
"""

import argparse
import hashlib
import os
import struct
import sys

ENTRY_SIZE = 48


def dsha256(data):
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dat", default="blocks.dat")
    ap.add_argument("--idx", default="blocks.idx")
    ap.add_argument("--verify", action="store_true",
                    help="re-hash every indexed block header")
    args = ap.parse_args()

    if not os.path.exists(args.idx):
        print(f"{args.idx}: not present - nothing to check")
        return 0

    raw = open(args.idx, "rb").read()
    count, partial = divmod(len(raw), ENTRY_SIZE)

    entries = []
    indexed_end = 0
    heights = []
    for i in range(count):
        e = raw[i * ENTRY_SIZE:(i + 1) * ENTRY_SIZE]
        block_hash = e[0:32]
        offset = struct.unpack("<Q", e[32:40])[0]
        size = struct.unpack("<I", e[40:44])[0]
        height = struct.unpack("<i", e[44:48])[0]
        entries.append((block_hash, offset, size, height))
        heights.append(height)
        indexed_end = max(indexed_end, offset + size)

    actual = os.path.getsize(args.dat) if os.path.exists(args.dat) else 0

    print(f"index  : {count} entries"
          + (f", plus a {partial}-byte partial tail" if partial else ""))
    if heights:
        print(f"heights: {min(heights)}..{max(heights)} "
              f"({len(set(heights))} distinct)")
    print(f"data   : {actual} bytes on disk, {indexed_end} accounted for by the index")

    verdicts = []
    if partial:
        verdicts.append(f"truncate idx by {partial} bytes")
    if actual > indexed_end:
        verdicts.append(f"truncate dat by {actual - indexed_end} bytes")
    elif actual < indexed_end:
        verdicts.append(f"REFUSE: dat is {indexed_end - actual} bytes short")
    print("verdict:", "; ".join(verdicts) if verdicts else "clean")

    rc = 2 if actual < indexed_end else 0

    if args.verify and os.path.exists(args.dat):
        bad = []
        with open(args.dat, "rb") as f:
            for block_hash, offset, size, height in entries:
                f.seek(offset)
                header = f.read(80)
                if len(header) < 80 or dsha256(header) != block_hash:
                    bad.append(height)
        if bad:
            rc = 2
            shown = ", ".join(str(h) for h in bad[:20])
            print(f"MISALIGNED: {len(bad)} entries do not hash to their "
                  f"recorded hash: {shown}"
                  + (" ..." if len(bad) > 20 else ""))
        else:
            print(f"verified: all {len(entries)} entries hash to their "
                  f"recorded hash")

    return rc


if __name__ == "__main__":
    sys.exit(main())
