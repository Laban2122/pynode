"""Crash-recovery tests for the blocks.dat / blocks.idx store.

Both files are append-only logs with no header, so an interrupted write
leaves a torn tail. The failure that matters is not a crash on read - it is
that the store comes back up *looking* intact and hands out the wrong
blocks, which is what happened at heights 939008-939078
(docs/pynode-blockstore-corruption-report.md).

Run:  python -m pytest test_blockstore_recovery.py -q
  or: python test_blockstore_recovery.py
"""

import hashlib
import os
import struct
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from blockchain.blockchain import BlockChain

ENTRY_SIZE = 48


def dsha(b):
    return hashlib.sha256(hashlib.sha256(b).digest()).digest()


def fake_block(tag, size=200):
    """80-byte header with a recognisable nonce, then filler."""
    header = bytearray(80)
    header[76:80] = struct.pack("<I", tag)
    return bytes(header) + bytes([tag & 0xFF]) * (size - 80)


class Store:
    """A BlockChain in a scratch directory, reopenable in place."""

    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="blockstore-test-")
        self.dat = os.path.join(self.dir, "blocks.dat")
        self.idx = os.path.join(self.dir, "blocks.idx")

    def open(self):
        return BlockChain(blocks_file=self.dat, index_file=self.idx)

    def fill(self, n, start_height=0):
        bc = self.open()
        blocks = []
        for i in range(n):
            raw = fake_block(i + 1)
            assert bc.add_block(raw, height=start_height + i)
            blocks.append(raw)
        return bc, blocks


def check_all_readable(bc, blocks, start_height=0):
    for i, raw in enumerate(blocks):
        got = bc.get_block(dsha(raw[:80]))
        assert got == raw, f"block {start_height + i} came back wrong"


# --------------------------------------------------------------------------


def test_round_trip():
    s = Store()
    bc, blocks = s.fill(5)
    check_all_readable(bc, blocks)
    assert bc.height == 4

    # and again after a clean reopen
    check_all_readable(s.open(), blocks)


def test_partial_index_entry_is_truncated():
    """A half-written index entry must not shift every later entry."""
    s = Store()
    _, blocks = s.fill(4)

    with open(s.idx, "ab") as f:          # 20 bytes of a 48-byte entry
        f.write(b"\xde\xad\xbe\xef" * 5)

    bc = s.open()                          # loader repairs it
    assert os.path.getsize(s.idx) % ENTRY_SIZE == 0
    assert len(bc.index_by_hash) == 4
    check_all_readable(bc, blocks)

    # The real test: the next block must still land at a correct offset.
    extra = fake_block(99)
    assert bc.add_block(extra, height=4)
    check_all_readable(s.open(), blocks + [extra])


def test_unindexed_block_bytes_are_truncated():
    """Block bytes with no index entry - the 939008 case.

    Without the truncate, add_block takes its offset from the file size,
    so the next block is written past the orphaned bytes while its index
    entry says otherwise, and every later read is misaligned.
    """
    s = Store()
    _, blocks = s.fill(3)
    good_size = os.path.getsize(s.dat)

    with open(s.dat, "ab") as f:           # an interrupted append
        f.write(b"\x7f" * 137)
    assert os.path.getsize(s.dat) == good_size + 137

    bc = s.open()
    assert os.path.getsize(s.dat) == good_size, "orphaned tail not removed"
    check_all_readable(bc, blocks)

    extra = fake_block(42)
    assert bc.add_block(extra, height=3)
    check_all_readable(s.open(), blocks + [extra])


def test_both_files_torn_at_once():
    """A power cut can tear both logs in the same instant."""
    s = Store()
    _, blocks = s.fill(6)
    good_dat = os.path.getsize(s.dat)

    with open(s.dat, "ab") as f:
        f.write(b"\x01" * 90)
    with open(s.idx, "ab") as f:
        f.write(b"\x02" * 31)

    bc = s.open()
    assert os.path.getsize(s.dat) == good_dat
    assert os.path.getsize(s.idx) == 6 * ENTRY_SIZE
    check_all_readable(bc, blocks)


def test_short_data_file_is_refused():
    """The index points past the end of the data: reads would be wrong.

    Nothing here can repair that, so it must raise rather than serve
    silently corrupt blocks.
    """
    s = Store()
    s.fill(4)
    with open(s.dat, "r+b") as f:
        f.truncate(os.path.getsize(s.dat) - 50)

    try:
        s.open()
    except RuntimeError as e:
        assert "SHORTER" in str(e), e
    else:
        raise AssertionError("a short data file was accepted")


def test_unreadable_index_does_not_wipe_the_store():
    """The old handler reset the index to empty on any read error, and the
    next add_block then wrote at offset 0 over every stored block."""
    s = Store()
    _, blocks = s.fill(3)
    data_before = open(s.dat, "rb").read()

    # Hold the index open exclusively; on Windows that makes the loader's
    # read fail, which is the condition the old code swallowed.
    handle = os.open(s.idx, os.O_RDWR | getattr(os, "O_EXLOCK", 0))
    try:
        if not hasattr(os, "O_EXLOCK"):
            import msvcrt

            msvcrt.locking(handle, msvcrt.LK_NBLCK, 1)
        raised = False
        try:
            s.open()
        except RuntimeError:
            raised = True
        except OSError:
            raised = True  # also acceptable: it did not continue regardless
        if not raised:
            # Some platforms allow the read anyway; then nothing was lost
            # and there is nothing to assert beyond the data being intact.
            pass
    finally:
        os.close(handle)

    assert open(s.dat, "rb").read() == data_before
    check_all_readable(s.open(), blocks)


def test_duplicate_add_is_rejected():
    s = Store()
    bc, blocks = s.fill(2)
    assert bc.add_block(blocks[0], height=0) is False
    assert os.path.getsize(s.dat) == sum(len(b) for b in blocks)


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception as exc:
                failures += 1
                print(f"FAIL {name}: {type(exc).__name__}: {exc}")
    print(f"\n{failures} failure(s)")
    sys.exit(1 if failures else 0)
