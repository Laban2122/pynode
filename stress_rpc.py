"""
stress_rpc.py — Reproduce the pynode RPC freeze bug (Windows ProactorEventLoop / CLOSE_WAIT)

Usage:
    python pynode.py offline          # in one terminal
    python stress_rpc.py              # in another

Six attack patterns run concurrently against the RPC server:
  clean          — valid + invalid requests, Connection: close
  keepalive_drop — keep-alive request, close before reading response  (primary CLOSE_WAIT trigger)
  partial_body   — lies about Content-Length, closes after partial body  (hangs request.json())
  rst            — sends complete request then RST instead of FIN
  empty          — TCP connect, send nothing, close immediately
  garbage        — non-HTTP bytes

Health check polls getblockcount every 3s. Bug detected when no response for 15s.
"""

import asyncio
import json
import random
import socket
import struct
import sys
import time
from datetime import datetime

HOST = "127.0.0.1"
PORT = 8888

# ── shared stats (written from coroutines and threads, reads are advisory) ────
_stats = {
    "sent": 0,
    "ok": 0,
    "fail": 0,
    "last_ok": time.time(),
}
_bug_triggered = False


# ── raw socket helpers (blocking — must run in executor) ──────────────────────

def _post_bytes(method: str, params: list, connection: str = "close", extra_len: int = 0) -> bytes:
    body = json.dumps({"method": method, "params": params}).encode()
    header = (
        f"POST / HTTP/1.1\r\n"
        f"Host: {HOST}:{PORT}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body) + extra_len}\r\n"
        f"Connection: {connection}\r\n"
        f"\r\n"
    ).encode()
    return header + body


def _attack_keepalive_drop():
    """
    Keep-alive request where we close before reading the response.
    Server side ends up in CLOSE_WAIT — the primary trigger pattern.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((HOST, PORT))
        s.sendall(_post_bytes("getblockcount", [], connection="keep-alive"))
        time.sleep(random.uniform(0.02, 0.2))
        s.close()   # close without reading response or sending more requests
        return True
    except Exception:
        return False


def _attack_partial_body():
    """
    Promise N+50 bytes in Content-Length, only send N bytes, then close.
    aiohttp's request.json() awaits the remaining bytes that never arrive.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        s.connect((HOST, PORT))
        body = json.dumps({"method": "getblockcount", "params": []}).encode()
        header = (
            f"POST / HTTP/1.1\r\n"
            f"Host: {HOST}:{PORT}\r\n"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(body) + 50}\r\n"   # lie: promise 50 extra bytes
            f"Connection: close\r\n"
            f"\r\n"
        ).encode()
        s.sendall(header + body)   # send exactly len(body) — 50 short
        time.sleep(random.uniform(0.05, 0.5))
        s.close()
        return True
    except Exception:
        return False


def _attack_rst():
    """
    Send complete request, then force RST on close instead of clean FIN.
    RST may not deliver EOF completion via IOCP on time.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_LINGER with l_onoff=1, l_linger=0 causes RST on close()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("HH", 1, 0))
        s.settimeout(2)
        s.connect((HOST, PORT))
        s.sendall(_post_bytes("getblockcount", []))
        s.close()   # sends RST
        return True
    except Exception:
        return False


def _attack_empty():
    """TCP connect, send nothing, close immediately."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((HOST, PORT))
        time.sleep(random.uniform(0.01, 0.05))
        s.close()
        return True
    except Exception:
        return False


def _attack_garbage():
    """Send bytes that look nothing like HTTP."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((HOST, PORT))
        s.sendall(b"\xff\xfeNOTHTTP" * 8 + b"\r\n\r\n")
        time.sleep(0.05)
        s.close()
        return True
    except Exception:
        return False


# ── async workers ─────────────────────────────────────────────────────────────

_METHODS = [
    ("getblockcount",    []),
    ("getblockchaininfo",[]),
    ("getnetworkinfo",   []),
    ("getblockhash",     [0]),
    ("getblockhash",     [500]),
    ("getblockhash",     [-1]),          # error: out of range
    ("getblockheader",   ["00" * 32]),   # error: not found
    ("getblockheader",   ["notahex"]),   # error: invalid
    ("nonexistent",      []),            # error: no such method
    ("scantxoutset",     ["start", []]), # error: empty descriptors
]


async def worker_clean():
    """Valid and invalid requests with Connection: close — the normal traffic baseline."""
    global _bug_triggered
    loop = asyncio.get_running_loop()
    while not _bug_triggered:
        method, params = random.choice(_METHODS)
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT), timeout=5
            )
            payload = _post_bytes(method, params)
            writer.write(payload)
            await writer.drain()
            data = await asyncio.wait_for(reader.read(4096), timeout=5)
            writer.close()
            await writer.wait_closed()
            if data:
                _stats["ok"] += 1
            else:
                _stats["fail"] += 1
        except Exception:
            _stats["fail"] += 1
        _stats["sent"] += 1
        await asyncio.sleep(random.uniform(0.005, 0.04))


async def worker_get():
    """GET / requests — should always return immediately (no body read)."""
    global _bug_triggered
    while not _bug_triggered:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT), timeout=5
            )
            writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1:8888\r\nConnection: close\r\n\r\n")
            await writer.drain()
            data = await asyncio.wait_for(reader.read(4096), timeout=5)
            writer.close()
            await writer.wait_closed()
            if data:
                _stats["ok"] += 1
            else:
                _stats["fail"] += 1
        except Exception:
            _stats["fail"] += 1
        _stats["sent"] += 1
        await asyncio.sleep(random.uniform(0.02, 0.1))


async def worker_raw(fn, delay: float):
    """Wrapper that runs a blocking socket attack function in the thread pool."""
    global _bug_triggered
    loop = asyncio.get_running_loop()
    while not _bug_triggered:
        ok = await loop.run_in_executor(None, fn)
        _stats["sent"] += 1
        if not ok:
            _stats["fail"] += 1
        await asyncio.sleep(delay)


# ── health check ──────────────────────────────────────────────────────────────

async def health_check_loop():
    global _bug_triggered
    consecutive_fail = 0
    while True:
        t0 = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(HOST, PORT), timeout=5
            )
            payload = _post_bytes("getblockcount", [])
            writer.write(payload)
            await writer.drain()
            data = await asyncio.wait_for(reader.read(4096), timeout=5)
            writer.close()
            await writer.wait_closed()

            elapsed_ms = (time.monotonic() - t0) * 1000
            if not data:
                raise ConnectionError("empty response")

            try:
                body = data.split(b"\r\n\r\n", 1)[1]
                height = json.loads(body).get("result", "?")
            except Exception:
                height = "?"

            _stats["last_ok"] = time.time()
            consecutive_fail = 0
            print(
                f"[HEALTH] OK   height={height:<7}  {elapsed_ms:5.0f}ms  "
                f"| sent={_stats['sent']} ok={_stats['ok']} fail={_stats['fail']}"
            )

        except Exception as e:
            consecutive_fail += 1
            since = time.time() - _stats["last_ok"]
            print(f"[HEALTH] FAIL #{consecutive_fail}  ({e})  — last OK {since:.0f}s ago")
            if since > 15:
                print()
                print("=" * 60)
                print(f"  *** BUG TRIGGERED — RPC frozen for {since:.0f}s ***")
                print(f"  Last OK:    {datetime.fromtimestamp(_stats['last_ok'])}")
                print(f"  Sent:       {_stats['sent']}")
                print(f"  OK / Fail:  {_stats['ok']} / {_stats['fail']}")
                print("=" * 60)
                _bug_triggered = True
                return

        await asyncio.sleep(3)


async def stats_loop():
    while not _bug_triggered:
        await asyncio.sleep(20)
        since = time.time() - _stats["last_ok"]
        print(
            f"[STATS]  sent={_stats['sent']}  ok={_stats['ok']}  "
            f"fail={_stats['fail']}  last_ok={since:.0f}s ago"
        )


# ── main ──────────────────────────────────────────────────────────────────────

async def main():
    print(f"Stress RPC — {HOST}:{PORT}")
    print("Patterns: clean / get / keepalive_drop / partial_body / rst / empty / garbage")
    print("Bug detection: getblockcount every 3s; freeze flagged after 15s of no response")
    print("Stop: Ctrl+C\n")

    # Verify server is up
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        s.connect((HOST, PORT))
        s.close()
    except Exception as e:
        print(f"[ERROR] Cannot reach {HOST}:{PORT} — {e}")
        print("Start node first:  python pynode.py offline")
        sys.exit(1)
    print("[OK] Server reachable — starting attack\n")

    # Attack concurrency knobs
    #   increase CLEAN_WORKERS / RAW_DELAY to be more aggressive
    CLEAN_WORKERS = 4
    RAW_DELAYS = {
        "keepalive_drop": 0.04,   # ~25 connections/s
        "partial_body":   0.07,   # ~14 connections/s
        "rst":            0.03,   # ~33 connections/s
        "empty":          0.02,   # ~50 connections/s
        "garbage":        0.03,   # ~33 connections/s
    }

    tasks = [
        asyncio.create_task(health_check_loop(), name="health"),
        asyncio.create_task(stats_loop(),         name="stats"),
        asyncio.create_task(worker_get(),          name="get"),
        *[asyncio.create_task(worker_clean(), name=f"clean-{i}") for i in range(CLEAN_WORKERS)],
        asyncio.create_task(worker_raw(_attack_keepalive_drop, RAW_DELAYS["keepalive_drop"]), name="keepalive_drop"),
        asyncio.create_task(worker_raw(_attack_partial_body,   RAW_DELAYS["partial_body"]),   name="partial_body"),
        asyncio.create_task(worker_raw(_attack_rst,            RAW_DELAYS["rst"]),            name="rst"),
        asyncio.create_task(worker_raw(_attack_empty,          RAW_DELAYS["empty"]),          name="empty"),
        asyncio.create_task(worker_raw(_attack_garbage,        RAW_DELAYS["garbage"]),        name="garbage"),
    ]

    health_task = tasks[0]

    try:
        await health_task   # runs until bug triggered or Ctrl+C
    except asyncio.CancelledError:
        pass
    finally:
        for t in tasks[1:]:
            t.cancel()
        await asyncio.gather(*tasks[1:], return_exceptions=True)

    if not _bug_triggered:
        print(
            f"\n[RESULT] Run ended without triggering bug  "
            f"({_stats['sent']} connections, {_stats['ok']} OK, {_stats['fail']} fail)"
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user")
        since = time.time() - _stats["last_ok"]
        print(
            f"       sent={_stats['sent']} ok={_stats['ok']} fail={_stats['fail']}  "
            f"last_ok={since:.0f}s ago"
        )
