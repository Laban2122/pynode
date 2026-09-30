#!/usr/bin/env python3
"""
DNS seed quality probe (standalone — does not import or touch node code).

For each Bitcoin DNS seed it measures:
  - how many addresses it returns (A + AAAA, deduped),
  - how long resolution took, and any resolution error,
  - what fraction of those addresses actually accept a TCP connection on :8333.

Addresses that resolve but never connect are worthless for peer acquisition, so
"reachable %" is the real quality signal. Reachability is a property of an address;
a seed's reachable count = how many of *its* returned addresses connected.

Usage:
    python tools/dns_seed_probe.py
    python tools/dns_seed_probe.py --port 8333 --timeout 5 --concurrency 50
    python tools/dns_seed_probe.py --nameserver 1.1.1.1     # force an upstream resolver
    python tools/dns_seed_probe.py --max-per-seed 20         # cap TCP checks per seed
"""
import argparse
import asyncio
import socket
import time

import dns.resolver

# Seeds. "current" = already used by network_manager.py; "candidate" = other
# mainnet seeds from Bitcoin Core chainparams, included so we can compare.
SEEDS = [
    ("seed.bitcoin.sipa.be",            "current"),
    ("dnsseed.bluematt.me",             "current"),
    ("dnsseed.bitcoin.dashjr.org",      "current"),
    ("seed.bitcoin.sprovoost.nl",       "current"),
    ("seed.bitcoin.wiz.biz",            "current"),
    ("seed.btc.petertodd.net",          "candidate"),
    ("seed.bitcoinstats.com",           "candidate"),
    ("dnsseed.emzy.de",                 "candidate"),
    ("seed.mainnet.achownodes.xyz",     "candidate"),
    ("seed.bitcoin.jonasschnelli.ch",   "candidate"),
]


def resolve_seed(seed: str, dns_timeout: float, nameserver: str | None):
    """Return (addrs:set[str], n_a:int, n_aaaa:int, latency_s:float, error:str|None)."""
    r = dns.resolver.Resolver(configure=True)
    if nameserver:
        r.nameservers = [nameserver]
    r.lifetime = dns_timeout
    r.timeout = dns_timeout

    addrs: set[str] = set()
    n_a = n_aaaa = 0
    errors = []
    t0 = time.time()
    for rtype, counter in (("A", "a"), ("AAAA", "aaaa")):
        try:
            for rec in r.resolve(seed, rtype):
                addrs.add(str(rec))
                if rtype == "A":
                    n_a += 1
                else:
                    n_aaaa += 1
        except dns.resolver.NoAnswer:
            pass  # seed simply has no record of this type — not an error
        except Exception as e:
            errors.append(f"{rtype}:{type(e).__name__}")
    latency = time.time() - t0
    return addrs, n_a, n_aaaa, latency, (", ".join(errors) if errors else None)


async def tcp_check(ip: str, port: int, timeout: float, sem: asyncio.Semaphore) -> bool:
    """True if a TCP connection to ip:port completes within timeout."""
    async with sem:
        try:
            fut = asyncio.open_connection(ip, port)
            reader, writer = await asyncio.wait_for(fut, timeout=timeout)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            return True
        except Exception:
            return False


async def main():
    ap = argparse.ArgumentParser(description="Probe Bitcoin DNS seed quality.")
    ap.add_argument("--port", type=int, default=8333)
    ap.add_argument("--timeout", type=float, default=5.0, help="TCP connect timeout (s)")
    ap.add_argument("--dns-timeout", type=float, default=5.0, help="DNS resolution timeout (s)")
    ap.add_argument("--concurrency", type=int, default=50, help="max parallel TCP checks")
    ap.add_argument("--max-per-seed", type=int, default=0, help="cap addresses tested per seed (0 = all)")
    ap.add_argument("--nameserver", default=None, help="force an upstream resolver, e.g. 1.1.1.1")
    args = ap.parse_args()

    print(f"DNS seed probe - port {args.port}, tcp timeout {args.timeout}s, "
          f"resolver {args.nameserver or 'system default'}\n")

    # 1. Resolve every seed (sync; fast).
    seed_addrs: dict[str, set[str]] = {}
    resolve_rows = []
    for seed, kind in SEEDS:
        addrs, n_a, n_aaaa, latency, error = resolve_seed(seed, args.dns_timeout, args.nameserver)
        if args.max_per_seed and len(addrs) > args.max_per_seed:
            addrs = set(list(addrs)[:args.max_per_seed])
        seed_addrs[seed] = addrs
        resolve_rows.append((seed, kind, n_a, n_aaaa, len(addrs), latency, error))

    # 2. TCP-check every unique address once, concurrently.
    all_addrs = sorted({a for s in seed_addrs.values() for a in s})
    print(f"Resolving done. Testing TCP reachability of {len(all_addrs)} unique addresses "
          f"to :{args.port} ...\n")
    sem = asyncio.Semaphore(args.concurrency)
    t0 = time.time()
    results = await asyncio.gather(*(tcp_check(a, args.port, args.timeout, sem) for a in all_addrs))
    reachable = {a for a, ok in zip(all_addrs, results) if ok}
    tcp_elapsed = time.time() - t0

    # 3. Per-seed report.
    hdr = f"{'seed':<32}{'kind':<10}{'A':>4}{'AAAA':>6}{'uniq':>6}{'reach':>7}{'reach%':>8}{'dns_s':>7}  error"
    print(hdr)
    print("-" * len(hdr))
    for seed, kind, n_a, n_aaaa, uniq, latency, error in resolve_rows:
        addrs = seed_addrs[seed]
        r = sum(1 for a in addrs if a in reachable)
        pct = (100.0 * r / uniq) if uniq else 0.0
        print(f"{seed:<32}{kind:<10}{n_a:>4}{n_aaaa:>6}{uniq:>6}{r:>7}{pct:>7.0f}%{latency:>7.2f}  {error or ''}")

    # 4. Summary.
    total_uniq = len(all_addrs)
    total_reach = len(reachable)
    ipv4 = [a for a in all_addrs if ":" not in a]
    ipv6 = [a for a in all_addrs if ":" in a]
    r4 = sum(1 for a in ipv4 if a in reachable)
    r6 = sum(1 for a in ipv6 if a in reachable)
    print("-" * len(hdr))
    print(f"\nTotals: {total_uniq} unique addrs, {total_reach} reachable "
          f"({(100.0*total_reach/total_uniq if total_uniq else 0):.0f}%) "
          f"in {tcp_elapsed:.1f}s")
    print(f"  IPv4: {len(ipv4)} addrs, {r4} reachable "
          f"({(100.0*r4/len(ipv4) if ipv4 else 0):.0f}%)")
    print(f"  IPv6: {len(ipv6)} addrs, {r6} reachable "
          f"({(100.0*r6/len(ipv6) if ipv6 else 0):.0f}%)  "
          f"{'(IPv6 may be unavailable on this host)' if ipv6 and r6 == 0 else ''}")
    if total_uniq == 0:
        print("\nWARNING: Zero addresses resolved from ALL seeds - DNS resolution itself is failing "
              "on this host. Try --nameserver 1.1.1.1 to test a different upstream resolver.")


if __name__ == "__main__":
    asyncio.run(main())
