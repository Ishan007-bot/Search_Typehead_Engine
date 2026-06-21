"""
Performance benchmark for the Search Typeahead System.

Measures the three things the assignment's non-functional section asks for:
  1. Suggestion latency  (p50 / p95 / p99), cold (cache miss) vs warm (cache hit)
  2. Cache hit rate       under a realistic prefix workload
  3. Write reduction      from batching (searches enqueued vs DB transactions)

Usage:
    # 1) start the server in another terminal:
    python -m uvicorn app.main:app --port 8800
    # 2) run the benchmark against it:
    python bench/benchmark.py --base http://127.0.0.1:8800

It only uses the stdlib (urllib), so there's nothing extra to install.
"""

import argparse
import json
import random
import statistics
import time
import urllib.request as u
from urllib.parse import quote

# A realistic set of prefixes a user might type. These match the ORCAS dataset
# (real Bing search queries), so the benchmark exercises real cache keys.
PREFIXES = [
    "w", "we", "wea", "weath", "weather",
    "you", "yout", "youtube",
    "face", "faceb", "facebook",
    "goog", "googl", "google",
    "amaz", "amazon",
    "map", "maps", "mapq",
    "how", "how to",
    "wal", "walm", "walmart",
    "net", "netf", "netflix",
    "ebay", "eba",
    "gma", "gmail",
    "craig", "craigslist",
]


def _get(base, path):
    with u.urlopen(f"{base}{path}") as r:
        return json.load(r)


def _post(base, path, body):
    data = json.dumps(body).encode()
    req = u.Request(f"{base}{path}", data=data,
                    headers={"Content-Type": "application/json"}, method="POST")
    with u.urlopen(req) as r:
        return json.load(r)


def _pct(values, p):
    """Percentile via nearest-rank on the sorted sample."""
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round((p / 100) * len(s) + 0.5)) - 1))
    return s[k]


def time_suggest(base, prefix, mode="basic"):
    t0 = time.perf_counter()
    resp = _get(base, f"/suggest?q={quote(prefix)}&mode={mode}")
    dt = (time.perf_counter() - t0) * 1000.0   # ms
    return dt, resp.get("source")


def bench_latency(base, iterations=2000):
    print(f"\n=== LATENCY ({iterations} /suggest requests, mixed prefixes) ===")
    cold, warm, all_lat = [], [], []
    hits = misses = 0
    for i in range(iterations):
        prefix = random.choice(PREFIXES)
        dt, source = time_suggest(base, prefix)
        all_lat.append(dt)
        if source == "cache":
            warm.append(dt); hits += 1
        else:
            cold.append(dt); misses += 1

    def report(name, xs):
        if not xs:
            print(f"  {name:18} (no samples)")
            return
        print(f"  {name:18} n={len(xs):5}  "
              f"p50={_pct(xs,50):6.3f}ms  p95={_pct(xs,95):6.3f}ms  "
              f"p99={_pct(xs,99):6.3f}ms  max={max(xs):6.3f}ms")

    report("overall", all_lat)
    report("warm (cache hit)", warm)
    report("cold (cache miss)", cold)
    total = hits + misses
    print(f"  cache hit rate (this run): {hits}/{total} = {hits/total*100:.1f}%")
    return {
        "overall_p50": _pct(all_lat, 50), "overall_p95": _pct(all_lat, 95),
        "overall_p99": _pct(all_lat, 99),
        "warm_p95": _pct(warm, 95) if warm else None,
        "cold_p95": _pct(cold, 95) if cold else None,
        "hit_rate": round(hits / total, 4) if total else 0.0,
    }


def bench_cache_hitrate(base):
    print("\n=== CACHE HIT RATE (server-side counters) ===")
    s = _get(base, "/cache/stats")
    print(f"  overall_hit_rate: {s['overall_hit_rate']*100:.1f}%  "
          f"(hits={s['total_hits']} misses={s['total_misses']})")
    for n in s["nodes"]:
        print(f"    {n['node']:14} size={n['size']:4} hits={n['hits']:5} "
              f"misses={n['misses']:5} hit_rate={n['hit_rate']*100:5.1f}%")
    return s


def bench_write_reduction(base, n_searches=3000):
    print(f"\n=== WRITE REDUCTION ({n_searches} POST /search) ===")
    words = ["weather", "youtube", "facebook", "google", "amazon", "maps",
             "walmart", "netflix", "ebay", "gmail", "craigslist", "google maps"]
    before = _get(base, "/batch/stats")
    t0 = time.perf_counter()
    for _ in range(n_searches):
        _post(base, "/search", {"query": random.choice(words)})
    elapsed = time.perf_counter() - t0
    _post(base, "/batch/flush", {})          # flush remaining buffer
    after = _get(base, "/batch/stats")

    enq = after["searches_enqueued"] - before["searches_enqueued"]
    flushes = after["db_flushes"] - before["db_flushes"]
    print(f"  searches submitted:       {enq}")
    print(f"  DB transactions (flushes):{flushes}")
    if flushes:
        print(f"  searches per transaction: {enq/flushes:.1f}")
        print(f"  write reduction:          {(1 - flushes/enq)*100:.2f}% fewer writes")
    print(f"  throughput:               {n_searches/elapsed:.0f} searches/sec")
    print(f"  size_flushes={after['size_flushes']} timer_flushes={after['timer_flushes']}")
    return {"enqueued": enq, "flushes": flushes,
            "reduction": round(1 - flushes / enq, 4) if enq else 0.0}


def show_consistent_hashing(base):
    print("\n=== CONSISTENT HASHING (prefix -> owning node) ===")
    dbg = _get(base, "/cache/debug?prefix=iph")
    print(f"  sample distribution across nodes: {dbg['sample_distribution']}")
    for p in ["weath", "you", "face", "goog", "amaz", "maps", "netf"]:
        d = _get(base, f"/cache/debug?prefix={quote(p)}")
        print(f"    {p:8} -> {d['owner_node']:14} (ring_pos={d['ring_position']})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8800")
    ap.add_argument("--iters", type=int, default=2000)
    ap.add_argument("--searches", type=int, default=3000)
    args = ap.parse_args()

    print(f"Benchmarking {args.base}")
    try:
        _get(args.base, "/health")
    except Exception as e:
        print(f"ERROR: server not reachable at {args.base} ({e})")
        print("Start it first:  python -m uvicorn app.main:app --port 8800")
        return

    # Warm the cache first so we measure both hit and miss paths.
    for p in PREFIXES:
        time_suggest(args.base, p)

    lat = bench_latency(args.base, args.iters)
    hr = bench_cache_hitrate(args.base)
    wr = bench_write_reduction(args.base, args.searches)
    show_consistent_hashing(args.base)

    print("\n=== SUMMARY ===")
    print(f"  suggest p95 (overall): {lat['overall_p95']:.3f} ms")
    print(f"  suggest p95 (warm):    {lat['warm_p95']}")
    print(f"  cache hit rate:        {hr['overall_hit_rate']*100:.1f}%")
    print(f"  write reduction:       {wr['reduction']*100:.2f}%")


if __name__ == "__main__":
    main()
