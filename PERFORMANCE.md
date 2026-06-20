# Performance Report

All numbers below were produced by [`bench/benchmark.py`](bench/benchmark.py)
against a locally running server, plus a standalone consistent-hashing experiment.
They are reproducible — commands are given so the results can be regenerated and
defended.

> Environment: single machine, Windows, Python 3.12, SQLite, FastAPI/uvicorn,
> in-process cache. Dataset: the sample `data/queries.csv` (~119 queries). Absolute
> latencies depend on the machine; the *relative* results (hit rate, write
> reduction, remap fraction) are the meaningful ones.

Reproduce:
```bash
python -m uvicorn app.main:app --port 8800        # terminal 1
python bench/benchmark.py --base http://127.0.0.1:8800   # terminal 2
```

---

## 1. Suggestion latency

`/suggest` over 2000 mixed-prefix requests:

| Metric | p50 | p95 | p99 | max |
|---|---|---|---|---|
| Overall | 2.97 ms | **4.08 ms** | 4.75 ms | 7.15 ms |

Cold (cache miss, full pipeline) vs warm (cache hit), 300 requests each:

| Path | p50 | p95 |
|---|---|---|
| Warm (cache hit) | 1.29 ms | 3.04 ms |
| Cold (cache miss → trie → populate) | 1.38 ms | 2.80 ms |

**p95 suggestion latency ≈ 4 ms** end-to-end (including HTTP round-trip).

**Honest reading of cold vs warm:** on this dataset the gap is small, because
(a) the dataset is tiny (119 rows), so even the trie path is sub-millisecond, and
(b) the localhost HTTP round-trip dominates the in-process work. **The cache's
real value here is reducing CPU/ranking work and DB load at scale, not slashing
latency on a tiny in-memory dataset.** With a large dataset and the recency
re-rank (which scans a 50-candidate pool and computes decay per candidate), the
warm path's advantage widens, because a hit skips that work entirely.

---

## 2. Cache hit rate

Server-side counters after the benchmark workload (`GET /cache/stats`):

| Scope | Hit rate |
|---|---|
| **Overall** | **98.0%** (2001 hits / 40 misses) |
| cache-node-0 | 97.5% (size 10) |
| cache-node-1 | 98.1% (size 8) |
| cache-node-2 | 98.4% (size 13) |

Misses are first-touch-per-prefix (cold) plus post-write invalidations; everything
after is a hit until TTL/invalidation. The hit rate is consistent across nodes,
which confirms the ring spreads keys evenly (see §4).

---

## 3. Write reduction through batching

3000 `POST /search` requests across 12 distinct queries (`GET /batch/stats`):

| Metric | Value |
|---|---|
| Searches submitted | 3000 |
| **DB transactions (flushes)** | **5** |
| Searches per transaction | 600 |
| **Write reduction** | **99.83% fewer writes** |
| Throughput | ~311 searches/sec |

Without batching, 3000 searches = 3000 DB transactions. With batching,
**3000 searches = 5 transactions** — a ~600× reduction. Two mechanisms combine:
duplicate aggregation (same query collapses to one row) and time/size-triggered
flushing.

---

## 4. Consistent hashing behavior

Standalone experiment, 3000 keys over the ring (reproduce: see snippet in
`ARCHITECTURE.md` §4):

**Distribution (3 nodes, 150 vnodes each):**

| node-0 | node-1 | node-2 | ideal |
|---|---|---|---|
| 1028 | 980 | 992 | 1000 |

Spread 980–1028 — even, thanks to virtual nodes.

**Remap on membership change (the whole point of consistent hashing):**

| Event | Keys remapped |
|---|---|
| Remove 1 of 3 nodes | **32.7%** (≈ 1/N) |
| Naive `hash % N` would remap | ~67% |
| Keys *not* on the removed node | **0 moved** (all kept their owner) |

This is the headline result: consistent hashing moves only ~`1/N` of keys when a
node leaves, versus ~`(N-1)/N` for `hash % N`. That difference is what prevents a
cache stampede onto the primary store during scaling events.

---

## 5. Summary

| Requirement | Result |
|---|---|
| Suggest p95 latency | **~4 ms** |
| Cache hit rate | **98%** |
| Write reduction (batching) | **99.83%** (3000 → 5 writes) |
| Key remap on node removal | **32.7%** vs ~67% naive |
| Key distribution across nodes | 980–1028 / 1000 (even) |

All three non-functional targets (low-latency suggestions, high cache hit rate,
reduced DB writes) are met and measured, and the consistent-hashing behavior is
demonstrated with concrete numbers.
