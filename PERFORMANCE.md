# Performance Report

All numbers below were produced by [`bench/benchmark.py`](bench/benchmark.py)
against a locally running server, plus a standalone consistent-hashing experiment.
They are reproducible — commands are given so the results can be regenerated and
defended.

> Environment: single machine, Windows, Python 3.12, SQLite, FastAPI/uvicorn.
> **Cache: three real Redis nodes via Docker Compose.** **Dataset: 200,000 queries
> from ORCAS** (real Bing query–click logs; see README → Dataset). Absolute
> latencies depend on the machine; the *relative* results (latency vs dataset size,
> hit rate, write reduction, remap fraction) are the meaningful ones.

Reproduce:
```bash
docker compose up --build                                # terminal 1
python bench/benchmark.py --base http://127.0.0.1:8000   # terminal 2
```

---

## 1. Suggestion latency (200k ORCAS queries, Redis-backed cache)

`/suggest` over 2000 mixed-prefix requests against the Docker deployment:

| Metric | p50 | p95 | p99 |
|---|---|---|---|
| Overall | 4.56 ms | **6.46 ms** | 11.2 ms |

**p95 suggestion latency ≈ 6.5 ms end-to-end** — this includes the HTTP round-trip
*and* a network hop to Redis on a cache hit (plus Docker's port-forwarding
overhead on this host).

### Redis vs in-process: an honest latency trade-off
An earlier in-process cache measured suggest p95 ≈ 3 ms. Moving the cache to three
real Redis nodes roughly **doubles p95 (~3 → ~6.5 ms)** because every hit now
crosses a TCP boundary to a separate process instead of reading a local dict. That
is the deliberate cost of a *genuinely* distributed cache: we trade a few
milliseconds for real, independent cache nodes that survive an app restart and can
be scaled/shared across multiple app workers. ~6.5 ms p95 is still well within
typeahead budgets.

### Latency is independent of dataset size — the key result
The trie lookup is `O(len(prefix))` and returns a **precomputed top-K** at the
prefix node; it does not scan the dataset. So the cost is dominated by the
network/HTTP round-trip, not the index size — **200,000 queries serve at the same
low latency as a few hundred would**, which is exactly what a typeahead index
should do.

---

## 2. Cache hit rate

Server-side counters after the benchmark workload (`GET /cache/stats`):

| Scope | Hit rate |
|---|---|
| **Overall** | **96.8%** (2010 hits / 67 misses) across the three Redis nodes |
| cache-node-0 | 96.3% |
| cache-node-1 | 97.1% |
| cache-node-2 | 97.2% |

Misses are first-touch-per-prefix (cold) plus post-write invalidations; everything
after is a hit until TTL/invalidation. The hit rate is uniform across the three
Redis nodes (each reports its own hits/misses via `/cache/stats`), confirming the
ring spreads keys evenly (§4).

---

## 3. Write reduction through batching

3000 `POST /search` requests across a set of distinct queries (`GET /batch/stats`):

| Metric | Value |
|---|---|
| Searches submitted | 3000 |
| **DB transactions (flushes)** | **13** |
| Searches per transaction | ~231 |
| **Write reduction** | **99.57% fewer writes** |

Without batching, 3000 searches = 3000 DB transactions. With batching,
**3000 searches = 13 transactions** — a ~230× reduction. Two mechanisms combine:
duplicate aggregation (same query → one row) and time/size-triggered flushing.
(The flush count scales with how long the run takes — a 2 s timer interval over
the run produced 13 flushes here; a denser burst aggregates into even fewer.)

---

## 4. Consistent hashing behavior

Standalone experiment, 3000 keys over the ring:

**Distribution (3 nodes, 150 vnodes each):**

| node-0 | node-1 | node-2 | ideal |
|---|---|---|---|
| 1045 | 958 | 997 | 1000 |

Spread 958–1045 — even, thanks to virtual nodes.

**Remap on membership change (the whole point of consistent hashing):**

| Event | Keys remapped |
|---|---|
| Remove 1 of 3 nodes | **31.9%** (≈ 1/N) |
| Naive `hash % N` would remap | ~67% |
| Keys *not* on the removed node | **0 moved** (all kept their owner) |

Consistent hashing moves only ~`1/N` of keys when a node leaves (here 31.9% ≈ 1/3),
versus ~`(N-1)/N` for `hash % N`. That difference is what prevents a cache stampede
onto the primary store during scaling events.

---

## 5. Startup cost (honest note)

Building the trie from the 200,000 ORCAS rows at startup takes **~8 seconds**
(one-time, on server/container boot). This is the deliberate trade for fast reads:
we precompute each node's top-K once so every subsequent `/suggest` is single-digit
ms. For much larger datasets this could be amortized (persist the trie, or build
lazily); for the assignment dataset a one-time ~8 s boot is acceptable.

---

## 6. Summary

| Requirement | Result |
|---|---|
| Dataset | **200,000 queries from ORCAS** (real Bing query–click logs; ≥ 100k required) |
| Cache backend | **3 real Redis nodes** (Docker), routed by our consistent-hash ring |
| Suggest p95 latency | **~6.5 ms** (Redis-backed; ~3 ms with an in-process cache) |
| Cache hit rate | **96.8%** |
| Write reduction (batching) | **99.57%** (≈231 searches per DB transaction) |
| Key remap on node removal | **31.9%** vs ~67% naive |
| Key distribution across nodes | 958–1045 / 1000 (even) |

All three non-functional targets (low-latency suggestions, high cache hit rate,
reduced DB writes) are met and measured at 200k-query scale on real ORCAS data
against a genuinely distributed Redis cache, and the consistent-hashing behavior is
demonstrated with concrete numbers.
