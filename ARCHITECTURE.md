# Architecture & Design

This document explains how the system is built, **why** each choice was made, the
trade-offs involved, and a crib sheet for the viva. It is organized so you can
defend every major decision.

---

## 1. High-level architecture

```
                                  ┌─────────────────────────────────────────────┐
                                  │                API server (FastAPI)           │
   ┌──────────┐  GET /suggest     │                                               │
   │          │ ───(debounced)──► │   ┌──────────────────────────────────────┐    │
   │ Browser  │                   │   │  Consistent-hash ring  (ring.py)     │    │
   │   UI     │                   │   │  prefix ─► owning cache node         │    │
   │          │  POST /search     │   └───────────────┬──────────────────────┘    │
   │          │ ────────────────► │            hit │  │ miss                       │
   └──────────┘                   │   ┌────────────▼──▼─────────────────────┐     │
                                  │   │  Distributed cache (cache_cluster)   │     │
                                  │   │  Redis-0  Redis-1  Redis-2 (TTL)     │     │
                                  │   └────────────┬─────────────────────────┘     │
                                  │          miss  │ ▲ populate                     │
                                  │   ┌────────────▼─┴─────────────────────┐       │
                                  │   │  Trie read model (trie.py)         │       │
                                  │   │  prefix ─► cached top-K by count   │◄──┐   │
                                  │   └────────────────────────────────────┘   │   │
                                  │                                    bump │   │   │
                                  │   ┌────────────────────────────────────┐ │   │
   POST /search ──────────────────►  │  record_search()  (the choke-point)│─┘   │   │
                                  │   │   • trie.bump (read stays fresh)   │     │   │
                                  │   │   • batch.enqueue (defer DB write) │     │   │
                                  │   │   • trending.record (recency)      │     │   │
                                  │   │   • cache.invalidate (both modes)  │     │   │
                                  │   └───────────────┬────────────────────┘     │   │
                                  │            enqueue│                          │   │
                                  │   ┌───────────────▼────────────────────┐    │   │
                                  │   │  Batch writer (batch_writer.py)    │    │   │
                                  │   │  buffer ─► aggregate ─► flush       │────┘   │
                                  │   │  (size or timer trigger)            │ apply  │
                                  │   └───────────────┬────────────────────┘        │
                                  └───────────────────┼─────────────────────────────┘
                                                      │ one transaction per batch
                                              ┌───────▼────────┐
                                              │   SQLite       │  source of truth
                                              │ (datastore.py) │  (query, count, last_searched_at)
                                              └────────────────┘
```

**Read path** (`/suggest`): ring → cache node → **hit** returns instantly; **miss**
→ rank from the trie → store in cache (TTL) → return.

**Write path** (`/search`): bump trie (reads stay fresh) → enqueue into batch
buffer (DB write deferred) → record recency → invalidate affected cache keys.

---

## 2. Data model & storage

**SQLite is the source of truth.** One table:

```sql
queries(query TEXT PRIMARY KEY, count INTEGER, last_searched_at REAL)
```

- `query` is normalized (lowercased, trimmed) so case-insensitive matching is free.
- Index on `count DESC` so the fallback SQL suggestion query is fast.
- `last_searched_at` exists for recency, but the live recency signal is held
  in-memory (see §5) — the column is the durable record.

**Why SQLite (not pure in-memory)?** It is real, on-disk, and durable, and it
lets us *prove* the write-reduction metric (count rows / transactions). It needs
zero setup, so the project "runs locally easily."

**The trie is a derived read model, not the source of truth.** It is rebuilt from
SQLite at startup. This separation is what lets the cache and batch writer slot in
cleanly: the DB stays durable; the fast layers sit in front.

---

## 3. Serving suggestions fast — the Trie

A `LIKE 'prefix%'` SQL query works but hits disk on every keystroke. Typeahead is
read-heavy and latency-critical, so we hold the index in RAM as a **trie**.

**Key optimization:** every trie node caches its own **top-K (by count)**
suggestions. Answering `/suggest` is then: walk to the prefix node
(`O(len(prefix))`) and return its precomputed list — no subtree scan at request
time.

**Trade-off:** we pay at *write* time (refresh top-K along the inserted path) to
win at *read* time. That is exactly right for a read-heavy workload.

---

## 4. Distributed cache + consistent hashing  *(the core 60% — most likely viva topic)*

### Why a cache
Suggestion results are read constantly and change slowly. We cache the **finished
result list per prefix**, so a hit returns with zero ranking work.

### Where the cache lives — three real Redis nodes
Each logical cache node is a **separate Redis process** (`redis-0/1/2` on ports
6379/6380/6381, started by `docker-compose.yml`). This makes the cache *genuinely*
distributed — independent processes the app routes between over TCP, not one
in-process map. The split of responsibility is the key point:

- **Our consistent-hash ring** (`ring.py`) decides *which node* owns a prefix.
- **Redis** stores that node's cached suggestion lists and enforces TTL natively.

So the graded, must-explain logic (the distribution) is code we wrote; Redis is
just the storage behind each node. `redis_cache.py` and the (now-removed) earlier
in-memory node share the same interface, which is why swapping the storage never
touched the ring, routing, invalidation, or `/cache/debug`.

### Why consistent hashing (not `hash(key) % N`)
The naive shard is `hash(key) % N`. It works until `N` changes — add or remove
**one** cache node and `N` changes, so the mapping changes for **almost every
key**: the whole cache is suddenly "owned" by a different node → mass misses →
every request stampedes SQLite at once.

**Consistent hashing** places nodes on a circular keyspace (the ring). To find a
key's owner, hash the key onto the ring and walk **clockwise** to the first node.
Now adding/removing a node only re-homes the keys in **one arc** (~`1/N`), not all
of them.

**Measured proof (3 nodes, 3000 keys):**

| Event | Keys remapped |
|---|---|
| Remove 1 of 3 nodes (consistent hashing) | **32.7%** (~1/N) |
| Same with naive `hash % N` | ~67% |
| Every key *not* on the removed node | **kept its owner** |

### Virtual nodes
With few physical nodes, random placement leaves uneven gaps. We place each
physical node at **150 points** on the ring (vnodes), so arcs even out. Measured
distribution over 3000 keys: **980 / 1000 / 1028** across 3 nodes (ideal 1000).

### md5, not Python's `hash()`
Python salts `hash()` per process, so the ring would place keys differently on
every restart. We use **md5** (stable, deterministic) so routing is reproducible —
essential for a "distributed" cache where nodes must agree.

### Expiry **and** invalidation (belt and suspenders)
- **Invalidation** keeps the cache correct right after a write we know about: a
  search on `weather` invalidates the cached lists for every prefix of it
  (`w`, `we`, …, `weather`) in both ranking modes.
- **TTL** is the safety net that bounds how stale *any* entry can get, even if a
  write path ever misses a prefix. Redis enforces TTL natively (per-key expiry),
  so expired entries vanish on their own — no sweeper needed.

---

## 5. Trending — recency-aware ranking  *(20%)*

The same `/suggest` endpoint supports two modes:

- `mode=basic` — sort by all-time count (the 60% behavior).
- `mode=enhanced` — recency-aware blended score.

### (1) How recent searches are tracked
Per-query **time-bucketed counters**: time is chopped into fixed buckets
(`bucket_seconds`); each search increments the current bucket for that query. Only
the last `window_buckets` buckets are kept (sliding window). O(1) per search,
bounded memory.

### (2) How recent activity affects ranking
```
score(q) = log10(1 + all_time_count(q))      # historical popularity, compressed
         + BOOST * recent_score(q)           # decayed recent activity
recent_score(q) = Σ hits_in_bucket_b * decay^(age_of_b)   over buckets in window
```
The `log10` **compresses** the historical term so a 100k query (log ≈ 5) can't
permanently bury a fresh one — that compression is what lets recency actually move
the ranking. `BOOST` tunes how aggressively.

A recently-hot but low-count query must be *visible* to the re-ranker, so enhanced
mode pulls a **wide candidate pool (top-50 by count)** from the trie and re-ranks
that — otherwise such a query could never enter the top-10 to begin with.

### (3) How a short-lived spike doesn't over-rank forever  *(the critical point)*
`recent_score` sums **only buckets inside the sliding window, with decay**. When a
query stops being searched, its buckets age past the window and its decayed weight
falls to ~0 within `window_buckets` ticks → it falls back to its historical rank.
The boost is **temporary by construction**; nothing keeps it alive but continued
searching. *(Verified: a query moved past the window scores exactly 0.)*

### (4) How the cache stays correct when rankings change
- Recency scores drift every bucket tick **even with no writes**, so enhanced-mode
  cache entries use a **short TTL (5s)** vs basic mode's **30s**.
- On a write, affected prefixes are invalidated in **both** mode namespaces
  (`basic:` and `enhanced:`).

### (5) Trade-offs (freshness / latency / complexity)

| Knob | Increase it → | Trade-off |
|---|---|---|
| short enhanced TTL | fresher recency | more cache misses → higher latency |
| `BOOST` | recency dominates more | risk of jumpy rankings |
| `decay` (→1) | spikes linger longer | slower to forget |
| `window_buckets` | longer memory | more memory, slower fade |

Blended scoring + wide re-rank is more CPU than a raw count sort — paid only on an
enhanced-mode cache miss.

### Demonstrated difference (real ORCAS data)
For the prefix `weath`, the all-time leader is `weather` (~2.4k clicks). After
spiking the lower-ranked sibling `weather.com` (~1k clicks) with a burst of
searches:

| Mode | #1 result | Why (from the `enhanced` response's `count`/`recent` fields) |
|---|---|---|
| `basic` | `weather` | all-time click count wins |
| `enhanced` | `weather.com` | recency boost (blended ~306 vs `weather`'s ~156) |

Same endpoint, same prefix → different #1. Basic ranking is unchanged, proving the
two modes are genuinely different.

---

## 6. Batch writes  *(20%)*

### The problem
Writing SQLite synchronously on every `/search` = one transaction per submit.
Under load that floods the DB with tiny writes that mostly touch the same popular
queries.

### The approach: buffer → aggregate → flush
1. **Buffer**: each `/search` adds to an in-memory dict (no DB write).
2. **Aggregate**: duplicates collapse — `8×"weather"` → `{"weather": 8}` = one row.
3. **Flush** (single transaction) on whichever fires first:
   - **size**: buffer holds ≥ `BATCH_SIZE` (50) distinct queries, or
   - **timer**: every `FLUSH_INTERVAL` (2s).

### Measured write reduction
**3000 searches → 5 DB transactions = 99.83% fewer writes** (600 searches per
transaction). See [PERFORMANCE.md](PERFORMANCE.md).

### Why reads stay fresh despite deferred writes  *(subtle, worth raising)*
`record_search()` bumps the **trie synchronously** but **defers the SQLite write**.
So `/suggest` reflects a search instantly (read model current) while the durable
write is batched (throughput win). They reconcile because both apply the same `+1`.

### Failure trade-off  *(the assignment explicitly wants this discussed)*
- Buffered counts live in RAM.
- **Graceful shutdown flushes them** (implemented & verified).
- **A hard crash between flushes loses that batch** — counts undercount slightly
  until traffic re-accumulates. Loss window ≤ `flush_interval` (2s) or `batch_size`
  (50) searches, whichever first.
- **Why acceptable here:** popularity counts are statistical, not transactional;
  losing a few seconds of increments slightly delays a ranking shift, it doesn't
  break correctness.
- **Stronger durability (discussed, not implemented):** append each search to a
  write-ahead log before buffering and replay on restart. Deliberately not done —
  a per-search WAL append re-introduces exactly the per-write cost batching exists
  to remove. It's a conscious latency/throughput-vs-durability trade.

---

## 7. The `record_search()` seam  *(the clean-layering point)*

All count updates funnel through one helper, `record_search()`. This is why M6's
batch writer changed essentially one line of the write path
(`store.apply_increments(...)` → `batch.enqueue(...)`) without touching the
endpoint, the UI, or the response contract. "M3 establishes the update semantics
and the choke-point; M6 swaps the write *strategy* behind that choke-point."

---

## 8. Known limitations (honest scope)

- **Single app process (one trie).** The cache is genuinely distributed across
  three separate Redis processes, but the *app* itself is one process — the trie
  and trending state live in its memory. On multiple app workers each would have
  its own trie; SQLite is the shared truth and periodic rebuilds + the shared Redis
  cache would reconcile them. Right scope for a local demo.
- **Trie rebuild on startup** is O(rows): ~8s for the 200k-query ORCAS dataset
  (one-time boot cost). It's the deliberate price for fast reads — top-K is
  precomputed once so every `/suggest` is single-digit ms. Could be amortized
  (persist/lazy-build) at larger scale; fine for the assignment dataset.
- **Recency state is in-memory** (not persisted) — by design; it's a live signal.

---

## 9. Viva crib sheet (one-liners)

- **Why a trie?** Read-heavy typeahead; precompute top-K per node, pay at write,
  win at read.
- **Why consistent hashing?** `hash%N` remaps ~all keys when N changes →
  stampede; ring remaps only ~1/N. *Proof: 32.7% vs ~67% on node removal.*
- **Why virtual nodes?** Even load with few physical nodes (measured 980–1028/3000).
- **Why md5 over `hash()`?** Python salts `hash()` per process; ring must be stable.
- **TTL vs invalidation?** Invalidation = correct after known writes; TTL = bound
  staleness if a write is ever missed. Both.
- **How does trending avoid permanent over-ranking?** Sliding window + decay →
  recency weight → 0 once searching stops → falls back to historical rank.
- **Why bump trie now but defer DB write?** Reads stay fresh; writes get batched.
- **What if it crashes before a flush?** Lose ≤ one batch of counts; acceptable for
  statistical popularity; WAL would fix it but re-adds per-write cost.
- **Why funnel writes through `record_search()`?** One seam → swap write strategy
  without changing the API.
