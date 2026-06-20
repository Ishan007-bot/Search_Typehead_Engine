# 🔎 Search Typeahead System

A search-as-you-type engine — the autocomplete box you see on Google, Amazon, and
content platforms — built to showcase the **backend data-system design** rather
than just a UI. As you type a prefix, it returns the most popular matching queries
in milliseconds; as people search, popularity updates and *trending* queries rise.

The interesting parts are all under the hood and **hand-written so every decision
is explainable** (no Redis, no external libraries doing the hard work):

- a **trie** with precomputed top-K for sub-millisecond prefix lookups,
- a **distributed cache** sharded with **consistent hashing** (virtual nodes),
- **recency-aware ranking** that surfaces trending queries without letting old
  spikes dominate forever,
- a **batch writer** that collapses thousands of searches into a handful of DB
  writes.

> **Measured:** ~4 ms p95 suggestion latency · 98% cache hit rate · 99.8% fewer DB
> writes via batching · only ~1/N keys remapped when a cache node is added/removed.
> Full report in [PERFORMANCE.md](PERFORMANCE.md).

---

## Table of contents

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Guided demo](#guided-demo-5-minutes)
- [How it works](#how-it-works)
- [Project layout](#project-layout)
- [Dataset](#dataset)
- [API reference](#api-reference)
- [Measuring performance](#measuring-performance)
- [Design docs & viva prep](#design-docs--viva-prep)
- [Rubric mapping](#rubric-mapping)
- [Troubleshooting](#troubleshooting)

---

## What it does

| Capability | Where it lives | One-line summary |
|---|---|---|
| Prefix suggestions, top-10 by popularity | `trie.py` | Walk to the prefix node, return its precomputed top-K. |
| Distributed cache + consistent hashing | `ring.py`, `cache.py`, `cache_cluster.py` | Each prefix is owned by a cache node chosen on a hash ring. |
| Search submission + count updates | `main.py`, `datastore.py` | `POST /search` returns a dummy response and bumps counts. |
| Trending / recency-aware ranking | `trending.py` | Blends all-time popularity with time-decayed recent activity. |
| Batch writes | `batch_writer.py` | Buffer → aggregate duplicates → flush in one transaction. |
| Single-page UI | `ui/index.html` | Debounced search box, ranked dropdown, trending, keyboard nav. |

---

## Quick start

**Requirements:** Python 3.10+ (developed on 3.12). That's it — only FastAPI and
uvicorn are installed; everything else is the standard library.

```bash
# 1. install dependencies
pip install -r requirements.txt

# 2. load the dataset into SQLite (creates data/typeahead.db)
python data/load_data.py --reset

# 3. run the server (the UI is served at the root URL)
python -m uvicorn app.main:app --reload

# 4. open the app
#    →  http://127.0.0.1:8000/
```

> **Port already in use?** Run `python -m uvicorn app.main:app --port 8800` and
> open `http://127.0.0.1:8800/`. (On some Windows setups port 8000 is taken by
> another app.)

---

## Guided demo (5 minutes)

A walkthrough that exercises every graded feature — good for a screen recording or
a viva.

**1. Suggestions ranked by popularity**
Open the UI and type `iph`. The dropdown shows `iphone`, `iphone 15`,
`iphone 15 pro`… ranked by search count, each with a popularity bar.

**2. The cache, live**
Type the same prefix again. The badge in the dropdown header flips from
`served from index` to **`served from cache`** — the second read was a cache hit.

**3. Consistent hashing**
Visit `http://127.0.0.1:8000/cache/debug?prefix=iph`. You'll see which node owns
the prefix, its position on the ring, and how a sample of prefixes spreads across
nodes. Try different prefixes (`sam`, `lap`, `head`) — they land on different nodes.

**4. Search submission updates counts**
In the UI, search a query (type it and press Enter). You get a "Searched" banner;
the count is bumped immediately in suggestions.

**5. Trending (recency beats raw popularity)**
Search a *low-popularity* query many times — e.g. `iphone holder`. Then compare:
```bash
curl "http://127.0.0.1:8000/suggest?q=i&mode=basic"     # #1 = iphone (all-time)
curl "http://127.0.0.1:8000/suggest?q=i&mode=enhanced"  # #1 = iphone holder (just spiked)
```
Same endpoint, same prefix → different #1. The **Trending now** section in the UI
reflects this too.

**6. Batch writes (write reduction)**
Fire a lot of searches, then inspect:
```bash
curl "http://127.0.0.1:8000/batch/stats"
# searches_enqueued: 3303, db_flushes: 9, write_reduction_ratio: 0.9973
```
Thousands of searches became single-digit DB transactions.

---

## How it works

### Read path — `GET /suggest`
```
prefix ─► consistent-hash ring ─► owning cache node
                                      │
                          ┌───────────┴───────────┐
                       HIT │                       │ MISS
                          ▼                        ▼
                 return cached list      trie.suggest() ─► cache.set(TTL) ─► return
                 (source = "cache")               (source = "trie")
```

### Write path — `POST /search`
```
POST /search ─► record_search():
                  • trie.bump()        → suggestions reflect the search instantly
                  • batch.enqueue()    → DB write is deferred + aggregated
                  • trending.record()  → recency signal for enhanced ranking
                  • cache.invalidate() → affected prefixes (both ranking modes)
                                              │
                  batch writer flushes on size OR timer
                                              ▼
                         SQLite — one transaction per batch (source of truth)
```

The single `record_search()` choke-point is why batching could be added late
without touching the endpoint or the response contract. Full diagram and rationale
in [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Project layout

```
app/
  main.py            FastAPI app — endpoints + wiring of every component
  datastore.py       SQLite source of truth (query, count, last_searched_at)
  trie.py            in-memory prefix index with per-node cached top-K
  ring.py            consistent-hash ring (md5, virtual nodes)
  cache.py           one cache node — TTL dict + hit/miss stats
  cache_cluster.py   N cache nodes behind the ring + invalidation + stats
  trending.py        recency tracking (time buckets) + decay-based blended score
  batch_writer.py    buffer → aggregate → flush (size/timer) + write metrics
data/
  queries.csv        sample e-commerce dataset (query,count)
  load_data.py       CSV → SQLite loader (normalizes columns; aggregates if needed)
ui/
  index.html         single-page UI (debounce, dropdown, keyboard nav, trending)
bench/
  benchmark.py       latency p50/p95/p99, cache hit rate, write reduction
README.md            this file
ARCHITECTURE.md      diagram, design choices, trade-offs, viva crib sheet
PERFORMANCE.md       measured numbers + how to reproduce them
```

---

## Dataset

**Source:** an e-commerce search-queries dataset (the kind published on Kaggle).
The full Kaggle file is large and requires a login, so it is **not committed**. A
small, representative sample lives in [`data/queries.csv`](data/queries.csv) in the
exact same `query,count` schema, so the project runs **offline out of the box**.

**Schema** (the only thing the loader needs):

```csv
query,count
iphone,100000
iphone 15,85000
laptop,95000
...
```

**Loading the full Kaggle file (or any e-commerce CSV)** — the loader normalizes
column names, so no code changes are needed:

```bash
python data/load_data.py path/to/kaggle_file.csv --reset
```

- **Query column** is auto-detected from: `query, search_term, search_query,
  keyword, term, title, product_name, name`.
- **Count column** is auto-detected from: `count, frequency, freq, popularity,
  searches, search_count, views, rating_count`.
- **No count column?** The loader **derives counts by aggregation** (`GROUP BY
  query`) — the row frequency becomes the count. This is the approach the
  assignment explicitly permits for count-less datasets.

`load_data.py` upserts (adds counts on conflict), so loading the sample and then
the full file accumulates rather than overwriting.

---

## API reference

Base URL: `http://127.0.0.1:8000`

### `GET /suggest`
Up to 10 prefix-matching suggestions.

| Param | Default | Description |
|---|---|---|
| `q` | `""` | The typed prefix (case-insensitive). |
| `mode` | `basic` | `basic` = sort by all-time count · `enhanced` = recency-aware. |

```bash
curl "http://127.0.0.1:8000/suggest?q=iph&mode=basic"
```
```json
{
  "query": "iph",
  "mode": "basic",
  "count": 10,
  "suggestions": [
    { "query": "iphone",    "score": 100261 },
    { "query": "iphone 15", "score": 85002 }
  ],
  "source": "cache",
  "owner_node": "cache-node-2"
}
```
`source` is `cache` (hit), `trie` (miss → recomputed), or `none` (empty prefix).
In `enhanced` mode each suggestion also includes `count` and `recent` so you can
see *why* it ranked where it did. Empty / whitespace / unmatched prefixes return
an empty list with HTTP 200 (handled gracefully, never an error).

### `POST /search`
Submit a search: returns the dummy response and records the query.

```bash
curl -X POST http://127.0.0.1:8000/search \
     -H "Content-Type: application/json" -d '{"query":"iphone"}'
```
```json
{ "message": "Searched", "query": "iphone", "count": 100262 }
```
New queries are inserted with an initial count; existing ones are incremented.
The update is reflected in `/suggest` immediately and persisted via batching.

### `GET /trending?limit=<n>`
Top queries by recent (time-decayed) activity — independent of all-time count.
```json
{ "trending": [ { "query": "iphone holder", "score": 40.0 } ], "window_seconds": 180 }
```

### `GET /cache/debug?prefix=<p>`
Shows consistent-hashing routing for a prefix.
```json
{
  "prefix": "iph", "normalized_key": "iph",
  "ring_position": 2952454930,
  "owner_node": "cache-node-2",
  "status": "miss",
  "all_nodes": ["cache-node-0", "cache-node-1", "cache-node-2"],
  "sample_distribution": { "cache-node-0": 3, "cache-node-1": 5, "cache-node-2": 8 }
}
```

### `GET /cache/stats`
Per-node and overall cache hit rate (used in the performance report).

### `GET /batch/stats`
Write-reduction evidence.
```json
{
  "searches_enqueued": 3303,
  "db_flushes": 9,
  "actual_db_transactions": 9,
  "naive_synchronous_writes": 3303,
  "write_reduction_ratio": 0.9973
}
```

### `POST /batch/flush`
Force a flush now (handy in demos so buffered counts hit SQLite immediately).

### `GET /health`
Liveness — queries loaded and active cache nodes.

---

## Measuring performance

With the server running in one terminal:

```bash
python bench/benchmark.py --base http://127.0.0.1:8000
# optional: --iters 2000 --searches 3000
```

It reports suggestion latency (p50/p95/p99, cold vs warm), cache hit rate, write
reduction from batching, and the consistent-hashing key distribution. It uses only
the standard library, so there is nothing extra to install.

> Note: the benchmark submits searches, which mutates the SQLite counts. Reset
> anytime with `python data/load_data.py --reset`.

Captured results and discussion: [PERFORMANCE.md](PERFORMANCE.md).

---

## Design docs & viva prep

[ARCHITECTURE.md](ARCHITECTURE.md) is the deep dive: full architecture diagram,
the reason behind every component, the trending scoring/windowing math, the
batch-write failure trade-offs, known limitations, and a **viva crib sheet** of
one-line answers to the design questions a reviewer is likely to ask.

---

## Rubric mapping

Where each graded requirement is satisfied, for quick verification.

| Requirement | Marks | Implementation | Evidence |
|---|---|---|---|
| Dataset ingestion | — | `data/load_data.py` + `data/queries.csv` | `GET /health` shows queries loaded |
| Search UI + suggestions dropdown | — | `ui/index.html` | open `/` |
| Suggestions API (top-10, prefix, by count) | — | `GET /suggest`, `trie.py` | API reference above |
| Search API + query-count updates | — | `POST /search`, `datastore.py` | returns `{"message":"Searched"}` |
| **Distributed cache + consistent hashing** | **60** | `ring.py`, `cache_cluster.py` | `GET /cache/debug`, PERFORMANCE §4 |
| **Trending searches** (recency + explanation) | **20** | `trending.py`, `mode=enhanced` | demo step 5, ARCHITECTURE §5 |
| **Batch writes** (+ failure trade-off) | **20** | `batch_writer.py` | `GET /batch/stats`, ARCHITECTURE §6 |
| Latency / hit-rate / write-reduction report | — | `bench/benchmark.py` | PERFORMANCE.md |
| Consistent-hashing behavior explanation | — | ARCHITECTURE §4 | 32.7% vs ~67% remap |

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `trie is empty` warning on startup | Run `python data/load_data.py --reset` first. |
| Port 8000 in use | Start with `--port 8800` and open that port. |
| Suggestions look stale after many searches | Counts are batched; force `POST /batch/flush`, or wait for the 2s timer. |
| Trending is empty | It populates only after searches happen — search a few queries. |
| Want a clean dataset again | `python data/load_data.py --reset`. |
