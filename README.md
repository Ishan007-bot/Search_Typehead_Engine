# Search Typeahead System

A search autocomplete system (like the suggestion box on Google / Amazon) built
around the **backend data-system design**: how query-count data is stored, how
suggestions are served with low latency, how the cache is distributed with
**consistent hashing**, how rankings incorporate **recency (trending)**, and how
write pressure is reduced with **batch writes**.

- **Suggestions** ranked by search popularity, served via a distributed cache.
- **Search submission** that updates query counts.
- **Trending** searches via recency-aware ranking.
- **Batch writes** that turn thousands of searches into a handful of DB writes.

Everything is hand-written (trie, consistent-hash ring, cache, batch writer) so
every design decision is visible, explainable code — no Redis/black boxes.

---

## Quick start

```bash
# 1. install deps (FastAPI + uvicorn)
pip install -r requirements.txt

# 2. load the dataset into SQLite
python data/load_data.py --reset

# 3. run the server (the UI is served at /)
python -m uvicorn app.main:app --reload

# 4. open the UI
#    http://127.0.0.1:8000/
```

> On Windows, if port 8000 is taken, run with `--port 8800` and open that port.

---

## Project layout

```
app/
  main.py            FastAPI app: endpoints + wiring of all components
  datastore.py       SQLite source of truth (query, count, last_searched_at)
  trie.py            in-memory prefix index with per-node cached top-K
  cache.py           one cache node: TTL dict + hit/miss stats
  ring.py            consistent-hash ring (virtual nodes)
  cache_cluster.py   N cache nodes behind the ring + invalidation
  trending.py        recency tracking + decay-based blended scoring
  batch_writer.py    buffer -> aggregate -> flush (size/timer) + metrics
data/
  queries.csv        sample e-commerce dataset (query,count)
  load_data.py       CSV -> SQLite loader (schema-normalizing)
ui/
  index.html         single-page UI (debounce, dropdown, keyboard nav, trending)
bench/
  benchmark.py       latency p95 / cache hit rate / write reduction
ARCHITECTURE.md      design, diagram, trade-offs, and the viva crib sheet
PERFORMANCE.md       measured numbers + how to reproduce them
```

---

## Dataset

**Source:** an e-commerce search-queries dataset (the kind on Kaggle). The full
Kaggle file is large and requires a login, so it is **not committed**. A small
representative sample lives in [`data/queries.csv`](data/queries.csv) in the same
`query,count` schema, so the project runs offline immediately.

**Loading your own / the full file** — the loader normalizes columns, so any
e-commerce CSV works without code changes:

```bash
python data/load_data.py path/to/kaggle_file.csv --reset
```

- Query column accepted as any of: `query, search_term, search_query, keyword,
  term, title, product_name, name`.
- Count column accepted as any of: `count, frequency, freq, popularity,
  searches, search_count, views, rating_count`.
- **If there is no count column, the loader derives counts by aggregation**
  (`GROUP BY query`), which the assignment explicitly allows.

---

## API

| Method & path | Purpose | Notes |
|---|---|---|
| `GET /suggest?q=<prefix>&mode=<basic\|enhanced>` | Up to 10 prefix matches | `basic` = by count; `enhanced` = recency-aware. Served via cache, falls back to trie. |
| `POST /search` | Submit a search | Body `{"query":"..."}` → `{"message":"Searched","query":...,"count":...}`. Updates counts (batched) + recency. |
| `GET /trending?limit=<n>` | Top queries by recent activity | Pure recency (time-decayed), independent of all-time count. |
| `GET /cache/debug?prefix=<p>` | Cache routing | Owning node, ring position, hit/miss, sample distribution. |
| `GET /cache/stats` | Cache hit rate | Per-node + overall. |
| `GET /batch/stats` | Write-reduction evidence | Searches enqueued vs DB transactions. |
| `POST /batch/flush` | Force a flush | For demos (see counts hit SQLite without waiting). |
| `GET /health` | Liveness | Queries loaded + cache nodes. |

### Examples

```bash
# basic vs recency-aware ranking on the SAME endpoint
curl "http://127.0.0.1:8000/suggest?q=i&mode=basic"
curl "http://127.0.0.1:8000/suggest?q=i&mode=enhanced"

# submit a search
curl -X POST http://127.0.0.1:8000/search \
     -H "Content-Type: application/json" -d '{"query":"iphone"}'

# which cache node owns a prefix?
curl "http://127.0.0.1:8000/cache/debug?prefix=iph"

# how much did batching reduce writes?
curl "http://127.0.0.1:8000/batch/stats"
```

---

## Measuring performance

```bash
# with the server running:
python bench/benchmark.py --base http://127.0.0.1:8000
```

Reports suggestion latency (p50/p95/p99), cache hit rate, write reduction, and
the consistent-hashing key distribution. See [PERFORMANCE.md](PERFORMANCE.md) for
captured results and discussion.

---

## Design & trade-offs

See [ARCHITECTURE.md](ARCHITECTURE.md) for the architecture diagram, the reason
behind every component, the trending scoring/windowing logic, the batch-write
failure trade-offs, and a crib sheet of the design choices to be able to explain.
