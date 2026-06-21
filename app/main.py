"""
FastAPI application — Search Typeahead System.

Current scope (through M4):
- Loads query-count data from SQLite into an in-memory Trie at startup.
- GET  /suggest?q=<prefix> -> up to 10 prefix matches, served via the
       distributed cache (consistent hashing) with a trie fallback.
- POST /search             -> dummy response + query-count update + cache invalidation.
- GET  /cache/debug?prefix -> which cache node owns a prefix, and hit/miss.
- GET  /cache/stats        -> per-node + overall cache hit rate.
- GET  /health.

Later milestones add: trending searches (M5) and batch writes (M6).
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Body, FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

from app.batch_writer import BatchWriter
from app.cache_cluster import CacheCluster
from app.datastore import DataStore
from app.trending import TrendingTracker
from app.trie import Trie

MAX_SUGGESTIONS = 10
INITIAL_COUNT = 1          # count assigned to a brand-new query on first search
MAX_QUERY_LEN = 200        # reject absurdly long inputs
CACHE_TTL_BASIC = 30.0     # basic (count) ranking changes slowly -> long TTL
CACHE_TTL_ENHANCED = 5.0   # recency ranking drifts every bucket tick -> short TTL
CACHE_SWEEP_SECONDS = 15.0 # how often the background task trims expired entries
CANDIDATE_POOL = 50        # how many count-ranked candidates the re-ranker considers
# Trending knobs (kept small here so the demo shows decay within a short run).
BUCKET_SECONDS = 30.0
WINDOW_BUCKETS = 6
DECAY = 0.7
BOOST = 3.0
# Batch-write knobs: flush when 50 distinct queries buffered OR every 2s.
BATCH_SIZE = 50
FLUSH_INTERVAL = 2.0
UI_DIR = Path(__file__).resolve().parent.parent / "ui"

# Module-level singletons, wired up in the lifespan handler.
store: DataStore | None = None
trie: Trie | None = None
cache: CacheCluster | None = None
trending: TrendingTracker | None = None
batch: BatchWriter | None = None


def _ck(prefix: str, mode: str) -> str:
    """Cache key namespaced by ranking mode, so basic and enhanced results never
    collide (the same prefix has a different answer in each mode)."""
    return f"{mode}:{prefix.strip().lower()}"


async def _cache_sweeper():
    """Background task: periodically drop expired cache entries so memory doesn't
    grow unbounded with stale prefixes. (Lazy expiry already handles correctness;
    this just reclaims space.)"""
    while True:
        await asyncio.sleep(CACHE_SWEEP_SECONDS)
        if cache is not None:
            cache.sweep_expired()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the trie from SQLite and start the cache cluster, at startup."""
    global store, trie, cache, trending, batch
    store = DataStore()
    trie = Trie(top_k=MAX_SUGGESTIONS)
    trie.build_from_rows(store.all_rows())
    cache = CacheCluster(ttl_seconds=CACHE_TTL_BASIC)
    trending = TrendingTracker(
        bucket_seconds=BUCKET_SECONDS, window_buckets=WINDOW_BUCKETS,
        decay=DECAY, boost=BOOST,
    )
    # The batch writer's flush function is the ONLY thing that writes search
    # increments to SQLite now. apply_increments() persists the whole aggregated
    # batch in one transaction and returns rows written.
    batch = BatchWriter(
        flush_fn=store.apply_increments,
        batch_size=BATCH_SIZE, flush_interval=FLUSH_INTERVAL,
    )
    batch.start()
    print(f"[startup] Loaded {len(trie)} queries into the trie from SQLite.")
    print(f"[startup] Cache cluster up with nodes: {cache.ring.nodes}")
    if len(trie) == 0:
        print("[startup] WARNING: trie is empty. Run: python data/load_data.py")
    sweeper = asyncio.create_task(_cache_sweeper())
    yield
    sweeper.cancel()
    # Graceful shutdown: stop the timer and flush the last partial batch BEFORE
    # closing the DB, so buffered searches aren't lost on a clean stop.
    if batch is not None:
        batch.stop()
    if store is not None:
        store.close()


app = FastAPI(title="Search Typeahead System", version="0.1.0", lifespan=lifespan)


@app.get("/")
def index():
    """Serve the single-page UI (same origin as the API, so no CORS needed)."""
    return FileResponse(UI_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "queries_loaded": len(trie) if trie else 0,
        "cache_nodes": cache.ring.nodes if cache else [],
    }


def _rank_basic(prefix: str) -> list[dict]:
    """BASIC ranking (60% version): top-K by all-time count, straight from the
    trie's per-node cached top-K."""
    results = trie.suggest(prefix, limit=MAX_SUGGESTIONS) if trie else []
    return [{"query": text, "score": cnt} for text, cnt in results]


def _rank_enhanced(prefix: str) -> list[dict]:
    """ENHANCED ranking (20% version): recency-aware re-rank.

    Take a WIDE pool of count-ranked candidates (so a recently-hot query can
    enter the top 10 even if its raw count wouldn't), then sort by the blended
    score = log10(1+count) + BOOST * decayed_recent_score. We expose both the
    blended score and the raw count so the difference vs basic is visible.
    """
    if not trie:
        return []
    pool = trie.candidates(prefix, limit=CANDIDATE_POOL)
    rescored = []
    for query, count in pool:
        blended = trending.blended_score(query, count) if trending else 0.0
        recent = trending.recent_score(query) if trending else 0.0
        rescored.append((query, count, blended, recent))
    rescored.sort(key=lambda x: x[2], reverse=True)
    return [
        {"query": q, "score": round(b, 4), "count": c, "recent": round(r, 4)}
        for (q, c, b, r) in rescored[:MAX_SUGGESTIONS]
    ]


@app.get("/suggest")
def suggest(
    q: str = Query(default="", description="The prefix the user has typed"),
    mode: str = Query(default="basic", description="Ranking mode: 'basic' (count) or 'enhanced' (recency-aware)"),
):
    """Return up to 10 prefix-matching suggestions.

    Two ranking modes via the SAME endpoint (assignment section 7):
      - mode=basic    -> sorted by all-time count (the 60% version)
      - mode=enhanced -> recency-aware blended score (the 20% version)

    Read path (M4 + M5):
        1. Route the (mode-namespaced) prefix to its owning cache node.
        2. CACHE HIT  -> return cached list (source="cache").
        3. CACHE MISS -> rank via the chosen mode, cache with a mode-specific TTL
                         (basic = long, enhanced = short since recency drifts),
                         return it (source="trie").

    Graceful handling: empty/whitespace/missing q -> empty list (not cached);
    mixed-case matched case-insensitively; no-match -> empty list.
    """
    prefix = (q or "").strip()
    mode = mode if mode in ("basic", "enhanced") else "basic"
    if not prefix:
        return {"query": prefix, "mode": mode, "count": 0, "suggestions": [], "source": "none"}

    key = _ck(prefix, mode)
    cached = cache.get(key) if cache else None
    if cached is not None:
        return {
            "query": prefix, "mode": mode, "count": len(cached),
            "suggestions": cached, "source": "cache", "owner_node": cache.owner(key),
        }

    payload = _rank_enhanced(prefix) if mode == "enhanced" else _rank_basic(prefix)
    if cache is not None:
        ttl = CACHE_TTL_ENHANCED if mode == "enhanced" else CACHE_TTL_BASIC
        cache.set(key, payload, ttl=ttl)
    return {
        "query": prefix, "mode": mode, "count": len(payload),
        "suggestions": payload, "source": "trie",
        "owner_node": cache.owner(key) if cache else None,
    }


def record_search(query: str) -> int:
    """Record one search for `query` and return its new count.

    This is the single choke-point for count updates. As of M6:
      - Trie (read model): bumped synchronously, so /suggest reflects the search
        immediately (reads stay fresh without waiting for a DB flush).
      - SQLite (source of truth): NOT written here. The increment is ENQUEUED
        into the batch writer, which aggregates duplicates and flushes the whole
        batch in one transaction (size- or timer-triggered). This is the M6 swap
        that the M3 seam was designed for — the endpoint above is unchanged.
      - Recency tracker: updated for trending / enhanced ranking.

    Why bump the trie now but defer the DB write? The trie is our fast read
    model; keeping it current means suggestions reflect a search instantly. The
    DB is the durable record; batching its writes is where the write-reduction
    win comes from. They reconcile because both apply the SAME +INITIAL_COUNT.
    """
    norm = query.strip().lower()
    # Trie bump returns the authoritative new count (existing + 1, or INITIAL_COUNT).
    new_count = trie.bump(query, delta=INITIAL_COUNT)
    # Enqueue the SQLite increment instead of writing it now (batched).
    if batch is not None:
        batch.enqueue(norm, INITIAL_COUNT)
    # Record recency for trending / enhanced ranking (M5): this search lands in
    # the current time bucket for this query.
    if trending is not None:
        trending.record(norm, INITIAL_COUNT)
    # Invalidate cached suggestion lists that could now be stale. Bumping
    # "iphone" can re-rank every prefix of it ("i","ip",...,"iphone") in BOTH
    # ranking modes, so we invalidate both namespaces. (TTL is the backstop.)
    if cache is not None:
        prefixes = []
        for i in range(1, len(norm) + 1):
            p = norm[:i]
            prefixes.append(_ck(p, "basic"))
            prefixes.append(_ck(p, "enhanced"))
        cache.invalidate_prefixes(prefixes)
    return new_count


@app.post("/search")
def search(payload: dict = Body(...)):
    """Submit a search.

    Request body: {"query": "<text>"}
    Response:     {"message": "Searched", "query": "<text>", "count": <new count>}

    Behavior (assignment section 4.2):
    - Returns the dummy response {"message": "Searched"}.
    - Updates the query-count store: existing query -> count increases;
      new query -> inserted with an initial count.
    - The update is reflected in /suggest right away (trie is bumped in-process).
    """
    raw = payload.get("query") if isinstance(payload, dict) else None
    query = (raw or "").strip()
    if not query:
        return JSONResponse(
            status_code=400,
            content={"error": "empty_query", "message": "Provide a non-empty 'query'."},
        )
    if len(query) > MAX_QUERY_LEN:
        query = query[:MAX_QUERY_LEN]

    new_count = record_search(query)
    return {"message": "Searched", "query": query.lower(), "count": new_count}


@app.get("/trending")
def get_trending(limit: int = Query(default=10, ge=1, le=50)):
    """Top queries by recent (time-decayed) activity — what's hot right now.

    Independent of all-time popularity: ranks purely on the recency signal, so a
    query that just spiked appears even if its all-time count is small. As its
    activity ages out of the sliding window, it drops off automatically.
    """
    if trending is None:
        return {"trending": []}
    items = trending.trending(limit=limit)
    return {
        "trending": [{"query": q, "score": round(s, 4)} for q, s in items],
        "window_seconds": BUCKET_SECONDS * WINDOW_BUCKETS,
    }


@app.get("/trending/stats")
def trending_stats():
    if trending is None:
        return {"error": "not_ready"}
    return trending.stats()


@app.get("/batch/stats")
def batch_stats():
    """Write-reduction evidence (assignment section 8): how many searches were
    enqueued vs how many actual DB transactions (flushes) happened."""
    if batch is None:
        return JSONResponse(status_code=503, content={"error": "batch_not_ready"})
    return batch.stats()


@app.post("/batch/flush")
def batch_flush():
    """Force-flush the buffer now (handy for the demo so you can see counts hit
    SQLite without waiting for the timer)."""
    if batch is None:
        return JSONResponse(status_code=503, content={"error": "batch_not_ready"})
    rows = batch.flush_now()
    return {"flushed_rows": rows, "stats": batch.stats()}


@app.get("/cache/debug")
def cache_debug(
    prefix: str = Query(default="", description="Prefix to inspect"),
    mode: str = Query(default="basic", description="Ranking mode the prefix is cached under"),
):
    """Show which cache node owns `prefix` and whether it is currently a hit/miss.

    Demonstrates consistent-hashing routing (assignment section 5). Includes the
    ring position so you can see *why* a prefix lands on a given node, and the
    distribution of a sample of prefixes across nodes to show even spread.

    IMPORTANT: /suggest caches under a mode-namespaced key (e.g. "basic:iph") so
    that basic and recency-aware results don't collide. We inspect that SAME key
    here, otherwise the reported owner/hit-status wouldn't match what /suggest
    actually stores.
    """
    if cache is None:
        return JSONResponse(status_code=503, content={"error": "cache_not_ready"})
    mode = mode if mode in ("basic", "enhanced") else "basic"
    key = _ck(prefix, mode)
    info = cache.debug(key)
    # Re-label so the response talks about the user's prefix, not the internal key.
    info["prefix"] = prefix
    info["mode"] = mode
    info["cache_key"] = key
    # Bonus: how a sample of prefixes (in this mode) spreads across nodes.
    sample = ["a", "b", "c", "i", "ip", "iph", "sam", "lap", "head", "watch",
              "camera", "mouse", "tablet", "speaker", "shoes", "charger"]
    info["sample_distribution"] = cache.ring.distribution([_ck(p, mode) for p in sample])
    return info


@app.get("/cache/stats")
def cache_stats():
    """Per-node and overall cache hit rate (for the performance report)."""
    if cache is None:
        return JSONResponse(status_code=503, content={"error": "cache_not_ready"})
    return cache.stats()


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    # Keep the API resilient: never leak a stack trace to the UI.
    return JSONResponse(status_code=500, content={"error": "internal_error", "detail": str(exc)})
