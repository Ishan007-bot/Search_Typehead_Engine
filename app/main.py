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

from app.cache_cluster import CacheCluster
from app.datastore import DataStore
from app.trie import Trie

MAX_SUGGESTIONS = 10
INITIAL_COUNT = 1          # count assigned to a brand-new query on first search
MAX_QUERY_LEN = 200        # reject absurdly long inputs
CACHE_TTL_SECONDS = 30.0   # how long a cached suggestion list stays fresh
CACHE_SWEEP_SECONDS = 15.0 # how often the background task trims expired entries
UI_DIR = Path(__file__).resolve().parent.parent / "ui"

# Module-level singletons, wired up in the lifespan handler.
store: DataStore | None = None
trie: Trie | None = None
cache: CacheCluster | None = None


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
    global store, trie, cache
    store = DataStore()
    trie = Trie(top_k=MAX_SUGGESTIONS)
    trie.build_from_rows(store.all_rows())
    cache = CacheCluster(ttl_seconds=CACHE_TTL_SECONDS)
    print(f"[startup] Loaded {len(trie)} queries into the trie from SQLite.")
    print(f"[startup] Cache cluster up with nodes: {cache.ring.nodes}")
    if len(trie) == 0:
        print("[startup] WARNING: trie is empty. Run: python data/load_data.py")
    sweeper = asyncio.create_task(_cache_sweeper())
    yield
    sweeper.cancel()
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


@app.get("/suggest")
def suggest(q: str = Query(default="", description="The prefix the user has typed")):
    """Return up to 10 suggestions whose query starts with the prefix `q`,
    sorted by all-time count (descending).

    Read path (M4):
        1. Route the prefix to its owning cache node (consistent hashing).
        2. CACHE HIT  -> return the cached list (source="cache"). No ranking work.
        3. CACHE MISS -> ask the trie, cache the result with a TTL, return it
                         (source="trie").

    Graceful handling:
    - empty / missing / whitespace `q` -> empty list, not cached (200).
    - mixed-case `q` -> matched case-insensitively (cache key is normalized).
    - prefix with no matches -> empty list (200).
    """
    prefix = (q or "").strip()
    if not prefix:
        return {"query": prefix, "count": 0, "suggestions": [], "source": "none"}

    # 1+2. Try the cache first.
    cached = cache.get(prefix) if cache else None
    if cached is not None:
        return {
            "query": prefix,
            "count": len(cached),
            "suggestions": cached,
            "source": "cache",
            "owner_node": cache.owner(prefix),
        }

    # 3. Miss -> compute from the trie, then populate the cache.
    results = trie.suggest(prefix, limit=MAX_SUGGESTIONS) if trie else []
    payload = [{"query": text, "score": cnt} for text, cnt in results]
    if cache is not None:
        cache.set(prefix, payload)
    return {
        "query": prefix,
        "count": len(payload),
        "suggestions": payload,
        "source": "trie",
        "owner_node": cache.owner(prefix) if cache else None,
    }


def record_search(query: str) -> int:
    """Record one search for `query` and return its new count.

    This is the single choke-point for count updates. In M3 it writes through
    synchronously:
      - SQLite (source of truth): +1 (insert with INITIAL_COUNT if new)
      - Trie (read model): bump so /suggest reflects it immediately
    In M6 the SQLite write is replaced by an enqueue into the batch buffer; the
    endpoint and trie update stay exactly the same. Keeping this seam here is why
    M6 won't require touching the endpoint.
    """
    # Trie bump returns the authoritative new count (existing + 1, or INITIAL_COUNT).
    new_count = trie.bump(query, delta=INITIAL_COUNT)
    # Persist the same increment to SQLite (one query -> +INITIAL_COUNT).
    store.apply_increments({query.strip().lower(): INITIAL_COUNT})
    # Invalidate cached suggestion lists that could now be stale. Bumping
    # "iphone" can change the ranking for every prefix of it: "i","ip",...,"iphone".
    # We invalidate exactly those prefixes so the next /suggest recomputes a fresh
    # list. (TTL is the backstop if a prefix is somehow missed.)
    if cache is not None:
        norm = query.strip().lower()
        prefixes = [norm[:i] for i in range(1, len(norm) + 1)]
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


@app.get("/cache/debug")
def cache_debug(prefix: str = Query(default="", description="Prefix to inspect")):
    """Show which cache node owns `prefix` and whether it is currently a hit/miss.

    Demonstrates consistent-hashing routing (assignment section 5). Includes the
    ring position so you can see *why* a prefix lands on a given node, and the
    distribution of a sample of prefixes across nodes to show even spread.
    """
    if cache is None:
        return JSONResponse(status_code=503, content={"error": "cache_not_ready"})
    info = cache.debug(prefix)
    # Bonus: show how a sample of prefixes spreads across nodes (even distribution).
    sample = ["a", "b", "c", "i", "ip", "iph", "sam", "lap", "head", "watch",
              "camera", "mouse", "tablet", "speaker", "shoes", "charger"]
    info["sample_distribution"] = cache.ring.distribution(sample)
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
