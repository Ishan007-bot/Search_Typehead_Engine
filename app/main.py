"""
FastAPI application — Search Typeahead System.

Milestone 1 (this file's current scope):
- Loads query-count data from SQLite into an in-memory Trie at startup.
- GET /suggest?q=<prefix> -> up to 10 prefix matches sorted by count desc.
- GET /health -> basic status.

Later milestones add: POST /search, distributed cache + consistent hashing,
GET /cache/debug, trending searches, and batch writes.
"""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse

from app.datastore import DataStore
from app.trie import Trie

MAX_SUGGESTIONS = 10
UI_DIR = Path(__file__).resolve().parent.parent / "ui"

# Module-level singletons, wired up in the lifespan handler.
store: DataStore | None = None
trie: Trie | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Build the in-memory trie from SQLite once, at startup."""
    global store, trie
    store = DataStore()
    trie = Trie(top_k=MAX_SUGGESTIONS)
    rows = store.all_rows()
    trie.build_from_rows(rows)
    print(f"[startup] Loaded {len(trie)} queries into the trie from SQLite.")
    if len(trie) == 0:
        print("[startup] WARNING: trie is empty. Run: python data/load_data.py")
    yield
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
    }


@app.get("/suggest")
def suggest(q: str = Query(default="", description="The prefix the user has typed")):
    """Return up to 10 suggestions whose query starts with the prefix `q`,
    sorted by all-time count (descending).

    Graceful handling:
    - empty / missing / whitespace `q` -> empty suggestions list (200).
    - mixed-case `q` -> matched case-insensitively.
    - prefix with no matches -> empty suggestions list (200).
    """
    prefix = (q or "").strip()
    results = trie.suggest(prefix, limit=MAX_SUGGESTIONS) if trie else []
    return {
        "query": prefix,
        "count": len(results),
        "suggestions": [{"query": text, "score": cnt} for text, cnt in results],
        "source": "trie",  # will become "cache"/"trie" once M4 lands
    }


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    # Keep the API resilient: never leak a stack trace to the UI.
    return JSONResponse(status_code=500, content={"error": "internal_error", "detail": str(exc)})
