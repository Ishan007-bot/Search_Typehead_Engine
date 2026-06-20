"""
SQLite-backed primary data store (the source of truth) for query counts.

Design notes (for the viva):
- SQLite is the *primary data store* / source of truth. It is on-disk and durable.
- The fast suggestion path does NOT hit SQLite on every keystroke. Instead an
  in-memory Trie (see trie.py) is built from SQLite at startup, and a distributed
  cache (added in M4) sits in front of it. SQLite is read at startup and written
  to by the batch writer (M6).
- We keep an index on `count` so the fallback SQL suggestion query
  (used to rebuild the trie / for cache misses if needed) is fast.

Schema:
    queries(
        query            TEXT PRIMARY KEY,   -- the search string (lowercased)
        count            INTEGER NOT NULL,   -- all-time popularity
        last_searched_at REAL                -- unix epoch seconds of last search (recency, used in M5)
    )
"""

import sqlite3
import threading
import time
from pathlib import Path
from typing import Iterable, List, Tuple

# Default DB location: <project>/data/typeahead.db
DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "typeahead.db"


class DataStore:
    """Thin wrapper around SQLite holding query -> count data."""

    def __init__(self, db_path: Path | str = DEFAULT_DB_PATH):
        self.db_path = str(db_path)
        # check_same_thread=False so FastAPI's threadpool can share the connection;
        # we guard writes with a lock to keep things simple and correct.
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS queries (
                    query            TEXT PRIMARY KEY,
                    count            INTEGER NOT NULL DEFAULT 0,
                    last_searched_at REAL
                );
                CREATE INDEX IF NOT EXISTS idx_queries_count
                    ON queries(count DESC);
                """
            )
            self._conn.commit()

    # ----- bulk load (used by load_data.py) -----------------------------------

    def upsert_many(self, rows: Iterable[Tuple[str, int]]) -> int:
        """Insert/replace many (query, count) rows. Returns number of rows written.

        On conflict we ADD counts so re-running the loader (or loading the sample
        plus the full Kaggle file) accumulates rather than silently overwriting.
        """
        rows = [(q.strip().lower(), int(c)) for q, c in rows if q and q.strip()]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO queries(query, count, last_searched_at)
                VALUES(?, ?, NULL)
                ON CONFLICT(query) DO UPDATE SET count = count + excluded.count
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    # ----- writes (used by the batch writer in M6) ----------------------------

    def apply_increments(self, increments: dict[str, int]) -> int:
        """Apply a batch of aggregated count increments in ONE transaction.

        `increments` maps query -> amount to add. New queries are inserted.
        Returns the number of distinct queries written. This is the method the
        batch writer calls; counting calls to it = the DB write-reduction metric.
        """
        if not increments:
            return 0
        now = time.time()
        rows = [(q, amt, now) for q, amt in increments.items()]
        with self._lock:
            self._conn.executemany(
                """
                INSERT INTO queries(query, count, last_searched_at)
                VALUES(?, ?, ?)
                ON CONFLICT(query) DO UPDATE SET
                    count = count + excluded.count,
                    last_searched_at = excluded.last_searched_at
                """,
                rows,
            )
            self._conn.commit()
        return len(rows)

    # ----- reads --------------------------------------------------------------

    def all_rows(self) -> List[sqlite3.Row]:
        """Return every (query, count, last_searched_at) row. Used to build the trie."""
        with self._lock:
            cur = self._conn.execute(
                "SELECT query, count, last_searched_at FROM queries"
            )
            return cur.fetchall()

    def suggest_sql(self, prefix: str, limit: int = 10) -> List[Tuple[str, int]]:
        """Fallback prefix search straight from SQLite, sorted by count desc.

        Not used on the hot path (the trie is), but kept for correctness checks
        and as a pure-DB reference implementation.
        """
        prefix = (prefix or "").strip().lower()
        like = prefix.replace("%", r"\%").replace("_", r"\_") + "%"
        with self._lock:
            cur = self._conn.execute(
                """
                SELECT query, count FROM queries
                WHERE query LIKE ? ESCAPE '\\'
                ORDER BY count DESC
                LIMIT ?
                """,
                (like, limit),
            )
            return [(r["query"], r["count"]) for r in cur.fetchall()]

    def count_queries(self) -> int:
        with self._lock:
            cur = self._conn.execute("SELECT COUNT(*) AS n FROM queries")
            return cur.fetchone()["n"]

    def close(self) -> None:
        with self._lock:
            self._conn.close()
