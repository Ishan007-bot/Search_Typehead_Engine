"""
A single logical cache node.

It stores suggestion RESULT LISTS keyed by prefix (not raw rows). Caching the
finished answer means a hit returns instantly with zero ranking work.

Features required by the assignment:
- TTL expiry: every entry has an expiry timestamp; a key past its TTL is a MISS
  (lazy expiry — we check on read; a background sweep also trims it).
- Invalidation: entries can be deleted explicitly when their ranking changes
  (e.g. after a /search write).
- Hit/miss stats: so we can report cache hit rate in the performance section.

Why TTL *and* invalidation (viva point):
- Invalidation keeps the cache correct right after a write we know about.
- TTL is the safety net: it bounds how stale ANY entry can get, even if some
  write path forgot to invalidate it. Belt and suspenders.
"""

import threading
import time
from typing import Any, Optional


class CacheNode:
    def __init__(self, name: str, ttl_seconds: float = 30.0):
        self.name = name
        self.ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}   # key -> (expiry_epoch, value)
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        """Return the cached value, or None on miss/expiry. Updates hit/miss."""
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None
            expiry, value = entry
            if now >= expiry:                 # lazy expiry
                del self._store[key]
                self.misses += 1
                return None
            self.hits += 1
            return value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        expiry = time.time() + (ttl if ttl is not None else self.ttl)
        with self._lock:
            self._store[key] = (expiry, value)

    def invalidate(self, key: str) -> bool:
        """Drop one key. Returns True if it was present."""
        with self._lock:
            return self._store.pop(key, None) is not None

    def clear(self) -> int:
        with self._lock:
            n = len(self._store)
            self._store.clear()
            return n

    def sweep_expired(self) -> int:
        """Actively remove expired entries (called by a background task)."""
        now = time.time()
        with self._lock:
            dead = [k for k, (exp, _) in self._store.items() if now >= exp]
            for k in dead:
                del self._store[k]
            return len(dead)

    def stats(self) -> dict:
        with self._lock:
            total = self.hits + self.misses
            return {
                "node": self.name,
                "size": len(self._store),
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0,
                "ttl_seconds": self.ttl,
            }
