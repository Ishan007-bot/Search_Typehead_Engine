"""
Redis-backed cache node — a real, separate cache process per logical node.

This is the OPTIONAL "distributed for real" backend (enable with USE_REDIS=1 and
`docker compose up`). It implements the exact same interface as the in-memory
CacheNode (get / set / invalidate / peek / clear / sweep_expired / stats), so the
CacheCluster and the consistent-hash ring don't change at all — only where the
cached suggestion lists physically live.

Design notes (viva):
- One RedisCacheNode == one Redis instance (its own port/process). The ring still
  decides which node owns a prefix; this just stores that node's keys in Redis
  instead of a Python dict. So the distribution is across genuinely separate
  processes, which is the strongest reading of "distributed cache".
- Values are JSON-encoded (the suggestion list). Redis stores bytes; JSON keeps
  it human-inspectable with `redis-cli GET`.
- TTL uses Redis's native per-key expiry (SET ... EX), so expiry is enforced by
  Redis itself — no background sweeper needed (sweep_expired is a no-op).
- Hit/miss counters are kept in-process (per node object) so /cache/stats reports
  the same shape as the in-memory backend.
"""

import json
import threading
from typing import Any, Optional


class RedisCacheNode:
    def __init__(self, name: str, host: str = "127.0.0.1", port: int = 6379,
                 ttl_seconds: float = 30.0):
        # Imported here so the app only needs the `redis` package when USE_REDIS=1.
        import redis  # type: ignore

        self.name = name
        self.host = host
        self.port = port
        self.ttl = ttl_seconds
        # decode_responses=False: we store/retrieve JSON bytes ourselves.
        self._r = redis.Redis(host=host, port=port, socket_connect_timeout=3)
        self._r.ping()  # fail fast at startup if the node is unreachable
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[Any]:
        raw = self._r.get(key)
        with self._lock:
            if raw is None:
                self.misses += 1
                return None
            self.hits += 1
        return json.loads(raw)

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        ttl_s = ttl if ttl is not None else self.ttl
        # ex must be an int >= 1 second; round up so short TTLs still expire.
        self._r.set(key, json.dumps(value), ex=max(1, int(round(ttl_s))))

    def peek(self, key: str) -> bool:
        """Currently cached? No hit/miss side effect (for /cache/debug)."""
        return bool(self._r.exists(key))

    def invalidate(self, key: str) -> bool:
        return self._r.delete(key) > 0

    def clear(self) -> int:
        """Flush only THIS node's database. Each node uses its own Redis instance,
        so flushdb is scoped to this node."""
        n = self._r.dbsize()
        self._r.flushdb()
        return int(n)

    def sweep_expired(self) -> int:
        # Redis enforces TTL itself; nothing to sweep.
        return 0

    def stats(self) -> dict:
        try:
            size = int(self._r.dbsize())
        except Exception:
            size = -1
        with self._lock:
            total = self.hits + self.misses
            return {
                "node": self.name,
                "endpoint": f"{self.host}:{self.port}",
                "size": size,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate": round(self.hits / total, 4) if total else 0.0,
                "ttl_seconds": self.ttl,
            }
