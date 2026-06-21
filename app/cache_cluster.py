"""
Distributed cache cluster = N real Redis nodes + a ConsistentHashRing in front.

This is the "distributed cache" the assignment asks for. The ring (which we wrote,
see ring.py) decides which node owns a prefix; the nodes are genuinely separate
Redis processes (one per logical node, on their own ports), so the distribution is
across real, independent processes — the strongest reading of "distributed cache".

Run it with `docker compose up` (three Redis containers on 6379/6380/6381 plus the
app). To point at Redis without Docker, set REDIS_NODES to host:port pairs.

What the ring owns vs what Redis owns (viva point):
  - OUR consistent-hash ring decides which node a prefix maps to, using virtual
    nodes for even spread and minimal remapping on membership change.
  - REDIS stores that node's cached suggestion lists and enforces TTL natively.
  The routing/distribution logic is ours and fully explainable; Redis is just the
  storage behind each node.

Routing: for any prefix key, the ring picks the owning node. The same key always
maps to the same node (until membership changes), so reads and writes for a key
go to one place — that's what makes a hit possible.
"""

import os
from typing import Any, List, Optional

from app.redis_cache import RedisCacheNode
from app.ring import ConsistentHashRing

DEFAULT_NODES = ["cache-node-0", "cache-node-1", "cache-node-2"]

# Each logical node maps to a real Redis instance. Endpoints come from REDIS_NODES
# as comma-separated host:port pairs, one per logical node — works for Docker
# (separate service hosts) and local (several ports on localhost).
REDIS_NODES = os.getenv(
    "REDIS_NODES", "127.0.0.1:6379,127.0.0.1:6380,127.0.0.1:6381"
).split(",")


def _parse_endpoint(ep: str) -> tuple[str, int]:
    host, _, port = ep.strip().rpartition(":")
    return (host or "127.0.0.1"), int(port)


class CacheCluster:
    def __init__(self, node_names: Optional[List[str]] = None, ttl_seconds: float = 30.0):
        names = node_names or DEFAULT_NODES
        self.ring = ConsistentHashRing(names)
        self.backend = "redis"
        self.nodes = {}
        for i, name in enumerate(names):
            host, port = _parse_endpoint(REDIS_NODES[i % len(REDIS_NODES)])
            self.nodes[name] = RedisCacheNode(name, host=host, port=port,
                                              ttl_seconds=ttl_seconds)
        self.ttl = ttl_seconds

    # ----- key normalization --------------------------------------------------

    @staticmethod
    def _key(prefix: str) -> str:
        """Cache key for a prefix. Lowercased+stripped so 'IPH' and 'iph' share
        an entry — same normalization the trie uses."""
        return (prefix or "").strip().lower()

    def owner(self, prefix: str) -> Optional[str]:
        """Which node owns this prefix (consistent-hash decision)."""
        return self.ring.get_node(self._key(prefix))

    # ----- routed operations --------------------------------------------------

    def get(self, prefix: str) -> Optional[Any]:
        key = self._key(prefix)
        node_name = self.ring.get_node(key)
        if node_name is None:
            return None
        return self.nodes[node_name].get(key)

    def set(self, prefix: str, value: Any, ttl: Optional[float] = None) -> str:
        key = self._key(prefix)
        node_name = self.ring.get_node(key)
        if node_name is not None:
            self.nodes[node_name].set(key, value, ttl=ttl)
        return node_name

    def invalidate(self, prefix: str) -> bool:
        key = self._key(prefix)
        node_name = self.ring.get_node(key)
        if node_name is None:
            return False
        return self.nodes[node_name].invalidate(key)

    def invalidate_prefixes(self, prefixes: List[str]) -> int:
        """Invalidate several prefixes (used after a write). Returns how many
        keys were actually present and dropped."""
        return sum(1 for p in prefixes if self.invalidate(p))

    # ----- maintenance & introspection ---------------------------------------

    def sweep_expired(self) -> int:
        # Redis enforces TTL itself, so this is a no-op; kept for interface parity.
        return sum(node.sweep_expired() for node in self.nodes.values())

    def clear(self) -> int:
        return sum(node.clear() for node in self.nodes.values())

    def stats(self) -> dict:
        per_node = [node.stats() for node in self.nodes.values()]
        hits = sum(s["hits"] for s in per_node)
        misses = sum(s["misses"] for s in per_node)
        total = hits + misses
        return {
            "backend": self.backend,
            "nodes": per_node,
            "total_hits": hits,
            "total_misses": misses,
            "overall_hit_rate": round(hits / total, 4) if total else 0.0,
            "vnodes_per_node": self.ring.vnodes,
        }

    def debug(self, prefix: str) -> dict:
        """Everything /cache/debug needs: which node owns the prefix, the ring
        position, and whether the key is currently cached. Uses node.peek() so it
        never mutates hit/miss counters."""
        key = self._key(prefix)
        node_name = self.ring.get_node(key)
        node = self.nodes.get(node_name) if node_name else None
        cached = node.peek(key) if node is not None else False
        return {
            "backend": self.backend,
            "prefix": prefix,
            "normalized_key": key,
            "ring_position": self.ring.position_of(key),
            "owner_node": node_name,
            "status": "hit" if cached else "miss",
            "all_nodes": self.ring.nodes,
        }
