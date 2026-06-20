"""
Distributed cache cluster = N CacheNodes + a ConsistentHashRing in front.

This is the "distributed cache" the assignment asks for. The nodes are separate
logical CacheNode objects (each its own store + stats), exactly as N Redis
instances would be — but in-process, so the project runs with one command and
every routing decision is visible, inspectable code (ideal for the viva).

Routing: for any prefix key, the ring picks the owning node. The same key always
maps to the same node (until membership changes), so reads and writes for a key
go to one place — that's what makes a hit possible.
"""

from typing import Any, List, Optional

from app.cache import CacheNode
from app.ring import ConsistentHashRing

DEFAULT_NODES = ["cache-node-0", "cache-node-1", "cache-node-2"]


class CacheCluster:
    def __init__(self, node_names: Optional[List[str]] = None, ttl_seconds: float = 30.0):
        names = node_names or DEFAULT_NODES
        self.ring = ConsistentHashRing(names)
        self.nodes: dict[str, CacheNode] = {
            n: CacheNode(n, ttl_seconds=ttl_seconds) for n in names
        }
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
        return sum(node.sweep_expired() for node in self.nodes.values())

    def clear(self) -> int:
        return sum(node.clear() for node in self.nodes.values())

    def stats(self) -> dict:
        per_node = [node.stats() for node in self.nodes.values()]
        hits = sum(s["hits"] for s in per_node)
        misses = sum(s["misses"] for s in per_node)
        total = hits + misses
        return {
            "nodes": per_node,
            "total_hits": hits,
            "total_misses": misses,
            "overall_hit_rate": round(hits / total, 4) if total else 0.0,
            "vnodes_per_node": self.ring.vnodes,
        }

    def debug(self, prefix: str) -> dict:
        """Everything /cache/debug needs: which node owns the prefix, the ring
        position, and whether the key is currently cached (a hit if requested now)."""
        key = self._key(prefix)
        node_name = self.ring.get_node(key)
        node = self.nodes.get(node_name) if node_name else None
        # Peek WITHOUT touching hit/miss counters.
        cached = False
        if node is not None:
            with node._lock:
                import time
                entry = node._store.get(key)
                cached = entry is not None and time.time() < entry[0]
        return {
            "prefix": prefix,
            "normalized_key": key,
            "ring_position": self.ring.position_of(key),
            "owner_node": node_name,
            "status": "hit" if cached else "miss",
            "all_nodes": self.ring.nodes,
        }
