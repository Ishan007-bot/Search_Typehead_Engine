"""
Consistent hashing ring — decides which cache node owns a given prefix key.

THE PROBLEM IT SOLVES (the #1 viva question):
    The naive way to shard keys across N cache nodes is `hash(key) % N`.
    That works until N changes. Add or remove ONE node and N changes, so
    `hash(key) % N` changes for almost EVERY key -> nearly the whole cache is
    suddenly "owned" by a different node -> mass cache misses -> every request
    stampedes the primary data store at once. Bad.

THE FIX:
    Place nodes on a fixed circular keyspace (the "ring", 0 .. 2^32-1). To find
    the owner of a key, hash the key to a point on the ring and walk CLOCKWISE
    to the first node you meet. Now adding/removing a node only re-homes the
    keys in ONE arc of the ring (~1/N of keys), not all of them.

VIRTUAL NODES (vnodes):
    With few physical nodes, random placement leaves big uneven gaps, so one
    node ends up owning far more of the ring than another. Fix: place each
    physical node at MANY points on the ring (replicas / vnodes). More points
    -> the arcs even out -> balanced load. We use 150 vnodes per node, a common
    default (it's what Ketama / libketama uses).

We hash with md5 (stable across processes & runs, unlike Python's salted hash()).
"""

import bisect
import hashlib
from typing import List, Optional


def _hash(key: str) -> int:
    """Map a string to a point on the 32-bit ring. Stable across runs."""
    digest = hashlib.md5(key.encode("utf-8")).digest()
    # take first 4 bytes -> 32-bit unsigned int
    return int.from_bytes(digest[:4], "big")


class ConsistentHashRing:
    def __init__(self, nodes: Optional[List[str]] = None, vnodes: int = 150):
        self.vnodes = vnodes
        # Sorted list of ring positions, and a parallel map position -> node name.
        self._ring: List[int] = []          # sorted vnode positions
        self._pos_to_node: dict[int, str] = {}
        self._nodes: set[str] = set()
        for n in (nodes or []):
            self.add_node(n)

    # ----- membership ---------------------------------------------------------

    def add_node(self, node: str) -> None:
        """Add a physical node by placing `vnodes` replicas on the ring."""
        if node in self._nodes:
            return
        self._nodes.add(node)
        for i in range(self.vnodes):
            pos = _hash(f"{node}#{i}")
            # On the rare collision, nudge until free (keeps the ring a bijection).
            while pos in self._pos_to_node:
                pos = (pos + 1) % (1 << 32)
            self._pos_to_node[pos] = node
            bisect.insort(self._ring, pos)

    def remove_node(self, node: str) -> None:
        """Remove a physical node and all its vnodes. Only this node's arcs
        re-home; every other key keeps its owner."""
        if node not in self._nodes:
            return
        self._nodes.discard(node)
        positions = [p for p, n in self._pos_to_node.items() if n == node]
        for p in positions:
            del self._pos_to_node[p]
            idx = bisect.bisect_left(self._ring, p)
            if idx < len(self._ring) and self._ring[idx] == p:
                self._ring.pop(idx)

    @property
    def nodes(self) -> List[str]:
        return sorted(self._nodes)

    # ----- lookup (the hot path) ----------------------------------------------

    def get_node(self, key: str) -> Optional[str]:
        """Return the node that owns `key`: hash the key, walk clockwise to the
        first vnode at or after that position (wrapping past the end)."""
        if not self._ring:
            return None
        pos = _hash(key)
        idx = bisect.bisect_right(self._ring, pos)
        if idx == len(self._ring):   # wrapped past the last vnode -> first one
            idx = 0
        return self._pos_to_node[self._ring[idx]]

    # ----- introspection (for /cache/debug and the perf report) --------------

    def distribution(self, keys: List[str]) -> dict[str, int]:
        """Count how many of `keys` land on each node — used to demonstrate that
        consistent hashing spreads load roughly evenly."""
        counts = {n: 0 for n in self._nodes}
        for k in keys:
            owner = self.get_node(k)
            if owner is not None:
                counts[owner] += 1
        return counts

    def position_of(self, key: str) -> int:
        """The ring position a key hashes to (for debug output)."""
        return _hash(key)
