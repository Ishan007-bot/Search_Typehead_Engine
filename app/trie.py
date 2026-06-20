"""
In-memory prefix index (Trie) for fast typeahead suggestions.

Why a Trie (viva talking point):
- A SQL `LIKE 'prefix%'` works but hits the DB on every keystroke. The whole
  point of typeahead is sub-millisecond reads, so we hold the index in RAM.
- A Trie maps a prefix to its matching queries in O(len(prefix)) to reach the
  node, then we return the node's precomputed top-K list.
- KEY OPTIMIZATION: each trie node caches its own top-K (by count) suggestions.
  So answering /suggest is: walk to the prefix node, return its cached top-K.
  No scanning of the whole subtree at request time.

Trade-off:
- Precomputing top-K per node costs memory and makes inserts a little heavier
  (we re-evaluate top-K along the inserted path). For a read-heavy typeahead
  workload that's exactly the trade we want: pay at write time, win at read time.
- This trie holds COUNT only (the basic 60% ranking). Recency-aware ranking
  (M5) re-scores candidates on top of the trie's count-sorted candidates.

Thread-safety:
- Reads are lock-free (dict walks). Rebuilds/inserts take a lock. Suggestions
  are served from the cache layer (M4) most of the time anyway.
"""

import threading
from typing import List, Optional, Tuple

TOP_K = 10  # we keep up to this many suggestions cached per node


class TrieNode:
    __slots__ = ("children", "is_word", "count", "top")

    def __init__(self) -> None:
        self.children: dict[str, "TrieNode"] = {}
        self.is_word: bool = False
        self.count: int = 0          # count if this node terminates a query
        # cached top-K (query, count) for this node's subtree, sorted desc by count
        self.top: List[Tuple[str, int]] = []


class Trie:
    def __init__(self, top_k: int = TOP_K):
        self.root = TrieNode()
        self.top_k = top_k
        self._lock = threading.Lock()
        self._size = 0

    def __len__(self) -> int:
        return self._size

    # ----- building -----------------------------------------------------------

    def insert(self, query: str, count: int) -> None:
        """Insert or update a query with an absolute count value."""
        query = (query or "").strip().lower()
        if not query:
            return
        with self._lock:
            self._insert_locked(query, count)

    def _insert_locked(self, query: str, count: int) -> None:
        # Walk/extend the path, collecting nodes so we can refresh their top-K.
        path: List[TrieNode] = [self.root]
        node = self.root
        for ch in query:
            nxt = node.children.get(ch)
            if nxt is None:
                nxt = TrieNode()
                node.children[ch] = nxt
            node = nxt
            path.append(node)

        if not node.is_word:
            self._size += 1
        node.is_word = True
        node.count = count

        # Refresh the cached top-K for every node along the path (root..leaf),
        # since this query is a candidate for all of its prefixes.
        for n in path:
            self._update_top(n, query, count)

    def _update_top(self, node: TrieNode, query: str, count: int) -> None:
        """Insert (query, count) into node.top, keep it sorted & capped at top_k."""
        # Remove any existing entry for this query (count may have changed).
        node.top = [(q, c) for (q, c) in node.top if q != query]
        node.top.append((query, count))
        node.top.sort(key=lambda qc: qc[1], reverse=True)
        if len(node.top) > self.top_k:
            node.top = node.top[: self.top_k]

    def bump(self, query: str, delta: int = 1) -> int:
        """Increment a query's count by `delta` (insert it if new). Returns the
        new absolute count. Refreshes cached top-K along the path so suggestions
        reflect the update immediately. Used by the search-record path (M3) and
        by the batch writer's in-memory sync (M6)."""
        query = (query or "").strip().lower()
        if not query:
            return 0
        with self._lock:
            node = self._find_node(query)
            current = node.count if (node is not None and node.is_word) else 0
            new_count = current + delta
            self._insert_locked(query, new_count)
            return new_count

    def build_from_rows(self, rows) -> None:
        """Bulk-build from an iterable of objects/tuples with query & count.

        Accepts sqlite3.Row (row['query'], row['count']) or (query, count) tuples.
        """
        with self._lock:
            self.root = TrieNode()
            self._size = 0
            for row in rows:
                if isinstance(row, tuple):
                    q, c = row[0], row[1]
                else:
                    q, c = row["query"], row["count"]
                self._insert_locked(q, int(c))

    # ----- reading (hot path) -------------------------------------------------

    def _find_node(self, prefix: str) -> Optional[TrieNode]:
        node = self.root
        for ch in prefix:
            node = node.children.get(ch)
            if node is None:
                return None
        return node

    def suggest(self, prefix: str, limit: int = TOP_K) -> List[Tuple[str, int]]:
        """Return up to `limit` (query, count) suggestions for `prefix`.

        - Case-insensitive (prefix is lowercased).
        - Empty/whitespace prefix -> [] (UI shows trending instead).
        - No match -> [].
        """
        prefix = (prefix or "").strip().lower()
        if not prefix:
            return []
        node = self._find_node(prefix)
        if node is None:
            return []
        return node.top[:limit]
