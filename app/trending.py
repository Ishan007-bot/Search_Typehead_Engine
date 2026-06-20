"""
Recency-aware ranking + trending searches (assignment section 7).

This module answers the five things section 7 says we must explain:

1) HOW RECENT SEARCHES ARE TRACKED
   Per query we keep time-bucketed hit counters. Time is chopped into fixed
   buckets of `bucket_seconds`. A search increments the counter for the CURRENT
   bucket for that query. We retain only the last `window_buckets` buckets per
   query (a sliding window); older buckets are dropped on access. This is O(1)
   per search and bounded in memory.

2) HOW RECENT ACTIVITY AFFECTS RANKING
   The enhanced score blends stable popularity with decayed recent activity:

       score(q) = log10(1 + all_time_count(q))            # historical, compressed
                + BOOST * recent_score(q)                  # recent, time-decayed

   recent_score sums the windowed buckets, weighting newer buckets more via an
   exponential decay: a hit `k` buckets ago counts as decay**k (0<decay<1).
   log10 on the historical term stops a single 100k query from permanently
   dominating — it compresses the popularity range so recency can actually move
   things. BOOST tunes how aggressively recency reorders results.

3) HOW WE AVOID PERMANENTLY OVER-RANKING A SHORT-LIVED SPIKE
   recent_score is computed ONLY from buckets inside the sliding window, with
   decay. Once a query stops being searched, its buckets age past the window and
   its decayed weight falls to ~0 within `window_buckets` ticks. So a spike adds
   a temporary boost that *automatically fades* — afterwards the query falls back
   to its historical (log popularity) rank. The boost cannot become permanent
   because nothing keeps the recent buckets alive except continued searching.

4) HOW THE CACHE IS UPDATED/INVALIDATED WHEN RANKINGS CHANGE  (handled in main.py)
   - Recency scores drift every bucket tick even with no writes, so enhanced-mode
     suggestion cache entries use a SHORT TTL (~ one bucket) vs basic mode's long TTL.
   - On a search write, affected prefixes are invalidated in BOTH cache namespaces.

5) TRADE-OFFS  (documented in ARCHITECTURE.md)
   freshness vs latency vs complexity — shorter TTL => fresher but more misses
   (higher latency); blended scoring is more complex than a raw count sort.

NOTE ON TIME: time.time() is read live here (this is runtime state, not a
workflow script), so the decay reflects real elapsed wall-clock.
"""

import math
import threading
import time
from collections import defaultdict
from typing import Dict, List, Tuple


class TrendingTracker:
    def __init__(
        self,
        bucket_seconds: float = 60.0,
        window_buckets: int = 10,
        decay: float = 0.8,
        boost: float = 3.0,
    ):
        self.bucket_seconds = bucket_seconds
        self.window_buckets = window_buckets
        self.decay = decay          # 0<decay<1; lower = recent activity matters more sharply
        self.boost = boost          # how strongly recency reorders vs historical popularity
        # query -> { bucket_index : hits_in_that_bucket }
        self._buckets: Dict[str, Dict[int, int]] = defaultdict(dict)
        self._lock = threading.Lock()

    # ----- time helpers -------------------------------------------------------

    def _now_bucket(self) -> int:
        return int(time.time() // self.bucket_seconds)

    # ----- tracking (called on every search) ----------------------------------

    def record(self, query: str, amount: int = 1) -> None:
        """Register `amount` searches for `query` in the current time bucket."""
        query = (query or "").strip().lower()
        if not query:
            return
        b = self._now_bucket()
        with self._lock:
            buckets = self._buckets[query]
            buckets[b] = buckets.get(b, 0) + amount
            self._prune_locked(query, b)

    def _prune_locked(self, query: str, now_bucket: int) -> None:
        """Drop buckets older than the sliding window for one query."""
        cutoff = now_bucket - self.window_buckets + 1
        buckets = self._buckets[query]
        for b in [b for b in buckets if b < cutoff]:
            del buckets[b]
        if not buckets:
            self._buckets.pop(query, None)

    # ----- scoring ------------------------------------------------------------

    def recent_score(self, query: str) -> float:
        """Time-decayed sum of windowed recent hits for `query`.

        A hit in the current bucket counts fully (decay**0 = 1); a hit k buckets
        ago counts decay**k. Buckets outside the window count 0.
        """
        query = (query or "").strip().lower()
        now_b = self._now_bucket()
        cutoff = now_b - self.window_buckets + 1
        with self._lock:
            buckets = self._buckets.get(query)
            if not buckets:
                return 0.0
            total = 0.0
            for b, hits in buckets.items():
                if b < cutoff:
                    continue
                age = now_b - b           # 0 = current bucket
                total += hits * (self.decay ** age)
            return total

    def blended_score(self, query: str, all_time_count: int) -> float:
        """Enhanced ranking score = compressed historical popularity + decayed recency."""
        historical = math.log10(1 + max(0, all_time_count))
        return historical + self.boost * self.recent_score(query)

    # ----- trending list ------------------------------------------------------

    def trending(self, limit: int = 10) -> List[Tuple[str, float]]:
        """Top queries by recent_score (pure recency — what's hot right now),
        independent of all-time popularity. Returns [(query, recent_score), ...]."""
        now_b = self._now_bucket()
        cutoff = now_b - self.window_buckets + 1
        scored: List[Tuple[str, float]] = []
        with self._lock:
            for query, buckets in self._buckets.items():
                s = 0.0
                for b, hits in buckets.items():
                    if b < cutoff:
                        continue
                    s += hits * (self.decay ** (now_b - b))
                if s > 0:
                    scored.append((query, s))
        scored.sort(key=lambda qs: qs[1], reverse=True)
        return scored[:limit]

    def stats(self) -> dict:
        with self._lock:
            return {
                "tracked_queries": len(self._buckets),
                "bucket_seconds": self.bucket_seconds,
                "window_buckets": self.window_buckets,
                "decay": self.decay,
                "boost": self.boost,
            }
