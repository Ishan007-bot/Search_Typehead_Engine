"""
Batch writer for search-count updates (assignment section 8).

THE PROBLEM:
    Writing to SQLite synchronously on every /search means one DB transaction
    per keystroke-submit. Under load that's a flood of tiny writes — the primary
    store becomes the bottleneck and most of those writes touch the SAME few
    popular queries anyway.

THE APPROACH (buffer -> aggregate -> flush):
    1. BUFFER:    each /search just adds to an in-memory dict. No DB write.
    2. AGGREGATE: repeated queries collapse — 8x "iphone" becomes {"iphone": 8},
                  i.e. ONE row touched instead of 8 writes.
    3. FLUSH:     the buffer is written to SQLite in a SINGLE transaction when
                  EITHER trigger fires (whichever comes first):
                    - size:  the buffer holds >= batch_size distinct queries, OR
                    - timer: flush_interval seconds have elapsed.

WRITE-REDUCTION EVIDENCE:
    We count `searches_enqueued` (logical writes requested) and `db_flushes`
    (actual DB transactions). 1000 searches over ~100 distinct queries might be
    ~5 flushes => a ~200x reduction in transactions. /batch/stats exposes this.

FAILURE TRADE-OFF (must be discussed):
    Buffered counts live in RAM. If the process crashes between flushes, that
    batch is LOST — counts undercount slightly until traffic re-accumulates.
    Mitigations: (a) flush on graceful shutdown (done here); (b) for stronger
    durability, append each search to a write-ahead log first and replay it on
    restart (documented, not implemented — the assignment asks us to DISCUSS the
    trade-off, and a WAL would re-introduce a per-write cost we're trying to avoid).
    For a typeahead, slightly-stale popularity counts are an acceptable loss; this
    is a deliberate latency/throughput vs durability trade.
"""

import threading
import time
from typing import Callable, Dict


class BatchWriter:
    def __init__(
        self,
        flush_fn: Callable[[Dict[str, int]], int],
        batch_size: int = 50,
        flush_interval: float = 2.0,
    ):
        """
        flush_fn:       called with an aggregated {query: total_delta} dict; should
                        persist it in one transaction and return rows written.
        batch_size:     flush when this many DISTINCT queries are buffered.
        flush_interval: flush at least this often (seconds), even if not full.
        """
        self.flush_fn = flush_fn
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self._buffer: Dict[str, int] = {}
        self._lock = threading.Lock()

        # Metrics (the write-reduction evidence).
        self.searches_enqueued = 0     # total logical writes requested
        self.db_flushes = 0            # actual DB transactions performed
        self.rows_written = 0          # total rows touched across all flushes
        self.last_flush_at = time.time()
        self.size_flushes = 0          # flushes triggered by buffer size
        self.timer_flushes = 0         # flushes triggered by the interval timer

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ----- enqueue (the hot path; replaces the synchronous DB write) ----------

    def enqueue(self, query: str, amount: int = 1) -> None:
        """Buffer one search. Aggregates with any pending count for the same
        query. Triggers a size-flush inline if the buffer is now full."""
        query = (query or "").strip().lower()
        if not query:
            return
        flush_now = False
        with self._lock:
            self._buffer[query] = self._buffer.get(query, 0) + amount
            self.searches_enqueued += amount
            if len(self._buffer) >= self.batch_size:
                flush_now = True
        if flush_now:
            self._flush(trigger="size")

    # ----- flush --------------------------------------------------------------

    def _flush(self, trigger: str) -> int:
        """Swap out the buffer under the lock, then write it OUTSIDE the lock so
        enqueues aren't blocked during the DB transaction. Returns rows written."""
        with self._lock:
            if not self._buffer:
                self.last_flush_at = time.time()
                return 0
            batch = self._buffer
            self._buffer = {}

        rows = self.flush_fn(batch)   # one transaction for the whole batch

        with self._lock:
            self.db_flushes += 1
            self.rows_written += rows
            self.last_flush_at = time.time()
            if trigger == "size":
                self.size_flushes += 1
            else:
                self.timer_flushes += 1
        return rows

    def flush_now(self) -> int:
        """Force a flush (used on shutdown and in tests)."""
        return self._flush(trigger="manual")

    # ----- background timer loop ----------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="batch-writer", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.wait(self.flush_interval):
            self._flush(trigger="timer")

    def stop(self) -> None:
        """Stop the timer and flush whatever remains (graceful shutdown)."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.flush_interval + 1)
            self._thread = None
        self.flush_now()   # don't lose the last partial batch

    # ----- introspection ------------------------------------------------------

    def stats(self) -> dict:
        with self._lock:
            pending = len(self._buffer)
            pending_total = sum(self._buffer.values())
        # How many DB writes we AVOIDED: every enqueue would have been its own
        # write synchronously; instead we did db_flushes transactions.
        naive_writes = self.searches_enqueued
        actual_writes = self.db_flushes
        reduction = (
            round(1 - actual_writes / naive_writes, 4) if naive_writes else 0.0
        )
        return {
            "searches_enqueued": self.searches_enqueued,
            "db_flushes": actual_writes,
            "rows_written": self.rows_written,
            "pending_in_buffer": pending,
            "pending_searches_unflushed": pending_total,
            "size_flushes": self.size_flushes,
            "timer_flushes": self.timer_flushes,
            "batch_size": self.batch_size,
            "flush_interval_seconds": self.flush_interval,
            "naive_synchronous_writes": naive_writes,
            "actual_db_transactions": actual_writes,
            "write_reduction_ratio": reduction,   # e.g. 0.99 => 99% fewer writes
        }
