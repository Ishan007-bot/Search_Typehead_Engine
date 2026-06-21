"""
Lightweight in-process activity log — a thread-safe ring buffer of recent server
events (cache hits/misses, batch flushes, recency updates/decay snapshots).

It exists purely for OBSERVABILITY / the demo: the UI polls GET /events and renders
a live activity feed so you can *see* the cache, batch writer, and recency ranking
working — instead of inferring it from curl output.

Design:
- Bounded ring buffer (deque maxlen) so memory is capped no matter how long the
  app runs. Old events fall off the back.
- Each event has a monotonically increasing `id`, so the UI can poll "give me
  everything after id N" and never miss or duplicate an event.
- Categories let the UI colour/filter: cache | batch | search | recency.
- A logical clock (incrementing counter) is used for ordering instead of wall time,
  so the log is deterministic and we don't depend on the system clock here.
"""

import threading
from collections import deque
from typing import Optional


class EventLog:
    def __init__(self, maxlen: int = 200):
        self._events: deque[dict] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._next_id = 1

    def emit(self, category: str, message: str, **fields) -> None:
        """Append an event. `category` is one of cache|batch|search|recency.
        Extra keyword fields are attached for the UI (e.g. node, prefix, count)."""
        with self._lock:
            ev = {"id": self._next_id, "category": category, "message": message}
            if fields:
                ev.update(fields)
            self._events.append(ev)
            self._next_id += 1

    def since(self, after_id: int = 0, limit: int = 100) -> list[dict]:
        """Return events with id > after_id (oldest first), capped at `limit`.
        The UI passes the highest id it has seen to get only new events."""
        with self._lock:
            out = [e for e in self._events if e["id"] > after_id]
        return out[-limit:]

    def latest_id(self) -> int:
        with self._lock:
            return self._next_id - 1

    def clear(self) -> None:
        with self._lock:
            self._events.clear()


# Module-level singleton the rest of the app emits to.
event_log = EventLog()
