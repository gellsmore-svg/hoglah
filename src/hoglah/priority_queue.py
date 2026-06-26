"""In-process priority queue with per-key serial execution.

A general scheduling primitive: submit callables with a PRIORITY and an optional
serialization KEY.

- Higher-priority work runs first (lower number = higher priority).
- Tasks that share a ``key`` run serially in submission order (FIFO within a key),
  so dependent work for the same key never races; tasks with different keys run
  concurrently up to the worker count.
- A busy key's queued tasks are skipped over until it frees up, so one key's
  backlog never blocks another key's work.

This is intentionally generic (no domain assumptions) so any caller can use it for
session-scoped, resource-scoped, or task-chain-scoped prioritisation. It is
in-process and best-effort; pair it with a durable store for restart safety.
"""

from __future__ import annotations

import heapq
import itertools
import threading
from typing import Any, Callable

# Conventional priority bands (lower = higher priority). Callers may use any ints.
PRIORITY_CRITICAL = 1
PRIORITY_HIGH = 2
PRIORITY_NORMAL = 3
PRIORITY_LOW = 4
PRIORITY_BACKGROUND = 5
PRIORITY_IDLE = 6


class SessionPriorityQueue:
    """Priority queue with per-key (e.g. per-session) serial execution."""

    def __init__(self, workers: int = 2) -> None:
        self._heap: list[tuple] = []
        self._seq = itertools.count()
        self._cond = threading.Condition()
        self._busy: set = set()  # keys with a task currently running
        for index in range(workers):
            threading.Thread(target=self._worker, name=f"hoglah-pq-{index}", daemon=True).start()

    def submit(
        self,
        fn: Callable,
        *args: Any,
        priority: int = PRIORITY_NORMAL,
        key: Any = None,
        **kwargs: Any,
    ) -> None:
        """Schedule ``fn(*args, **kwargs)``; tasks sharing ``key`` run serially."""
        with self._cond:
            heapq.heappush(self._heap, (int(priority), next(self._seq), key, fn, args, kwargs))
            self._cond.notify()

    def _claim(self) -> tuple | None:
        """Pop the highest-priority task whose key is not currently busy."""
        held: list[tuple] = []
        chosen: tuple | None = None
        while self._heap:
            item = heapq.heappop(self._heap)
            key = item[2]
            if key is not None and key in self._busy:
                held.append(item)
                continue
            chosen = item
            break
        for item in held:
            heapq.heappush(self._heap, item)
        if chosen is not None and chosen[2] is not None:
            self._busy.add(chosen[2])
        return chosen

    def _worker(self) -> None:
        while True:
            with self._cond:
                task = self._claim()
                while task is None:
                    self._cond.wait(timeout=1.0)
                    task = self._claim()
            _, _, key, fn, args, kwargs = task
            try:
                fn(*args, **kwargs)
            except Exception:
                pass
            finally:
                with self._cond:
                    if key is not None:
                        self._busy.discard(key)
                    self._cond.notify_all()

    def stats(self) -> dict[str, int]:
        with self._cond:
            return {"queued": len(self._heap), "busy_keys": len(self._busy)}
