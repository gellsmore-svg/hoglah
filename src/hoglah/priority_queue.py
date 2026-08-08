"""In-process priority queue with per-key serial execution.

A general scheduling primitive: submit callables with a PRIORITY and an optional
serialization KEY.

- Higher-priority work runs first (lower number = higher priority).
- Tasks that share a ``key`` run **serially** (never concurrent for that key).
  Among tasks for the same key, order is ``(priority, submission_seq)`` — so a
  later higher-priority task may run before an earlier lower-priority one.
  Same-priority tasks for a key are FIFO. Callers that need strict submission
  order for a chain should use the same priority (or a single sequential submit
  after the previous task finishes).
- A busy key's queued tasks are skipped over until it frees up, so one key's
  backlog never blocks another key's work.

This is intentionally generic (no domain assumptions) so any caller can use it for
session-scoped, resource-scoped, or task-chain-scoped prioritisation. It is
in-process and best-effort; pair it with a durable store for restart safety.

Call ``close()`` / ``shutdown()`` when finished so worker threads exit (review M8).
"""

from __future__ import annotations

import logging

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


logger = logging.getLogger("hoglah")

class SessionPriorityQueue:
    """Priority queue with per-key (e.g. per-session) serial execution."""

    def __init__(self, workers: int = 2) -> None:
        self._heap: list[tuple] = []
        self._seq = itertools.count()
        self._cond = threading.Condition()
        self._busy: set = set()  # keys with a task currently running
        self._failures = 0  # tasks that raised (for stats / monitoring)
        self._closed = False
        self._workers: list[threading.Thread] = []
        for index in range(workers):
            t = threading.Thread(
                target=self._worker, name=f"hoglah-pq-{index}", daemon=True
            )
            t.start()
            self._workers.append(t)

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
            if self._closed:
                raise RuntimeError("SessionPriorityQueue is closed")
            heapq.heappush(self._heap, (int(priority), next(self._seq), key, fn, args, kwargs))
            self._cond.notify()

    def close(self, *, wait: bool = True, timeout: float | None = 3.0) -> None:
        """Stop accepting work and ask workers to exit.

        ``wait=True`` joins worker threads (up to ``timeout`` seconds each).
        Daemon workers still exit with the process if join times out.
        """
        with self._cond:
            self._closed = True
            self._cond.notify_all()
        if wait:
            for t in self._workers:
                t.join(timeout=timeout)

    # Alias used by some callers / docs.
    shutdown = close

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
                    if self._closed and not self._heap:
                        return
                    self._cond.wait(timeout=1.0)
                    task = self._claim()
                    if task is None and self._closed and not self._heap:
                        return
            _, _, key, fn, args, kwargs = task
            try:
                fn(*args, **kwargs)
            except Exception:
                with self._cond:
                    self._failures += 1
                logger.exception(
                    "SessionPriorityQueue task failed (key=%s, fn=%s)",
                    key, getattr(fn, "__name__", fn),
                )
            finally:
                with self._cond:
                    if key is not None:
                        self._busy.discard(key)
                    self._cond.notify_all()

    def stats(self) -> dict[str, int]:
        with self._cond:
            return {
                "queued": len(self._heap),
                "busy_keys": len(self._busy),
                "failures": self._failures,
            }
