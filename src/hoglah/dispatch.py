"""Multi-backend dispatch — spread jobs across several Ollama backends.

Hoglah stays the single front end the callers submit to and still serialises work;
this lets one worker fan its in-flight jobs across more than one Ollama server (e.g.
two GPUs, or a local box + a bigger remote one) instead of a single host.

Dispatch is **warm-affinity, then least-loaded**:

  1. Loading a model into a GPU is slow and heavy (gigabytes); once loaded it stays
     "warm" and subsequent calls are fast. So a job for model M first prefers a
     backend that has *recently run M* (likely still warm) — avoiding a reload.
  2. Among those (or all backends, if none is warm for M) it picks the **least
     loaded** — fewest jobs in flight — to balance.

"Warm" is tracked cheaply by recency (the last few distinct models each backend
served), with no extra calls to Ollama; it degrades gracefully if a model was
actually evicted (worst case: a reload, same as before). The worker runs in a single
asyncio event loop, so the counters/recency need no locking.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any


def _model_name(entry: Any) -> str | None:
    if isinstance(entry, dict):
        return entry.get("model") or entry.get("name")
    return getattr(entry, "model", None) or getattr(entry, "name", None)


class BackendPool:
    """Warm-affinity, least-loaded dispatch across execution adapters (one per backend)."""

    def __init__(self, adapters: list[Any], warm_capacity: int = 2) -> None:
        if not adapters:
            raise ValueError("BackendPool needs at least one adapter")
        self._adapters = list(adapters)
        self._inflight = [0] * len(self._adapters)
        # Per-backend recency of served models (most-recent first); approximates which
        # models are still warm. maxlen ~ how many models a backend keeps loaded.
        self._recent: list[deque[str]] = [deque(maxlen=max(1, warm_capacity)) for _ in self._adapters]
        # Short per-model failure cooldown. Immediate retries avoid a backend that
        # just failed, but recovered backends automatically re-enter after 30 seconds.
        self._failed_until: dict[tuple[str, int], float] = {}

    def __len__(self) -> int:
        return len(self._adapters)

    @property
    def hosts(self) -> list[str | None]:
        return [getattr(a, "host", None) for a in self._adapters]

    def loads(self) -> list[int]:
        return list(self._inflight)

    def warm(self) -> list[list[str]]:
        """Per-backend recently-served (likely warm) models — for diagnostics."""
        return [list(d) for d in self._recent]

    def _note(self, idx: int, model: str) -> None:
        d = self._recent[idx]
        if model in d:
            d.remove(model)
        d.appendleft(model)  # maxlen evicts the oldest (rightmost)

    def _pick(self, model: str | None) -> int:
        available = list(range(len(self._adapters)))
        if model:
            now = time.monotonic()
            candidates = [
                i for i in available
                if self._failed_until.get((model, i), 0.0) <= now
            ]
            if not candidates:
                candidates = available
        else:
            candidates = available
        warm = [i for i in candidates if model and model in self._recent[i]]
        return min(warm or candidates, key=lambda i: (self._inflight[i], i))

    @asynccontextmanager
    async def lease(self, model: str | None = None):
        """Lease a backend for one job: prefer one already warm for `model`, else the
        least loaded. Counts the job in-flight and records the model as warm there."""
        idx = self._pick(model)
        self._inflight[idx] += 1
        try:
            yield self._adapters[idx]
        except Exception:
            if model:
                self._failed_until[(model, idx)] = time.monotonic() + 30.0
            raise
        else:
            if model:
                self._failed_until.pop((model, idx), None)
                self._note(idx, model)
        finally:
            self._inflight[idx] -= 1

    async def available_models(self) -> list[str]:
        """Deduped, sorted superset of the models available across all backends.
        A backend that errors (e.g. unreachable) contributes nothing rather than
        failing the whole call."""
        async def _safe(adapter: Any) -> set[str]:
            try:
                entries = await adapter.list_models()
            except Exception:
                return set()
            return {n for n in (_model_name(e) for e in (entries or [])) if n}

        sets = await asyncio.gather(*[_safe(a) for a in self._adapters])
        union: set[str] = set().union(*sets) if sets else set()
        return sorted(union)
