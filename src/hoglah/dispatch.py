"""Multi-backend dispatch — spread jobs across several Ollama backends.

Hoglah stays the single front end the callers submit to and still serialises work;
this just lets one worker fan its in-flight jobs across more than one Ollama server
(e.g. two GPUs, or a local box + a bigger remote one) instead of a single host.

Dispatch is **least-loaded**: each job goes to the backend currently running the
fewest jobs (ties broken by order). That balances naturally when jobs vary in
length, and degrades to round-robin when all backends are level. The worker runs in
a single asyncio event loop, so the in-flight counters need no locking — they are
only mutated synchronously around each `await`.

Per-model affinity (prefer a backend that already has the model loaded, to avoid a
multi-GB reload) is a deliberate future refinement; v1 balances by load alone.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any


class BackendPool:
    """Least-loaded dispatch across a list of execution adapters (one per backend)."""

    def __init__(self, adapters: list[Any]) -> None:
        if not adapters:
            raise ValueError("BackendPool needs at least one adapter")
        self._adapters = list(adapters)
        self._inflight = [0] * len(self._adapters)

    def __len__(self) -> int:
        return len(self._adapters)

    @property
    def hosts(self) -> list[str | None]:
        return [getattr(a, "host", None) for a in self._adapters]

    def loads(self) -> list[int]:
        """Current in-flight count per backend (for diagnostics)."""
        return list(self._inflight)

    def _pick(self) -> int:
        # Fewest in-flight wins; ties resolve to the lowest index (stable).
        return min(range(len(self._adapters)), key=lambda k: (self._inflight[k], k))

    @asynccontextmanager
    async def lease(self):
        """Lease the least-loaded backend for one job; counts it in-flight until done."""
        idx = self._pick()
        self._inflight[idx] += 1
        try:
            yield self._adapters[idx]
        finally:
            self._inflight[idx] -= 1
