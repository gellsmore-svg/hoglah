"""Hoglah client — synchronous public facade (ADR-012).

The public API is intentionally synchronous and simple:

    h = Hoglah()  # or Hoglah(config=..., callbacks={"key": my_func})
    job_id = h.submit(prompt=..., model="gemma:7b", callback=..., tags=...)
    result = h.wait(job_id, timeout=120)

Internally:
- Uses a pluggable JobStore (SQLite by default).
- Full request is persisted so a future worker can execute it.
- Supports both direct callables (current-process only) and named callback_key
  (durable across restarts when the registry is re-supplied) per ADR-006.
- No execution happens yet — this chunk only does enqueue + query.

The background asyncio worker + Ollama adapter will be added in the next chunk.
"""

from __future__ import annotations

import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from .config import HoglahConfig, HoglahSettings
from .models import (
    JobCallback,
    JobRequest,
    JobResult,
    JobStatus,
    new_job_id,
    normalize_request,
)
from .store import JobStore, SQLiteJobStore, create_sqlite_store


class Hoglah:
    """Main client for submitting and managing Ollama jobs.

    Example:
        h = Hoglah()
        job_id = h.submit(
            prompt="Explain Hoglah...",
            model="gemma3:1b",
            tags=["research"],
            callback=my_callback,           # direct (lives only while this process runs)
            # or
            # callback_key="my_handler",    # looked up in callbacks= registry
        )
        print(h.status(job_id))
        result = h.wait(job_id)
    """

    def __init__(
        self,
        config: HoglahConfig | dict[str, Any] | None = None,
        *,
        callbacks: dict[str, JobCallback] | None = None,
        store: JobStore | None = None,
        **overrides: Any,
    ) -> None:
        # Build settings (Pydantic handles env + defaults + overrides)
        if isinstance(config, dict):
            config = HoglahSettings(**config, **overrides)
        elif config is None:
            config = HoglahSettings(**overrides)
        elif overrides:
            # allow overriding individual fields
            config = HoglahSettings.model_copy(
                update=overrides, deep=True
            ) if hasattr(config, "model_copy") else HoglahSettings(**{**config.model_dump(), **overrides})

        self.config: HoglahSettings = config  # type: ignore[assignment]
        self.config.ensure_dirs()

        # Callbacks registry for named/durable callbacks (ADR-006)
        self._callbacks: dict[str, JobCallback] = callbacks or {}

        # In-memory direct callbacks (only for jobs submitted in *this* process lifetime)
        self._direct_callbacks: dict[str, JobCallback] = {}

        # Store (pluggable)
        if store is not None:
            self._store: JobStore = store
        else:
            self._store = create_sqlite_store(self.config.db_path)

        # Attempt restart callback re-delivery for jobs that completed while we were down
        self._redeliver_restart_callbacks()

    # ------------------------------------------------------------------ #
    # Public API (matches the spirit of the requirements submit signature)
    # ------------------------------------------------------------------ #

    def submit(
        self,
        *,
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        model: str,
        system_prompt: str | None = None,
        num_ctx: int | None = None,
        options: dict[str, Any] | None = None,
        callback: JobCallback | str | None = None,
        # callback_url is V2 per non-goals
        tags: list[str] | None = None,
        priority: int = 0,
        timeout_seconds: int | None = None,
        max_retries: int = 2,
        metadata: dict[str, Any] | None = None,
        parent_job_id: str | None = None,
        # Generation params
        temperature: float | None = None,
        top_p: float | None = None,
        top_k: int | None = None,
        repeat_penalty: float | None = None,
        seed: int | None = None,
        stop: list[str] | None = None,
        num_predict: int | None = None,
        format: str | None = None,
        keep_alive: str | int | None = None,
        **extra: Any,  # future-proof passthrough into options or ignored
    ) -> str:
        """Submit a job. Returns the job ID immediately.

        The full request is persisted. Execution happens later when a worker
        picks it up (not implemented in this chunk).
        """
        if not model:
            raise ValueError("model is required")

        # Handle callback (direct callable vs named key)
        callback_key: str | None = None
        direct_cb: JobCallback | None = None

        if callback is not None:
            if callable(callback):
                direct_cb = callback
            elif isinstance(callback, str):
                callback_key = callback
            else:
                raise TypeError("callback must be a callable or a string key")

        # Build the persistable request
        req = normalize_request(
            prompt=prompt,
            messages=messages,
            model=model,
            system_prompt=system_prompt,
            num_ctx=num_ctx,
            options=options,
            tags=tags,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            metadata=metadata,
            parent_job_id=parent_job_id,
            temperature=temperature,
            top_p=top_p,
            top_k=top_k,
            repeat_penalty=repeat_penalty,
            seed=seed,
            stop=stop,
            num_predict=num_predict,
            format=format,
            keep_alive=keep_alive,
            callback_key=callback_key,
        )

        # Enqueue (store the request)
        job_id = self._store.enqueue(req, callback_key=callback_key)

        # Remember direct callback for this process (if any)
        if direct_cb is not None:
            self._direct_callbacks[job_id] = direct_cb

        return job_id

    def get(self, job_id: str) -> JobResult:
        """Return a JobResult for the job (works for any status).

        For terminal jobs we reconstruct from the stored result when available.
        """
        row = self._store.get(job_id)
        if row is None:
            raise KeyError(f"Job not found: {job_id}")

        status = JobStatus(row["status"])

        if row.get("result"):
            # Terminal job with stored result
            res = row["result"]
            return JobResult(
                job_id=job_id,
                status=status,
                output=res.get("output"),
                model=res.get("model"),
                parameters=res.get("parameters", {}),
                usage=res.get("usage", {}),
                timings={k: self._parse_dt(v) for k, v in (res.get("timings") or {}).items()},
                error=res.get("error"),
                tags=res.get("tags", []),
                metadata=res.get("metadata", {}),
                parent_job_id=res.get("parent_job_id"),
                truncated=res.get("truncated", False),
                truncation_reason=res.get("truncation_reason"),
                estimated_prompt_tokens=res.get("estimated_prompt_tokens"),
                effective_num_ctx=res.get("effective_num_ctx"),
            )

        # Non-terminal or no result yet: synthesize a minimal JobResult
        req = row.get("request", {})
        return JobResult(
            job_id=job_id,
            status=status,
            model=req.get("model"),
            parameters={k: v for k, v in req.items() if k not in ("prompt", "messages")},
            tags=req.get("tags") or [],
            metadata=req.get("metadata") or {},
            parent_job_id=req.get("parent_job_id"),
            # timings can be enriched later
        )

    def list(
        self,
        *,
        status: JobStatus | str | None = None,
        tags: list[str] | None = None,
        limit: int = 100,
    ) -> list[JobResult]:
        """List recent jobs (lightweight view)."""
        if isinstance(status, str):
            status = JobStatus(status)

        rows = self._store.list(status=status, tags=tags, limit=limit)
        results: list[JobResult] = []
        for row in rows:
            results.append(self.get(row["id"]))
        return results

    def status(self, job_id: str) -> JobStatus:
        """Convenience: just the current status."""
        row = self._store.get(job_id)
        if row is None:
            raise KeyError(f"Job not found: {job_id}")
        return JobStatus(row["status"])

    def cancel(self, job_id: str) -> bool:
        """Best-effort cancellation.

        Marks the job CANCELLED. A running worker (future) may still finish
        the current generation; true mid-generation interrupt is best-effort.
        """
        row = self._store.get(job_id)
        if row is None:
            return False

        current = JobStatus(row["status"])
        if current in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
            return False

        self._store.update_status(job_id, JobStatus.CANCELLED)
        # Synthesize a cancelled result so get() returns something useful
        result = JobResult(
            job_id=job_id,
            status=JobStatus.CANCELLED,
            model=row.get("request", {}).get("model"),
            error="Cancelled by user",
        )
        self._store.set_result(job_id, result)
        return True

    def wait(self, job_id: str, timeout: float | None = None) -> JobResult:
        """Block until the job reaches a terminal state or timeout.

        Polling implementation (sufficient until we have events).
        Useful for tests and simple scripts.
        """
        deadline = time.time() + timeout if timeout else None
        while True:
            res = self.get(job_id)
            if res.status in (JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED):
                return res
            if deadline and time.time() > deadline:
                raise TimeoutError(f"Timed out waiting for job {job_id}")
            time.sleep(0.2)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _parse_dt(self, v: Any) -> datetime | None:
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            return None

    def _redeliver_restart_callbacks(self) -> None:
        """On startup, attempt to re-deliver callbacks for recently completed jobs.

        Only named keys (from the registry passed to this Hoglah instance) are
        re-delivered. Direct callables from previous process runs are lost
        (as expected).
        """
        if not self._callbacks:
            return

        # Look for terminal jobs (by status or by presence of result_json) that
        # have a callback_key we know about. This is robust even if a test
        # directly mutates the store.
        for row in self._store.list(limit=200):
            key = row.get("callback_key")
            if not key or key not in self._callbacks:
                continue

            status_val = row.get("status")
            has_result = bool(row.get("result_json"))
            if status_val in ("completed", "failed", "cancelled") or has_result:
                try:
                    result = self.get(row["id"])
                    self._callbacks[key](result)
                except Exception:
                    # Never let a callback failure affect job state or startup
                    pass

    def close(self) -> None:
        """Close underlying resources."""
        if hasattr(self._store, "close"):
            self._store.close()


# For convenience in __init__.py
__all__ = ["Hoglah", "HoglahConfig"]