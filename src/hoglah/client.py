"""Hoglah client — synchronous public facade (ADR-012).

The public API is intentionally synchronous and simple:

    h = Hoglah()  # or Hoglah(config=..., callbacks={"key": my_func})
    job_id = h.submit(prompt=..., model="gemma:7b", callback=..., tags=...)
    result = h.wait(job_id, timeout=120)

Internally:
- Uses a pluggable JobStore (SQLite by default).
- Full request is persisted so the background worker can execute it.
- Supports both direct callables (current-process only) and named callback_key
  (durable across restarts when the registry is re-supplied) per ADR-006.
- A background asyncio worker (in a daemon thread) picks up QUEUED jobs,
  executes them via Ollama, updates results, fires callbacks, and handles
  retries + recovery (per ADRs 003, 006, 009, 011, 012).
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from .adapters import BaseAdapter, OllamaAdapter, StubAdapter
from .config import HoglahConfig, HoglahSettings
from .models import (
    JobCallback,
    JobRequest,
    JobResult,
    JobStatus,
    normalize_request,
)
from .store import JobStore, create_sqlite_store

logger = logging.getLogger("hoglah")


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

    Context manager (recommended for scripts):
        with Hoglah() as h:
            job_id = h.submit(...)
            result = h.wait(job_id)

    Real execution (when Ollama is available):
        from hoglah import Hoglah, OllamaAdapter
        h = Hoglah(use_real=True)                    # or adapter=OllamaAdapter()
        # or: h = Hoglah(config={"ollama_host": "http://..."}, use_real=True)
    """

    def __init__(
        self,
        config: HoglahConfig | dict[str, Any] | None = None,
        *,
        callbacks: dict[str, JobCallback] | None = None,
        store: JobStore | None = None,
        adapter: BaseAdapter | None = None,
        use_real: bool = False,
        start_worker: bool = True,
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

        # Execution adapter selection (priority order):
        #   1. Explicit adapter= passed in
        #   2. use_real=True kwarg (or HOGLAH_USE_REAL_ADAPTER env)
        #   3. Safe default: StubAdapter (no network, works everywhere)
        if adapter is None:
            env_wants_real = os.environ.get("HOGLAH_USE_REAL_ADAPTER", "").lower() in ("1", "true", "yes", "on")
            if use_real or env_wants_real:
                adapter = OllamaAdapter(host=self.config.ollama_host)
            else:
                adapter = StubAdapter()
        self.adapter: BaseAdapter = adapter

        # Worker control (for background asyncio loop in daemon thread)
        self._worker_running = False
        self._worker_thread: threading.Thread | None = None

        # Attempt restart callback re-delivery for jobs that completed while we were down
        self._redeliver_restart_callbacks()

        # Recover any jobs that were PROCESSING when the previous process died
        self._recover_interrupted_jobs()

        # Start the background worker (asyncio loop in a daemon thread).
        # Pass start_worker=False when you want to control execution manually (e.g. tests).
        if start_worker:
            self._start_background_worker()

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

    # ------------------------------------------------------------------ #
    # Background worker (asyncio in thread) + Ollama execution (Chunk 2)
    # ------------------------------------------------------------------ #

    def _start_background_worker(self) -> None:
        if self._worker_running:
            return
        self._worker_running = True
        self._worker_thread = threading.Thread(
            target=self._run_worker_thread, daemon=True, name="hoglah-worker"
        )
        self._worker_thread.start()
        logger.debug("Hoglah background worker started (concurrency=%s)", self.config.concurrency)

    def _run_worker_thread(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._worker_loop())
        except Exception:
            logger.exception("Worker thread crashed")
        finally:
            loop.close()

    async def _worker_loop(self) -> None:
        sem = asyncio.Semaphore(self.config.concurrency)
        poll_interval = 0.5

        while self._worker_running:
            try:
                # Get a small batch of queued jobs (priority + age order is handled by store)
                queued = self._store.list(status=JobStatus.QUEUED, limit=10)
                for row in queued:
                    if not self._worker_running:
                        break
                    job_id = row["id"]
                    # Acquire slot and process (fire and forget the task)
                    await sem.acquire()
                    asyncio.create_task(self._process_job(job_id, sem))

                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in worker loop")
                await asyncio.sleep(1.0)

    async def _process_job(self, job_id: str, sem: asyncio.Semaphore) -> None:
        """Claim and execute one job. Release semaphore on exit."""
        try:
            claimed = self._store.claim_for_processing(job_id)
            if not claimed:
                return

            row = self._store.get(job_id)
            if not row:
                return

            request = JobRequest(**row.get("request", {}))
            callback_key = row.get("callback_key")

            # Execute with retries
            result = await self._execute_with_retries(job_id, request)

            # Persist final result
            self._store.set_result(job_id, result)

            # Fire callback (direct if present for this process, else via registry)
            self._fire_callback(job_id, result, callback_key)

        except Exception:
            logger.exception("Unexpected error processing job %s", job_id)
            # Best effort: record failure
            try:
                err_result = JobResult(
                    job_id=job_id,
                    status=JobStatus.FAILED,
                    error="Unexpected worker error",
                    model=getattr(request, "model", None) if "request" in locals() else None,
                )
                self._store.set_result(job_id, err_result)
            except Exception:
                pass
        finally:
            sem.release()

    async def _execute_with_retries(self, job_id: str, request: JobRequest) -> JobResult:
        """Run the Ollama call, with simple retry for transient errors."""
        max_retries = getattr(request, "max_retries", 2) or 2
        _timeout = getattr(request, "timeout_seconds", None)  # respected by caller / adapter if needed

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                output, usage, meta = await self._call_ollama(request)
                # Success (even if internally truncated)
                return JobResult(
                    job_id=job_id,
                    status=JobStatus.COMPLETED,
                    output=output,
                    model=request.model,
                    parameters=asdict(request),
                    usage=usage,
                    timings={"finished_at": datetime.now(timezone.utc)},
                    tags=request.tags or [],
                    metadata={**(request.metadata or {}), **meta},
                    parent_job_id=request.parent_job_id,
                    truncated=meta.get("truncated", False),
                    truncation_reason=meta.get("truncation_reason"),
                    estimated_prompt_tokens=usage.get("prompt_tokens"),
                    effective_num_ctx=request.num_ctx,
                )
            except Exception as exc:
                last_error = str(exc)
                is_transient = self._is_transient_error(exc)
                logger.warning(
                    "Job %s attempt %s/%s failed: %s (transient=%s)",
                    job_id, attempt + 1, max_retries + 1, last_error, is_transient
                )
                if is_transient and attempt < max_retries:
                    backoff = min(2 ** attempt, 10)  # simple exponential
                    await asyncio.sleep(backoff)
                    continue
                break

        # All retries exhausted or permanent error
        return JobResult(
            job_id=job_id,
            status=JobStatus.FAILED,
            model=request.model,
            error=last_error or "Unknown execution error",
            parameters=asdict(request),
            tags=request.tags or [],
            metadata=request.metadata or {},
            parent_job_id=request.parent_job_id,
        )

    async def _call_ollama(self, req: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
        """
        Delegate to the configured adapter (Stub by default; OllamaAdapter for real).
        """
        return await self.adapter.run(req)

    def _is_transient_error(self, exc: Exception) -> bool:
        """Simple classification for retry (per ADR-011)."""
        msg = str(exc).lower()
        if any(x in msg for x in ("connection", "timeout", "rate", "5", "server", "unavailable")):
            return True
        # Context errors are not transient (we still report them)
        if "context" in msg or "exceed" in msg:
            return False
        return False

    def _fire_callback(self, job_id: str, result: JobResult, callback_key: str | None) -> None:
        """Fire callback if registered (direct for this process or via named registry)."""
        cb = self._direct_callbacks.get(job_id)
        if cb is None and callback_key:
            cb = self._callbacks.get(callback_key)

        if cb is not None:
            try:
                cb(result)
            except Exception:
                logger.exception("Callback for job %s failed (ignored)", job_id)
            # Clean direct callback after firing
            self._direct_callbacks.pop(job_id, None)

    def _recover_interrupted_jobs(self) -> None:
        """On startup, deal with jobs that were left in PROCESSING state."""
        for row in self._store.list(status=JobStatus.PROCESSING, limit=50):
            job_id = row["id"]
            req = row.get("request", {})
            _max_r = req.get("max_retries", 2)  # available for future more sophisticated recovery policy

            # Simple policy: move back to QUEUED so the worker can retry.
            # (We could also mark FAILED with "interrupted" if we tracked attempts.)
            self._store.update_status(
                job_id, JobStatus.QUEUED, error="Recovered from interrupted processing"
            )
            logger.info("Recovered interrupted job %s (re-queued for retry)", job_id)

    def close(self) -> None:
        """Stop worker and close resources."""
        self._worker_running = False
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=3.0)
        if hasattr(self._store, "close"):
            self._store.close()

    def stats(self) -> dict[str, Any]:
        """Return basic queue statistics (status counts, totals).

        Useful for monitoring or dashboards. Example:
            s = h.stats()
            print(s["counts"])
        """
        counts = self._store.get_status_counts() if hasattr(self._store, "get_status_counts") else {}
        total = sum(counts.values()) if counts else 0
        return {
            "counts": counts,
            "total_jobs": total,
            "queued": counts.get(JobStatus.QUEUED.value, 0),
            "processing": counts.get(JobStatus.PROCESSING.value, 0),
            "completed": counts.get(JobStatus.COMPLETED.value, 0),
            "failed": counts.get(JobStatus.FAILED.value, 0),
            "cancelled": counts.get(JobStatus.CANCELLED.value, 0),
        }

    def clear(
        self,
        *,
        status: JobStatus | str | None = None,
        older_than_days: int | None = None,
    ) -> int:
        """Delete old/terminal jobs from the store.

        Returns the number of jobs removed.
        Useful for maintenance of long-running queues.
        """
        if isinstance(status, str):
            status = JobStatus(status)
        before = None
        if older_than_days is not None:
            cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
            before = cutoff.isoformat()
        return self._store.delete_jobs(status=status, before=before) if hasattr(self._store, "delete_jobs") else 0

    def pull_model(self, model: str) -> None:
        """Ensure the given model is available (pulls if using real adapter and missing).

        Safe no-op for StubAdapter. Useful before submitting jobs with use_real=True.
        """
        import asyncio

        async def _pull():
            await self.adapter.pull_model(model)

        try:
            asyncio.run(_pull())
        except RuntimeError:
            # Fallback if we're inside an existing event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_pull())
            loop.close()

    def __enter__(self) -> "Hoglah":
        """Support `with Hoglah(...) as h:` for automatic cleanup."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False  # do not suppress exceptions


# For convenience in __init__.py
__all__ = ["Hoglah", "HoglahConfig"]