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
import json
import logging
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
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

# Bounded window for in-flight jobs to finish on shutdown, kept under
# close()'s 3.0s worker-thread join so the join still succeeds.
_SHUTDOWN_DRAIN_SECONDS = 2.0


def _run_async(coro_factory):
    """Run an async coroutine to completion from synchronous code, whether
    or not an event loop is already running in this thread.

    `coro_factory` is a zero-arg callable returning a fresh coroutine (so
    it can be (re)created in whichever thread actually runs it). When no
    loop is running we use `asyncio.run`; when one IS running (e.g. the
    caller is inside `asyncio.run`, a notebook, or an async web handler),
    blocking the live loop is impossible, so we run the coroutine in a
    short-lived worker thread and wait for its result. The previous
    `except RuntimeError -> run_until_complete` fallback did not work —
    a second loop cannot run while one is already running in the thread.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro_factory())  # no running loop in this thread
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
        return ex.submit(lambda: asyncio.run(coro_factory())).result()


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

        # Configure hoglah logger (configurable level, basic handler if none)
        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        hoglah_logger = logging.getLogger("hoglah")
        hoglah_logger.setLevel(level)
        if not hoglah_logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(
                logging.Formatter("%(asctime)s [%(levelname)s] hoglah: %(message)s")
            )
            hoglah_logger.addHandler(handler)

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

        # Recover any jobs that were PROCESSING when the previous process died.
        # This is a WORKER responsibility (ADR-016): only an instance that runs
        # the worker may re-queue interrupted jobs. A pure submitter
        # (start_worker=False) sharing the queue with a separate worker daemon
        # must NOT do this, or it would re-queue the daemon's in-flight jobs
        # and cause double processing.
        if start_worker:
            self._recover_interrupted_jobs()

        # Start the background worker (asyncio loop in a daemon thread).
        # Pass start_worker=False when you want to control execution manually (e.g. tests)
        # or to act as a pure submitter into a shared queue (separate daemon executes).
        if start_worker:
            self._start_background_worker()

    # ------------------------------------------------------------------ #
    # Public API (matches the spirit of the requirements submit signature)
    # ------------------------------------------------------------------ #

    def submit(
        self,
        *,
        kind: str = "generate",
        prompt: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        model: str,
        system_prompt: str | None = None,
        num_ctx: int | None = None,
        options: dict[str, Any] | None = None,
        callback: JobCallback | str | None = None,
        callback_url: str | None = None,  # outbound HTTP push on completion (ADR-015)
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
            kind=kind,
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
            callback_url=callback_url,
        )

        # Enqueue (store the request)
        job_id = self._store.enqueue(req, callback_key=callback_key)

        # Remember direct callback for this process (if any)
        if direct_cb is not None:
            self._direct_callbacks[job_id] = direct_cb

        return job_id

    def submit_embedding(
        self,
        text: str,
        *,
        model: str,
        callback: JobCallback | str | None = None,
        callback_url: str | None = None,
        tags: list[str] | None = None,
        priority: int = 0,
        timeout_seconds: int | None = None,
        max_retries: int = 2,
        metadata: dict[str, Any] | None = None,
        parent_job_id: str | None = None,
        **extra: Any,
    ) -> str:
        """Submit an embedding job. Returns the job ID immediately.

        Convenience wrapper over submit(kind="embed"): the input text is
        carried in `prompt` and the worker routes it to adapter.embed(); the
        resulting JobResult has `embedding` / `embedding_dim` set and `output`
        is None.
        """
        return self.submit(
            kind="embed",
            prompt=text,
            model=model,
            callback=callback,
            callback_url=callback_url,
            tags=tags,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            metadata=metadata,
            parent_job_id=parent_job_id,
            **extra,
        )

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
                embedding=res.get("embedding"),
                embedding_dim=res.get("embedding_dim"),
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
        parent_job_id: str | None = None,
        limit: int = 100,
    ) -> list[JobResult]:
        """List recent jobs (lightweight view)."""
        if isinstance(status, str):
            status = JobStatus(status)

        rows = self._store.list(status=status, tags=tags, parent_job_id=parent_job_id, limit=limit)
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
        self._deliver(result)
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
        # Track in-flight job tasks so shutdown can drain them instead of
        # destroying the loop mid-generation. (Jobs are already time-bounded
        # by timeout_seconds; this just lets near-done ones finish cleanly.)
        inflight: set[asyncio.Task] = set()

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
                    task = asyncio.create_task(self._process_job(job_id, sem))
                    inflight.add(task)
                    task.add_done_callback(inflight.discard)

                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("Error in worker loop")
                await asyncio.sleep(1.0)

        # Graceful drain on shutdown: let in-flight jobs finish within a
        # bounded window (under close()'s 3s thread-join), then cancel any
        # stragglers so set_result still records a terminal state.
        pending = {t for t in inflight if not t.done()}
        if pending:
            done, still = await asyncio.wait(pending, timeout=_SHUTDOWN_DRAIN_SECONDS)
            for t in still:
                t.cancel()
            if still:
                await asyncio.gather(*still, return_exceptions=True)

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

            # Out-of-band delivery for decoupled submitters (output file; H3 callback)
            self._deliver(result, request)

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
                self._deliver(err_result, request if "request" in locals() else None)
            except Exception:
                pass
        finally:
            sem.release()

    async def _execute_with_retries(self, job_id: str, request: JobRequest) -> JobResult:
        """Run the Ollama call, with simple retry for transient errors.

        `timeout_seconds` (ADR-011) caps each attempt's wall-clock time and,
        on expiry, marks the job FAILED without retry — the budget is the
        whole point, so retrying would defeat it. `asyncio.wait_for` also
        cancels the in-flight generation, freeing the worker slot rather
        than leaking it to a stuck model.
        """
        max_retries = getattr(request, "max_retries", 2) or 2
        _timeout = getattr(request, "timeout_seconds", None)
        is_embed = getattr(request, "kind", "generate") == "embed"

        last_error = None
        timed_out = False
        for attempt in range(max_retries + 1):
            try:
                if _timeout is not None and _timeout > 0:
                    payload = await asyncio.wait_for(
                        self._dispatch(request), timeout=_timeout
                    )
                else:
                    payload = await self._dispatch(request)
                if is_embed:
                    vector, usage, meta = payload
                    return JobResult(
                        job_id=job_id,
                        status=JobStatus.COMPLETED,
                        model=request.model,
                        parameters=asdict(request),
                        usage=usage,
                        timings={"finished_at": datetime.now(timezone.utc)},
                        tags=request.tags or [],
                        metadata={**(request.metadata or {}), **meta},
                        parent_job_id=request.parent_job_id,
                        estimated_prompt_tokens=usage.get("prompt_tokens"),
                        embedding=vector,
                        embedding_dim=meta.get("embedding_dim") or len(vector),
                    )
                # Generation: success (even if internally truncated)
                output, usage, meta = payload
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
                    effective_num_ctx=meta.get("effective_num_ctx") or request.num_ctx,
                )
            except asyncio.TimeoutError:
                # ADR-011: timeout_seconds marks the job failed (terminal,
                # not retried). Caught before the generic handler so it is
                # never misread as a transient error and retried.
                timed_out = True
                last_error = f"Timed out after {_timeout}s (timeout_seconds)"
                logger.warning("Job %s timed out after %ss", job_id, _timeout)
                break
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
            metadata={**(request.metadata or {}), **({"timed_out": True} if timed_out else {})},
            parent_job_id=request.parent_job_id,
        )

    async def _dispatch(self, req: JobRequest) -> tuple[Any, dict[str, int], dict[str, Any]]:
        """Route a request to the adapter by kind: embeddings to embed(),
        everything else to run(). Both return (payload, usage, metadata) where
        payload is a vector for embeddings or output text for generation."""
        if getattr(req, "kind", "generate") == "embed":
            return await self.adapter.embed(req)
        return await self._call_ollama(req)

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

    def _deliver(self, result: JobResult, request: JobRequest | None = None) -> None:
        """Out-of-band delivery of a terminal result for decoupled submitters.

        Two independent mechanisms, both best-effort (a delivery failure must
        never change the job's persisted terminal status):
        - ADR-014: if `output_dir` is configured, write the full result to
          `<output_dir>/<job_id>.json` atomically (so a poller never reads a
          partial file).
        - ADR-015: if the request carries a `callback_url`, POST the result
          JSON to it. The output file (when configured) is the natural
          fallback if the push fails.
        """
        payload = json.dumps(asdict(result), default=str)

        out_dir = self.config.output_dir
        if out_dir is not None:
            try:
                dest = Path(out_dir) / f"{result.job_id}.json"
                tmp = dest.with_suffix(".json.tmp")
                tmp.write_text(payload, encoding="utf-8")
                os.replace(tmp, dest)  # atomic on the same filesystem
            except Exception:
                logger.exception("Failed to write output file for job %s", result.job_id)

        callback_url = getattr(request, "callback_url", None) if request is not None else None
        if callback_url:
            # Deliver on a daemon thread so a slow/unreachable endpoint and its
            # retry backoff never block the async worker loop (or cancel()'s
            # caller). The output file remains the durable fallback.
            threading.Thread(
                target=self._post_callback,
                args=(callback_url, result.job_id, payload),
                daemon=True,
                name=f"hoglah-callback-{result.job_id[:8]}",
            ).start()

    def _post_callback(self, url: str, job_id: str, payload: str) -> None:
        """POST `payload` (a serialized JobResult) to `url`, retrying transient
        failures with exponential backoff. Best-effort: gives up after
        `callback_max_retries` attempts and logs — the output file (if any) is
        the fallback path for the submitter."""
        attempts = self.config.callback_max_retries
        timeout = self.config.callback_timeout_seconds
        data = payload.encode("utf-8")
        last_err: str | None = None
        for attempt in range(attempts):
            try:
                req = urllib.request.Request(
                    url,
                    data=data,
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    if 200 <= resp.status < 300:
                        logger.debug("Callback for job %s delivered to %s", job_id, url)
                        return
                    last_err = f"HTTP {resp.status}"
            except urllib.error.HTTPError as exc:
                # 4xx is unlikely to succeed on retry; stop early. 5xx may be transient.
                last_err = f"HTTP {exc.code}"
                if 400 <= exc.code < 500:
                    break
            except Exception as exc:
                last_err = str(exc)
            if attempt < attempts - 1:
                time.sleep(min(2 ** attempt, 8))
        logger.warning(
            "Callback POST to %s failed for job %s after %d attempt(s): %s",
            url, job_id, attempts, last_err,
        )

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

    def remove(self, job_id: str) -> bool:
        """Delete a job by ID. Returns True if a job was removed.
        Works for any status (best-effort).
        """
        if hasattr(self._store, "delete_job"):
            return self._store.delete_job(job_id)
        return False

    def info(self) -> dict[str, Any]:
        """Return a snapshot of the instance: config, adapter type, and current stats.

        Useful for debugging and monitoring.
        """
        from . import __version__
        return {
            "version": __version__,
            "config": self.config.to_dict(),
            "adapter": type(self.adapter).__name__,
            "stats": self.stats(),
        }

    def pull_model(self, model: str) -> None:
        """Ensure the given model is available (pulls if using real adapter and missing).

        Safe no-op for StubAdapter. Useful before submitting jobs with use_real=True.
        Callable from sync code or from within a running event loop.
        """
        _run_async(lambda: self.adapter.pull_model(model))

    def show_model(self, model: str) -> dict[str, Any]:
        """Return details for a model (via adapter.show_model).

        Useful for inspecting context size, template, etc. (especially with real adapter).
        Callable from sync code or from within a running event loop.
        """
        return _run_async(lambda: self.adapter.show_model(model))

    def __enter__(self) -> "Hoglah":
        """Support `with Hoglah(...) as h:` for automatic cleanup."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False  # do not suppress exceptions


# For convenience in __init__.py
__all__ = ["Hoglah", "HoglahConfig"]