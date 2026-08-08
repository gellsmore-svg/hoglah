"""Optional Galeed trace emission — witness the job lifecycle on the family spine.

Hoglah is infrastructure: its work happens in a background worker, invisible to
the family's trace stream (Galeed) unless it testifies. When enabled, the client
emits one event per job-lifecycle transition (``job.queued``, ``job.started``,
``job.completed``, ``job.failed``, ``job.cancelled``) with the job's id as the
``trace_id`` — so one job's events stitch into one trace — and mirrored into
``metadata.job_id`` (Galeed's documented cross-project correlation key).

Strictly best-effort and optional: ``galeed`` (and ``pymongo``, for persistence
into the shared family MongoDB) are imported lazily. When disabled, not
installed, or unreachable, every call is a silent no-op — tracing must never
affect the queue.

Enable via config: ``galeed_enabled=True`` (env ``HOGLAH_GALEED_ENABLED=1``).
Events persist to ``galeed_mongo_uri`` / ``galeed_mongo_db``, which should point
at the database the family's trace API (Tirzah ``/api/trace``, Mizpah) reads.
"""

from __future__ import annotations

import logging
import threading
from collections import OrderedDict
from typing import Any

logger = logging.getLogger("hoglah")

# The job-lifecycle vocabulary (mirrors galeed.EventType.JOB_*; string literals
# here so any installed galeed version accepts them — the vocabulary is open).
JOB_QUEUED = "job.queued"
JOB_STARTED = "job.started"
JOB_COMPLETED = "job.completed"
JOB_FAILED = "job.failed"
JOB_CANCELLED = "job.cancelled"

_TERMINAL_SEVERITY = {JOB_FAILED: "error"}


class JobWitness:
    """Best-effort emitter of job-lifecycle events onto the Galeed spine."""

    def __init__(self, config: Any, db: Any = None) -> None:
        self._enabled = bool(getattr(config, "galeed_enabled", False))
        self._capture_io = bool(getattr(config, "galeed_capture_io", True))
        self._config = config
        self._db = db
        self._db_resolved = db is not None
        self._db_lock = threading.Lock()
        self._mongo_client: Any = None  # owned client, closed on close() (M7)
        self._tracers: "OrderedDict[str, Any]" = OrderedDict()
        self._tracers_lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def close(self) -> None:
        """Close any MongoClient this witness opened (review M7)."""
        with self._db_lock:
            client = self._mongo_client
            self._mongo_client = None
            self._db = None
            self._db_resolved = True
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                logger.debug("galeed MongoClient close failed", exc_info=True)

    def _database(self) -> Any:
        """Lazily open the shared trace database (None → bus-only emission).

        Locked, and the resolved flag flips only AFTER the handle is assigned:
        the submitter (main thread) and the worker (event-loop thread) can both
        hit the first emission within milliseconds, and an unlocked check-then-
        act here made the worker read a half-initialised None and silently drop
        its events."""
        with self._db_lock:
            if self._db_resolved:
                return self._db
            try:
                from pymongo import MongoClient

                client = MongoClient(
                    self._config.galeed_mongo_uri, serverSelectionTimeoutMS=2000
                )
                self._mongo_client = client
                self._db = client[self._config.galeed_mongo_db]
            except Exception:
                logger.debug("galeed trace db unavailable; emitting bus-only", exc_info=True)
                self._db = None
                self._mongo_client = None
            self._db_resolved = True
            return self._db

    def emit(
        self,
        type: str,
        *,
        job_id: str,
        status: str = "ok",
        summary: str = "",
        session_id: str | None = None,
        **metadata: Any,
    ) -> None:
        """Emit one lifecycle event; swallows every failure by design."""
        if not self._enabled:
            return
        try:
            from galeed import Tracer
        except ImportError:
            self._enabled = False
            logger.debug("galeed not installed; job tracing disabled for this instance")
            return
        try:
            tracer = self._tracer_for(Tracer, job_id, session_id or "hoglah")
            tracer.emit(
                type,
                status=status,
                summary=summary,
                severity=_TERMINAL_SEVERITY.get(type, "info"),
                job_id=job_id,
                **metadata,
            )
        except Exception:
            logger.debug("galeed emit failed (ignored)", exc_info=True)

    def _tracer_for(self, tracer_cls: Any, job_id: str, session_id: str) -> Any:
        """One Tracer per job (bounded LRU of 256): lifecycle events share a
        sequence and tracer/Mongo setup isn't repeated per emit."""
        with self._tracers_lock:
            tracer = self._tracers.get(job_id)
            if tracer is None:
                tracer = tracer_cls(
                    trace_id=job_id,
                    session_id=session_id,
                    db=self._database(),
                    source="hoglah",
                )
                self._tracers[job_id] = tracer
                while len(self._tracers) > 256:
                    self._tracers.popitem(last=False)
            else:
                self._tracers.move_to_end(job_id)
            return tracer

    def record_io(
        self,
        *,
        job_id: str,
        request: Any,
        result: Any,
        started_at: str | None = None,
        completed_at: str | None = None,
        duration_ms: int | None = None,
    ) -> None:
        """Record the job's FULL input/output into galeed's llm_calls (the family
        LLM debugging view). One document per executed job:

        - call_id = job_id; parent_call_id = parent_job_id (so Python-augmented
          chains render as a tree);
        - trace_id = request.metadata["trace_id"] when the caller threads one
          through (linking every call of a multi-step flow into one trace),
          else the job_id;
        - technical detail (params, usage, truncation) rides in metadata, which
          viewers only show in advanced mode.

        Best-effort like everything else here; embedding jobs are skipped
        (vectors are not debugging reading material).
        """
        if not (self._enabled and self._capture_io):
            return
        if getattr(request, "kind", "generate") == "embed":
            return
        try:
            from galeed import record_llm_call
        except ImportError:
            return
        try:
            request_meta = dict(getattr(request, "metadata", None) or {})
            detail = {
                "kind": getattr(request, "kind", None),
                "tags": getattr(request, "tags", None) or [],
                "system_prompt": getattr(request, "system_prompt", None),
                "usage": getattr(result, "usage", None) or {},
                "truncated": getattr(result, "truncated", False),
                "num_ctx": getattr(request, "num_ctx", None),
                "job_id": job_id,
            }
            record_llm_call(
                self._database(),
                call_id=job_id,
                trace_id=request_meta.get("trace_id") or job_id,
                session_id=request_meta.get("session_id") or "hoglah",
                source="hoglah",
                step_name=request_meta.get("step_name"),
                parent_call_id=getattr(request, "parent_job_id", None),
                model=getattr(request, "model", None),
                prompt=getattr(request, "prompt", None),
                messages=getattr(request, "messages", None),
                output=getattr(result, "output", None),
                error=getattr(result, "error", None),
                # Token counts are also left in `detail` for the advanced view,
                # but pass them explicitly: galeed stores usage as a first-class
                # field, which is what makes cost aggregatable.
                usage=getattr(result, "usage", None) or {},
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                metadata=detail,
                emit_event=False,  # the job.* lifecycle events already mark the spine
            )
        except TypeError:
            # An older galeed in this environment does not accept the newer
            # kwargs. Retry without them so lifecycle/IO capture still works,
            # and say so at WARNING: this silently produced cost-free records
            # once, and a metric that reads zero is worse than one that is
            # absent.
            logger.warning(
                "galeed is older than this hoglah — recording llm_call without "
                "usage/timing. Upgrade galeed to restore cost instrumentation.",
            )
            try:
                record_llm_call(
                    self._database(),
                    call_id=job_id,
                    trace_id=request_meta.get("trace_id") or job_id,
                    session_id=request_meta.get("session_id") or "hoglah",
                    source="hoglah",
                    step_name=request_meta.get("step_name"),
                    parent_call_id=getattr(request, "parent_job_id", None),
                    model=getattr(request, "model", None),
                    prompt=getattr(request, "prompt", None),
                    messages=getattr(request, "messages", None),
                    output=getattr(result, "output", None),
                    error=getattr(result, "error", None),
                    metadata=detail,  # usage still rides here for the old shape
                    emit_event=False,
                )
            except Exception:
                logger.debug("galeed llm_calls fallback failed (ignored)", exc_info=True)
        except Exception:
            logger.debug("galeed llm_calls record failed (ignored)", exc_info=True)
