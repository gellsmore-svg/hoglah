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
        self._config = config
        self._db = db
        self._db_resolved = db is not None

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _database(self) -> Any:
        """Lazily open the shared trace database (None → bus-only emission)."""
        if self._db_resolved:
            return self._db
        self._db_resolved = True
        try:
            from pymongo import MongoClient

            client = MongoClient(
                self._config.galeed_mongo_uri, serverSelectionTimeoutMS=2000
            )
            self._db = client[self._config.galeed_mongo_db]
        except Exception:
            logger.debug("galeed trace db unavailable; emitting bus-only", exc_info=True)
            self._db = None
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
            tracer = Tracer(
                trace_id=job_id,
                session_id=session_id or "hoglah",
                db=self._database(),
                source="hoglah",
            )
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
