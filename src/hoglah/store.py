"""Pluggable job persistence (ADR-002).

Defines a minimal JobStore protocol so that SQLite (default) and future
MongoDB (or other) implementations can be swapped with little change to the
rest of the code.

V1 implementation uses the stdlib sqlite3 (lightweight, no extra runtime dep).
DB operations are synchronous; the worker (when added) will run them via
loop.run_in_executor if needed.

The store is responsible only for durability + query. All business logic
(request normalization, status machines, truncation reporting, callbacks)
lives in the client.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from .models import JobRequest, JobResult, JobStatus, new_job_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@runtime_checkable
class JobStore(Protocol):
    """Minimal protocol for job storage backends.

    Implementations must be thread-safe enough for the intended usage
    (single worker + concurrent submit from user code is fine with WAL or
    simple locking).
    """

    def enqueue(
        self,
        request: JobRequest,
        *,
        job_id: str | None = None,
        callback_key: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        """Persist a new job and return its ID. Status starts as QUEUED.

        If `correlation_id` is given it is stored under a UNIQUE constraint and
        enqueue becomes **idempotent**: re-enqueuing the same correlation_id is a
        no-op that returns the existing job's id. This is what makes the Kafka
        bridge's at-least-once redelivery safe (ADR-018) — a message redelivered
        after a crash never creates a duplicate job.
        """
        ...

    def get(self, job_id: str) -> dict[str, Any] | None:
        """Return the raw stored job record (or None)."""
        ...

    def list(
        self,
        *,
        status: JobStatus | None = None,
        tags: list[str] | None = None,
        parent_job_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """List jobs with simple filters."""
        ...

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        *,
        error: str | None = None,
    ) -> None:
        """Update the status (and optionally error) of a job."""
        ...

    def set_result(self, job_id: str, result: JobResult) -> None:
        """Store the final JobResult for a completed/failed/cancelled job."""
        ...

    def claim_for_processing(self, job_id: str) -> bool:
        """Best-effort atomic transition QUEUED -> PROCESSING.

        Returns True if the transition succeeded (this worker owns the job).
        Used by the future worker loop.
        """
        ...

    def close(self) -> None:
        """Release any resources (e.g. DB connection)."""
        ...

    def get_status_counts(self) -> dict[str, int]:
        """Return dict of status -> count for quick queue overview."""
        ...

    def delete_jobs(
        self,
        *,
        status: JobStatus | None = None,
        before: str | None = None,  # ISO timestamp, delete jobs updated before this
    ) -> int:
        """Delete jobs matching filters. Returns number deleted."""
        ...

    def delete_job(self, job_id: str) -> bool:
        """Delete a single job by ID. Returns True if a job was deleted."""
        ...

    def mark_result_published(self, job_id: str) -> None:
        """Mark a terminal job's result as externally published (Kafka egress
        outbox, ADR-018). Set only AFTER the broker acks, so a crash before this
        leaves the job eligible for restart re-emit (transactional outbox)."""
        ...

    def list_unpublished_terminal(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Return terminal jobs (with a result) not yet marked published. Used on
        startup to re-emit results that the previous process may have computed
        but not yet delivered to Kafka before crashing (ADR-018)."""
        ...


class SQLiteJobStore:
    """SQLite-backed JobStore (default implementation).

    Schema (simple for V1):
        jobs(
            id TEXT PRIMARY KEY,
            status TEXT,
            priority INTEGER,
            created_at TEXT,
            updated_at TEXT,
            request_json TEXT,      -- full JobRequest
            result_json TEXT,       -- JobResult when terminal
            error TEXT,
            callback_key TEXT,
            tags_json TEXT
        )
    """

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path).expanduser()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            str(self.db_path),
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        # journal_mode=WAL (write-ahead logging): SQLite records changes to a
        # side-log first, so a reader (submit/get on the main thread) is not
        # blocked while the worker writes, and vice versa. busy_timeout=5000
        # makes a contending writer wait up to 5000 ms for the file lock
        # instead of failing immediately with "database is locked". Both matter
        # once a submitter and a worker (or two worker processes) share the
        # single SQLite file.
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._lock = threading.Lock()  # protect concurrent access from worker thread + main
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    result_json TEXT,
                    error TEXT,
                    callback_key TEXT,
                    tags_json TEXT,
                    correlation_id TEXT,
                    result_published INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            # Migrate older DBs created before the Kafka-bridge columns existed
            # (ADR-018): add any missing column in place. Cheap + idempotent.
            existing_cols = {
                r["name"] for r in self._conn.execute("PRAGMA table_info(jobs)").fetchall()
            }
            if "correlation_id" not in existing_cols:
                self._conn.execute("ALTER TABLE jobs ADD COLUMN correlation_id TEXT")
            if "result_published" not in existing_cols:
                self._conn.execute(
                    "ALTER TABLE jobs ADD COLUMN result_published INTEGER NOT NULL DEFAULT 0"
                )
            # Helpful indexes for common queries
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC, created_at)")
            # UNIQUE on correlation_id (when present) gives idempotent enqueue:
            # a redelivered Kafka message can never create a second job.
            self._conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_correlation "
                "ON jobs(correlation_id) WHERE correlation_id IS NOT NULL"
            )
            self._conn.commit()

    def _row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        d = dict(row)
        # Parse JSON fields for convenience
        if d.get("request_json"):
            d["request"] = json.loads(d["request_json"])
        if d.get("result_json"):
            d["result"] = json.loads(d["result_json"])
        if d.get("tags_json"):
            d["tags"] = json.loads(d["tags_json"])
        return d

    def enqueue(
        self,
        request: JobRequest,
        *,
        job_id: str | None = None,
        callback_key: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        if job_id is None:
            job_id = new_job_id()

        now = _now_iso()
        tags = request.tags or []

        with self._lock:
            # Idempotency: if this correlation_id is already enqueued, return the
            # existing job's id without inserting (the Kafka redelivery case).
            if correlation_id is not None:
                row = self._conn.execute(
                    "SELECT id FROM jobs WHERE correlation_id = ?", (correlation_id,)
                ).fetchone()
                if row is not None:
                    return row["id"]
            try:
                self._conn.execute(
                    """
                    INSERT INTO jobs (
                        id, status, priority, created_at, updated_at,
                        request_json, result_json, error, callback_key, tags_json,
                        correlation_id, result_published
                    ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, 0)
                    """,
                    (
                        job_id,
                        JobStatus.QUEUED.value,
                        request.priority,
                        now,
                        now,
                        json.dumps(asdict(request), default=str),
                        callback_key,
                        json.dumps(tags),
                        correlation_id,
                    ),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                # Lost a race on the UNIQUE(correlation_id) index (another
                # thread/process inserted the same correlation_id concurrently).
                # The other insert won; return its id — still exactly one job.
                self._conn.rollback()
                row = self._conn.execute(
                    "SELECT id FROM jobs WHERE correlation_id = ?", (correlation_id,)
                ).fetchone()
                if row is not None:
                    return row["id"]
                raise
        return job_id

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE id = ?", (job_id,)
            ).fetchone()
        return self._row_to_dict(row)

    def list(
        self,
        *,
        status: JobStatus | None = None,
        tags: list[str] | None = None,
        parent_job_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        query = "SELECT * FROM jobs"
        params: list[Any] = []

        where_clauses = []
        if status is not None:
            where_clauses.append("status = ?")
            params.append(status.value)

        if tags:
            # Simple contains check via json (good enough for V1; can be improved)
            for tag in tags:
                where_clauses.append("tags_json LIKE ?")
                params.append(f'%"{tag}"%')

        if parent_job_id:
            # Crude JSON contains for parent in request_json (consistent with tags)
            where_clauses.append("request_json LIKE ?")
            params.append(f'%"parent_job_id": "{parent_job_id}"%')

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)

        query += " ORDER BY priority DESC, created_at ASC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [d for r in rows if (d := self._row_to_dict(r)) is not None]

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        *,
        error: str | None = None,
    ) -> None:
        now = _now_iso()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = ?, updated_at = ?, error = COALESCE(?, error) WHERE id = ?",
                (status.value, now, error, job_id),
            )
            self._conn.commit()

    def set_result(self, job_id: str, result: JobResult) -> None:
        now = _now_iso()
        # Also update status from the result
        with self._lock:
            self._conn.execute(
                """
                UPDATE jobs
                SET status = ?, result_json = ?, updated_at = ?, error = ?
                WHERE id = ?
                """,
                (
                    result.status.value,
                    json.dumps(asdict(result), default=str),
                    now,
                    result.error,
                    job_id,
                ),
            )
            self._conn.commit()

    def claim_for_processing(self, job_id: str) -> bool:
        """Attempt to move QUEUED -> PROCESSING atomically.

        Uses a simple UPDATE ... WHERE for basic safety.
        """
        now = _now_iso()
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (JobStatus.PROCESSING.value, now, job_id, JobStatus.QUEUED.value),
            )
            self._conn.commit()
        return cur.rowcount > 0

    def close(self) -> None:
        self._conn.close()

    def get_status_counts(self) -> dict[str, int]:
        """Return dict of status -> count for quick queue overview."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT status, COUNT(*) as c FROM jobs GROUP BY status"
            ).fetchall()
        return {row["status"]: row["c"] for row in rows}

    def delete_jobs(
        self,
        *,
        status: JobStatus | None = None,
        before: str | None = None,  # ISO timestamp
    ) -> int:
        """Delete jobs matching filters. Returns number deleted."""
        query = "DELETE FROM jobs"
        params: list[Any] = []
        where = []
        if status is not None:
            where.append("status = ?")
            params.append(status.value)
        if before is not None:
            where.append("updated_at < ?")
            params.append(before)
        if where:
            query += " WHERE " + " AND ".join(where)
        with self._lock:
            cur = self._conn.execute(query, params)
            self._conn.commit()
        return cur.rowcount

    def delete_job(self, job_id: str) -> bool:
        """Delete a single job by ID. Returns True if a job was deleted."""
        with self._lock:
            cur = self._conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
            self._conn.commit()
        return cur.rowcount > 0

    def mark_result_published(self, job_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET result_published = 1 WHERE id = ?", (job_id,)
            )
            self._conn.commit()

    def list_unpublished_terminal(self, *, limit: int = 100) -> list[dict[str, Any]]:
        terminal = (JobStatus.COMPLETED.value, JobStatus.FAILED.value, JobStatus.CANCELLED.value)
        placeholders = ",".join("?" * len(terminal))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM jobs
                WHERE status IN ({placeholders})
                  AND result_json IS NOT NULL
                  AND result_published = 0
                  AND correlation_id IS NOT NULL
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (*terminal, limit),
            ).fetchall()
        return [d for r in rows if (d := self._row_to_dict(r)) is not None]


# Convenience factory (used by client)
def create_sqlite_store(db_path: Path | str) -> SQLiteJobStore:
    return SQLiteJobStore(db_path)