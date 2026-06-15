"""MongoDB-backed JobStore (ADR-002 option, implemented).

A drop-in `JobStore` for when a real database server is preferable to the
single-file SQLite default:

  - **Multi-process / multi-machine workers** connect to one server natively,
    so there is no single-file locking — the SQLite WAL / busy_timeout dance
    does not apply. `claim_for_processing` is atomic server-side via
    `find_one_and_update`, so concurrent workers still execute each job once.
  - **External visibility**: jobs are stored as native documents (request /
    result / tags are sub-documents, not JSON blobs), so the queue is directly
    inspectable from `mongosh`, Compass, or any other service.

Returned rows mirror SQLiteJobStore exactly (parsed `request`/`result`/`tags`
plus the raw `request_json`/`result_json`/`tags_json` strings the client also
reads), so this is a true drop-in.

`pymongo` is an optional dependency — install via `pip install "hoglah[mongo]"`.
It is imported lazily so SQLite users never need it.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from .models import JobRequest, JobResult, JobStatus, new_job_id


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bson_safe(obj: Any) -> Any:
    """Round-trip a dataclass-dict through JSON so it contains only BSON-safe
    primitives (datetimes etc. become strings), matching how SQLiteJobStore
    serialises, while still landing as a readable native Mongo sub-document."""
    return json.loads(json.dumps(obj, default=str))


class MongoJobStore:
    """MongoDB-backed JobStore. Document `_id` is the job id."""

    def __init__(
        self,
        uri: str = "mongodb://localhost:27017",
        db_name: str = "hoglah",
        collection: str = "jobs",
    ):
        try:
            from pymongo import ASCENDING, DESCENDING, MongoClient
        except ImportError as exc:  # pragma: no cover - import guard
            raise ImportError(
                "MongoJobStore requires pymongo. Install with: pip install 'hoglah[mongo]'"
            ) from exc

        self._client = MongoClient(uri)
        self._col = self._client[db_name][collection]
        # The worker polls `status == QUEUED` then sorts by (priority desc,
        # created_at asc); this compound index covers that filter+sort in one,
        # and its `status` prefix also serves the status-only lookups
        # (get_status_counts / delete_jobs by status). The second index serves
        # an unfiltered list()'s sort. create_index is idempotent.
        self._col.create_index([("status", ASCENDING), ("priority", DESCENDING), ("created_at", ASCENDING)])
        self._col.create_index([("priority", DESCENDING), ("created_at", ASCENDING)])
        # UNIQUE on correlation_id (only for docs that have one) gives idempotent
        # enqueue: a redelivered Kafka message can't create a second job (ADR-018).
        self._col.create_index(
            [("correlation_id", ASCENDING)],
            unique=True,
            partialFilterExpression={"correlation_id": {"$exists": True}},
        )

    def _doc_to_dict(self, doc: dict[str, Any] | None) -> dict[str, Any] | None:
        if doc is None:
            return None
        request = doc.get("request")
        result = doc.get("result")
        tags = doc.get("tags") or []
        return {
            "id": doc["_id"],
            "status": doc.get("status"),
            "priority": doc.get("priority", 0),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
            "error": doc.get("error"),
            "callback_key": doc.get("callback_key"),
            # Native sub-documents (what makes the queue inspectable in Mongo)…
            "request": request,
            "result": result,
            "tags": tags,
            # …and the raw-string forms SQLiteJobStore also returns, so callers
            # that read request_json/result_json/tags_json keep working.
            "request_json": json.dumps(request, default=str) if request is not None else None,
            "result_json": json.dumps(result, default=str) if result is not None else None,
            "tags_json": json.dumps(tags, default=str),
        }

    def enqueue(
        self,
        request: JobRequest,
        *,
        job_id: str | None = None,
        callback_key: str | None = None,
        correlation_id: str | None = None,
    ) -> str:
        from pymongo.errors import DuplicateKeyError

        if job_id is None:
            job_id = new_job_id()
        now = _now_iso()
        # Idempotency: a correlation_id already present means this is a Kafka
        # redelivery — return the existing job's id without inserting.
        if correlation_id is not None:
            existing = self._col.find_one({"correlation_id": correlation_id}, {"_id": 1})
            if existing is not None:
                return existing["_id"]
        doc: dict[str, Any] = {
            "_id": job_id,
            "status": JobStatus.QUEUED.value,
            "priority": request.priority,
            "created_at": now,
            "updated_at": now,
            "request": _bson_safe(asdict(request)),
            "result": None,
            "error": None,
            "callback_key": callback_key,
            "tags": list(request.tags or []),
            "result_published": False,
        }
        if correlation_id is not None:
            doc["correlation_id"] = correlation_id
        try:
            self._col.insert_one(doc)
        except DuplicateKeyError:
            # Lost a race on the unique correlation_id index; the other insert
            # won. Return its id — still exactly one job.
            existing = self._col.find_one({"correlation_id": correlation_id}, {"_id": 1})
            if existing is not None:
                return existing["_id"]
            raise
        return job_id

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self._doc_to_dict(self._col.find_one({"_id": job_id}))

    def list(
        self,
        *,
        status: JobStatus | None = None,
        tags: list[str] | None = None,
        parent_job_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        from pymongo import ASCENDING, DESCENDING

        query: dict[str, Any] = {}
        if status is not None:
            query["status"] = status.value
        if tags:
            query["tags"] = {"$all": tags}
        if parent_job_id:
            query["request.parent_job_id"] = parent_job_id
        cursor = (
            self._col.find(query)
            .sort([("priority", DESCENDING), ("created_at", ASCENDING)])
            .skip(offset)
            .limit(limit)
        )
        return [d for doc in cursor if (d := self._doc_to_dict(doc)) is not None]

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        *,
        error: str | None = None,
    ) -> None:
        update: dict[str, Any] = {"status": status.value, "updated_at": _now_iso()}
        if error is not None:  # COALESCE semantics: only overwrite when given
            update["error"] = error
        self._col.update_one({"_id": job_id}, {"$set": update})

    def set_result(self, job_id: str, result: JobResult) -> None:
        self._col.update_one(
            {"_id": job_id},
            {
                "$set": {
                    "status": result.status.value,
                    "result": _bson_safe(asdict(result)),
                    "updated_at": _now_iso(),
                    "error": result.error,
                }
            },
        )

    def claim_for_processing(self, job_id: str) -> bool:
        """Atomic QUEUED -> PROCESSING. Server-side via find_one_and_update, so
        concurrent workers (even on different machines) each claim a job once —
        no client-side lock or WAL needed."""
        from pymongo import ReturnDocument

        # return_document=BEFORE is pymongo's default, but pin it explicitly: a
        # successful claim returns the pre-update doc (not None) and a lost race
        # / wrong status returns None, so `doc is not None` is the claim result.
        # Being explicit guards against a future default change inverting this.
        doc = self._col.find_one_and_update(
            {"_id": job_id, "status": JobStatus.QUEUED.value},
            {"$set": {"status": JobStatus.PROCESSING.value, "updated_at": _now_iso()}},
            return_document=ReturnDocument.BEFORE,
        )
        return doc is not None

    def close(self) -> None:
        self._client.close()

    def get_status_counts(self) -> dict[str, int]:
        pipeline = [{"$group": {"_id": "$status", "c": {"$sum": 1}}}]
        return {row["_id"]: row["c"] for row in self._col.aggregate(pipeline)}

    def delete_jobs(
        self,
        *,
        status: JobStatus | None = None,
        before: str | None = None,
    ) -> int:
        query: dict[str, Any] = {}
        if status is not None:
            query["status"] = status.value
        if before is not None:
            query["updated_at"] = {"$lt": before}
        return self._col.delete_many(query).deleted_count

    def delete_job(self, job_id: str) -> bool:
        return self._col.delete_one({"_id": job_id}).deleted_count > 0

    def mark_result_published(self, job_id: str) -> None:
        self._col.update_one({"_id": job_id}, {"$set": {"result_published": True}})

    def list_unpublished_terminal(self, *, limit: int = 100) -> list[dict[str, Any]]:
        from pymongo import ASCENDING

        terminal = [
            JobStatus.COMPLETED.value,
            JobStatus.FAILED.value,
            JobStatus.CANCELLED.value,
        ]
        cursor = (
            self._col.find(
                {
                    "status": {"$in": terminal},
                    "result": {"$ne": None},
                    "result_published": {"$ne": True},
                    "correlation_id": {"$exists": True},
                }
            )
            .sort([("updated_at", ASCENDING)])
            .limit(limit)
        )
        return [d for doc in cursor if (d := self._doc_to_dict(doc)) is not None]


def create_mongo_store(
    uri: str = "mongodb://localhost:27017",
    db_name: str = "hoglah",
    collection: str = "jobs",
) -> MongoJobStore:
    return MongoJobStore(uri, db_name, collection)
