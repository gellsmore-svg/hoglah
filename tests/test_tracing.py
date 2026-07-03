"""Galeed job-lifecycle tracing (hoglah.tracing) — optional, best-effort spine witness."""

from __future__ import annotations

import pytest

pytest.importorskip("galeed", reason="galeed extra not installed")

from hoglah.client import Hoglah  # noqa: E402
from hoglah.tracing import JOB_CANCELLED, JOB_COMPLETED, JOB_QUEUED, JOB_STARTED, JobWitness  # noqa: E402


class FakeCollection:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def insert_one(self, doc: dict) -> None:
        self.rows.append(doc)


class FakeDb:
    """Minimal mapping-style db: db['trace_events'].insert_one(...)."""

    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def __getitem__(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())


@pytest.fixture()
def traced_client(tmp_path):
    """A Hoglah with tracing enabled and the trace db replaced by a fake."""
    h = Hoglah(config={"db_path": tmp_path / "queue.db", "galeed_enabled": True})
    fake = FakeDb()
    h._witness = JobWitness(h.config, db=fake)
    yield h, fake
    h.close()


def _events(fake: FakeDb) -> list[dict]:
    rows = fake.collections.get("trace_events", FakeCollection()).rows
    return sorted(rows, key=lambda r: (r.get("timestamp") or "", r.get("seq") or 0))


def test_job_lifecycle_emits_queued_started_completed(traced_client) -> None:
    h, fake = traced_client
    job_id = h.submit(prompt="hello", model="stub:1", metadata={"session_id": "sess-42"})
    h.wait(job_id, timeout=30)

    types = [e["type"] for e in _events(fake)]
    assert types == [JOB_QUEUED, JOB_STARTED, JOB_COMPLETED]
    for event in _events(fake):
        assert event["trace_id"] == job_id  # one job == one trace
        assert event["metadata"]["job_id"] == job_id  # correlation key mirrored
        assert event["source"] == "hoglah"
        assert event["session_id"] == "sess-42"  # caller session propagated


def test_cancel_emits_cancelled(traced_client) -> None:
    h, fake = traced_client
    # Submit many jobs so at least the last is still queued when we cancel it.
    job_ids = [h.submit(prompt=f"job {i}", model="stub:1") for i in range(5)]
    cancelled = h.cancel(job_ids[-1])
    if cancelled:  # the worker may already have finished it (timing)
        types = [e["type"] for e in _events(fake) if e["trace_id"] == job_ids[-1]]
        assert JOB_CANCELLED in types


def test_disabled_witness_emits_nothing(tmp_path) -> None:
    h = Hoglah(config={"db_path": tmp_path / "queue.db"})  # galeed_enabled defaults False
    fake = FakeDb()
    h._witness = JobWitness(h.config, db=fake)
    job_id = h.submit(prompt="quiet", model="stub:1")
    h.wait(job_id, timeout=30)
    h.close()
    assert _events(fake) == []


def test_witness_never_raises_on_broken_db(tmp_path) -> None:
    class ExplodingDb:
        def __getitem__(self, name):
            raise RuntimeError("no db for you")

    h = Hoglah(config={"db_path": tmp_path / "queue.db", "galeed_enabled": True})
    h._witness = JobWitness(h.config, db=ExplodingDb())
    job_id = h.submit(prompt="resilient", model="stub:1")
    result = h.wait(job_id, timeout=30)
    h.close()
    assert result.status.value == "completed"  # queue unaffected by tracing failure
