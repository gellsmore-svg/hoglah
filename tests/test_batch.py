"""Batches, depends_on names, cascade cancel, and queue remove."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from hoglah import BatchSubmitResult, Hoglah, JobStatus
from hoglah.batch import prepare_batch_items, topological_names
from hoglah.models import JobRequest
from hoglah.store import create_sqlite_store


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-batch-")
    return Path(td) / "test.db"


def test_topo_rejects_cycle():
    with pytest.raises(ValueError, match="cycle"):
        topological_names(["a", "b"], {"a": ["b"], "b": ["a"]})


def test_prepare_batch_resolves_local_names():
    bid, ordered, ids, specs, deps = prepare_batch_items(
        [
            {"name": "a", "prompt": "one"},
            {"name": "b", "prompt": "two", "depends_on": ["a"]},
        ]
    )
    assert ordered == ["a", "b"]
    assert deps["b"] == [ids["a"]]
    assert specs["a"]["prompt"] == "one"
    assert bid


def test_submit_batch_runs_child_after_parent():
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
    try:
        batch = h.submit_batch(
            [
                {"name": "a", "prompt": "first"},
                {"name": "b", "prompt": "second", "depends_on": ["a"]},
            ],
            model="m",
        )
        assert isinstance(batch, BatchSubmitResult)
        results = h.wait_batch(batch.batch_id, timeout=5.0)
        by_id = {r.job_id: r for r in results}
        assert by_id[batch["a"]].status == JobStatus.COMPLETED
        assert by_id[batch["b"]].status == JobStatus.COMPLETED
        listed = h.list(batch_id=batch.batch_id)
        assert {j.job_id for j in listed} == {batch["a"], batch["b"]}
    finally:
        h.close()


def test_submit_batch_cycle_enqueues_nothing():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    try:
        with pytest.raises(ValueError, match="cycle"):
            h.submit_batch(
                [
                    {"name": "a", "prompt": "x", "depends_on": ["b"]},
                    {"name": "b", "prompt": "y", "depends_on": ["a"]},
                ],
                model="m",
            )
        assert h.stats()["total_jobs"] == 0
    finally:
        h.close()


def test_cancel_cascades_to_dependents():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    try:
        parent = h.submit(prompt="p", model="m")
        child = h.submit(prompt="c", model="m", depends_on=[parent])
        assert h.cancel(parent) is True
        assert h.status(parent) == JobStatus.CANCELLED
        assert h.status(child) == JobStatus.FAILED
        assert parent in (h.get(child).error or "")
    finally:
        h.close()


def test_remove_takes_queued_job_off_the_queue():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    try:
        jid = h.submit(prompt="gone", model="m")
        child = h.submit(prompt="blocked", model="m", depends_on=[jid])
        assert h.remove(jid) is True
        with pytest.raises(KeyError):
            h.status(jid)
        assert h.status(child) == JobStatus.FAILED
    finally:
        h.close()


def test_cancel_batch():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    try:
        batch = h.submit_batch(
            [{"name": "a", "prompt": "x"}, {"name": "b", "prompt": "y"}],
            model="m",
        )
        cancelled = h.cancel_batch(batch.batch_id)
        assert set(cancelled) == {batch["a"], batch["b"]}
        assert h.status(batch["a"]) == JobStatus.CANCELLED
    finally:
        h.close()


def test_store_list_dependents_and_batch_filter(tmp_path: Path):
    store = create_sqlite_store(tmp_path / "jobs.db")
    a = store.enqueue(JobRequest(prompt="a", model="m", batch_id="batch-1"))
    b = store.enqueue(
        JobRequest(prompt="b", model="m", depends_on=[a], batch_id="batch-1")
    )
    store.enqueue(JobRequest(prompt="other", model="m", batch_id="batch-2"))
    assert store.list_dependents(a) == [b]
    ids = {r["id"] for r in store.list(batch_id="batch-1")}
    assert ids == {a, b}
    store.close()
