"""G7 — minimal depends_on execution dependencies."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.models import JobResult


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-deps-")
    return Path(td) / "test.db"


def test_child_waits_for_parent_then_runs():
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=False)
    parent = h.submit(prompt="parent", model="m", max_retries=0)
    child = h.submit(
        prompt="child",
        model="m",
        max_retries=0,
        depends_on=[parent],
    )
    # Parent not done → child stays queued even with a worker.
    h.close()

    h2 = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
    try:
        # Complete parent first by waiting (stub is fast).
        res_p = h2.wait(parent, timeout=5.0)
        assert res_p.status == JobStatus.COMPLETED
        res_c = h2.wait(child, timeout=5.0)
        assert res_c.status == JobStatus.COMPLETED
        assert "[STUB]" in (res_c.output or "")
    finally:
        h2.close()


def test_child_fails_if_parent_fails():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    parent = h.submit(prompt="p", model="m")
    child = h.submit(prompt="c", model="m", depends_on=[parent])
    h._store.set_result(
        parent,
        JobResult(job_id=parent, status=JobStatus.FAILED, error="boom", model="m"),
    )
    h.close()

    h2 = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            if h2.status(child) in (JobStatus.FAILED, JobStatus.COMPLETED):
                break
            time.sleep(0.05)
        res = h2.get(child)
        assert res.status == JobStatus.FAILED
        assert parent in (res.error or "")
        assert "failed" in (res.error or "").lower()
    finally:
        h2.close()


def test_child_fails_if_dependency_missing():
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
    try:
        child = h.submit(
            prompt="c",
            model="m",
            depends_on=["00000000-0000-0000-0000-000000000000"],
            max_retries=0,
        )
        res = h.wait(child, timeout=5.0)
        assert res.status == JobStatus.FAILED
        assert "not found" in (res.error or "").lower()
    finally:
        h.close()


def test_depends_on_persisted_on_request():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    a = h.submit(prompt="a", model="m")
    b = h.submit(prompt="b", model="m", depends_on=[a])
    row = h._store.get(b)
    assert row is not None
    assert row["request"]["depends_on"] == [a]
    h.close()


def test_eval_depends_on_wait_ready_blocked():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    a = h.submit(prompt="a", model="m")
    assert h._eval_depends_on([a]) == ("wait", None)
    h._store.set_result(
        a,
        JobResult(job_id=a, status=JobStatus.COMPLETED, output="ok", model="m"),
    )
    assert h._eval_depends_on([a]) == ("ready", None)
    assert h._eval_depends_on([]) == ("ready", None)
    assert h._eval_depends_on(None) == ("ready", None)
    h.close()
