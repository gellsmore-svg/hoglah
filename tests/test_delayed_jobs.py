"""G1 — delayed / scheduled enqueue.

Jobs stay QUEUED until ``run_at`` is due. The worker poll uses ``due_only``;
``claim_for_processing`` refuses future jobs as a second guard.
"""

from __future__ import annotations

import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.models import JobRequest, resolve_run_at
from hoglah.store import SQLiteJobStore


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-delay-")
    return Path(td) / "test.db"


def test_resolve_run_at_delay_and_mutual_exclusion():
    now = datetime(2026, 8, 7, 12, 0, 0, tzinfo=timezone.utc)
    iso = resolve_run_at(delay_seconds=30, now=now)
    assert iso == (now + timedelta(seconds=30)).isoformat()

    past = resolve_run_at(run_at=now - timedelta(hours=1))
    assert past is not None
    assert past.endswith("+00:00") or past.endswith("Z") or "+00:00" in past

    with pytest.raises(ValueError, match="only one"):
        resolve_run_at(run_at=now, delay_seconds=5)

    with pytest.raises(ValueError, match=">= 0"):
        resolve_run_at(delay_seconds=-1)

    with pytest.raises(ValueError, match="ISO-8601"):
        resolve_run_at(run_at="not-a-date")


def test_resolve_run_at_naive_datetime_treated_as_utc():
    naive = datetime(2026, 1, 1, 0, 0, 0)
    iso = resolve_run_at(run_at=naive)
    assert iso is not None
    assert "+00:00" in iso


def test_submit_delay_persists_run_at_and_stays_queued():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    job_id = h.submit(
        prompt="later",
        model="gemma:2b",
        delay_seconds=3600,
    )
    row = h._store.get(job_id)
    assert row is not None
    assert row["status"] == JobStatus.QUEUED.value
    assert row["run_at"] is not None
    assert row["request"]["run_at"] == row["run_at"]

    # Not due yet → due_only list excludes it; claim fails.
    due = h._store.list(status=JobStatus.QUEUED, due_only=True)
    assert all(r["id"] != job_id for r in due)
    assert h._store.claim_for_processing(job_id) is None

    # Full list still shows it as queued (inspectable, not hidden).
    all_q = h._store.list(status=JobStatus.QUEUED)
    assert any(r["id"] == job_id for r in all_q)

    h.close()


def test_submit_run_at_past_is_immediately_claimable():
    db = _temp_db()
    store = SQLiteJobStore(db)
    past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    jid = store.enqueue(JobRequest(prompt="now", model="m", run_at=past))
    assert store.claim_for_processing(jid) is not None
    store.close()


def test_submit_run_at_future_not_claimed_until_due():
    db = _temp_db()
    store = SQLiteJobStore(db)
    # Tiny delay so we can wait it out without flaking.
    future = (datetime.now(timezone.utc) + timedelta(seconds=0.4)).isoformat()
    jid = store.enqueue(JobRequest(prompt="soon", model="m", run_at=future))

    assert store.claim_for_processing(jid) is None
    assert store.list(status=JobStatus.QUEUED, due_only=True) == []

    time.sleep(0.5)

    due = store.list(status=JobStatus.QUEUED, due_only=True)
    assert len(due) == 1 and due[0]["id"] == jid
    assert store.claim_for_processing(jid) is not None
    store.close()


def test_worker_respects_delay_then_runs():
    """End-to-end: delayed job is not completed early; completes after due."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)

    try:
        job_id = h.submit(
            prompt="delayed worker",
            model="gemma:2b",
            delay_seconds=0.6,
            max_retries=0,
        )
        # Shortly after submit, still queued.
        time.sleep(0.2)
        assert h.status(job_id) == JobStatus.QUEUED

        # After the delay window + poll slack, stub worker should finish.
        res = h.wait(job_id, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        assert "[STUB]" in (res.output or "")
    finally:
        h.close()


def test_immediate_submit_unchanged():
    """No schedule args → run_at null → claimable immediately (regression)."""
    db = _temp_db()
    store = SQLiteJobStore(db)
    jid = store.enqueue(JobRequest(prompt="asap", model="m"))
    row = store.get(jid)
    assert row is not None
    assert row.get("run_at") is None
    assert store.claim_for_processing(jid) is not None
    store.close()
