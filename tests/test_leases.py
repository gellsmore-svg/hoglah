"""G3 — PROCESSING lease + heartbeat + stale reclaim."""

from __future__ import annotations

import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.models import JobRequest, JobResult
from hoglah.store import SQLiteJobStore, _lease_expiry_iso


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-lease-")
    return Path(td) / "test.db"


def test_claim_sets_lease_token_and_expiry():
    store = SQLiteJobStore(_temp_db())
    jid = store.enqueue(JobRequest(prompt="hi", model="m"))
    token = store.claim_for_processing(jid, lease_seconds=30)
    assert token is not None
    assert len(token) >= 16
    row = store.get(jid)
    assert row["status"] == JobStatus.PROCESSING.value
    assert row["lease_token"] == token
    assert row["lease_expires_at"] is not None
    # Second claim loses.
    assert store.claim_for_processing(jid) is None
    store.close()


def test_heartbeat_extends_lease_and_requires_token():
    store = SQLiteJobStore(_temp_db())
    jid = store.enqueue(JobRequest(prompt="hi", model="m"))
    token = store.claim_for_processing(jid, lease_seconds=30)
    assert token is not None
    before = store.get(jid)["lease_expires_at"]

    time.sleep(0.05)
    assert store.heartbeat(jid, token, lease_seconds=60) is True
    after = store.get(jid)["lease_expires_at"]
    assert after >= before

    assert store.heartbeat(jid, "wrong-token", lease_seconds=60) is False
    store.close()


def test_reclaim_only_stale_leases():
    store = SQLiteJobStore(_temp_db())
    live = store.enqueue(JobRequest(prompt="live", model="m"))
    dead = store.enqueue(JobRequest(prompt="dead", model="m"))
    legacy = store.enqueue(JobRequest(prompt="legacy", model="m"))

    live_tok = store.claim_for_processing(live, lease_seconds=120)
    dead_tok = store.claim_for_processing(dead, lease_seconds=30)
    assert live_tok and dead_tok

    # Force dead's lease into the past.
    past = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
    with store._lock:
        store._conn.execute(
            "UPDATE jobs SET lease_expires_at = ? WHERE id = ?",
            (past, dead),
        )
        store._conn.commit()

    # Legacy: PROCESSING with no lease fields (pre-G3 / missing write).
    store.update_status(legacy, JobStatus.PROCESSING)
    with store._lock:
        store._conn.execute(
            "UPDATE jobs SET lease_expires_at = NULL, lease_token = NULL WHERE id = ?",
            (legacy,),
        )
        store._conn.commit()

    requeued = store.reclaim_stale_leases(limit=50)
    assert dead in requeued
    assert legacy in requeued
    assert live not in requeued

    assert store.get(dead)["status"] == JobStatus.QUEUED.value
    assert store.get(legacy)["status"] == JobStatus.QUEUED.value
    assert store.get(live)["status"] == JobStatus.PROCESSING.value
    store.close()


def test_set_result_token_guard_prevents_lost_lease_write():
    store = SQLiteJobStore(_temp_db())
    jid = store.enqueue(JobRequest(prompt="race", model="m"))
    token = store.claim_for_processing(jid, lease_seconds=5)
    assert token is not None

    # Simulate reclaim by another worker.
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    with store._lock:
        store._conn.execute(
            "UPDATE jobs SET lease_expires_at = ? WHERE id = ?",
            (past, jid),
        )
        store._conn.commit()
    assert jid in store.reclaim_stale_leases()

    # Dead worker tries to complete — must not clobber the requeued job.
    ok = store.set_result(
        jid,
        JobResult(job_id=jid, status=JobStatus.COMPLETED, output="too late"),
        lease_token=token,
    )
    assert ok is False
    row = store.get(jid)
    assert row["status"] == JobStatus.QUEUED.value
    assert row.get("result") is None

    # Cancel path (no token) still works after a fresh claim.
    token2 = store.claim_for_processing(jid, lease_seconds=30)
    assert token2 is not None
    store.update_status(jid, JobStatus.CANCELLED)
    assert store.set_result(
        jid,
        JobResult(job_id=jid, status=JobStatus.CANCELLED, error="Cancelled by user"),
    ) is True
    assert store.get(jid)["status"] == JobStatus.CANCELLED.value
    store.close()


def test_worker_recovery_only_reclaims_stale():
    """ADR-016 + G3: live leases survive; expired ones requeue on recover."""
    db = _temp_db()
    h = Hoglah(
        config={"db_path": db, "lease_seconds": 30, "heartbeat_interval_seconds": 5},
        start_worker=False,
    )
    live = h.submit(prompt="live", model="m")
    stale = h.submit(prompt="stale", model="m")

    live_tok = h._store.claim_for_processing(live, lease_seconds=120)
    stale_tok = h._store.claim_for_processing(stale, lease_seconds=30)
    assert live_tok and stale_tok

    past = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
    with h._store._lock:
        h._store._conn.execute(
            "UPDATE jobs SET lease_expires_at = ? WHERE id = ?",
            (past, stale),
        )
        h._store._conn.commit()

    h._recover_interrupted_jobs()
    assert h.status(live) == JobStatus.PROCESSING
    assert h.status(stale) == JobStatus.QUEUED
    h.close()


def test_worker_heartbeat_keeps_lease_through_long_job():
    """Short lease + active heartbeat → job still completes without reclaim."""
    db = _temp_db()

    class SlowStub:
        """Minimal async adapter that takes longer than one lease window."""

        async def run(self, request):
            import asyncio

            await asyncio.sleep(0.8)
            return "[STUB] slow", {"prompt_tokens": 1, "completion_tokens": 1}, {}

        async def embed(self, request):
            return [0.1, 0.2], {"prompt_tokens": 1}, {"embedding_dim": 2}

        async def list_models(self):
            return [{"name": "stub-model"}]

    h = Hoglah(
        config={
            "db_path": db,
            "concurrency": 1,
            "lease_seconds": 5.0,
            "heartbeat_interval_seconds": 0.2,
        },
        adapter=SlowStub(),  # type: ignore[arg-type]
        start_worker=True,
    )
    try:
        jid = h.submit(prompt="slow", model="m", max_retries=0)
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        assert "STUB" in (res.output or "")
    finally:
        h.close()


def test_lease_expiry_helper():
    now = datetime(2026, 8, 7, 12, 0, 0, tzinfo=timezone.utc)
    iso = _lease_expiry_iso(30, now=now)
    assert iso == (now + timedelta(seconds=30)).isoformat()
