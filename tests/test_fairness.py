"""G5 — session/tag slots and token-bucket rate limits."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.adapters import StubAdapter
from hoglah.client import _TokenBucket
from hoglah.config import HoglahSettings


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-fair-")
    return Path(td) / "test.db"


def test_parse_tag_slots_and_rates():
    s = HoglahSettings(
        tag_slots="agent-a=1,agent-b=2",
        tag_rates_per_minute="agent-a=6,agent-b=12",
        session_slots=1,
        session_rate_per_minute=30,
    )
    assert s.tag_slots == {"agent-a": 1, "agent-b": 2}
    assert s.tag_rates_per_minute == {"agent-a": 6.0, "agent-b": 12.0}
    assert s.session_slots == 1
    assert s.session_rate_per_minute == 30


def test_token_bucket_refills():
    b = _TokenBucket(rate_per_minute=60)  # 1 token/sec
    assert b.try_take() is True
    # Drain capacity
    while b.try_take():
        pass
    assert b.try_take() is False
    time.sleep(0.05)
    # Should get a fractional refill toward 1 — wait enough for a token
    time.sleep(1.05)
    assert b.would_allow() is True


def test_session_slots_serialize_agents():
    """session_slots=1 → at most one job per session runs at a time."""

    class Tracking(StubAdapter):
        current: dict[str, int] = {}
        peak: dict[str, int] = {}

        async def run(self, request):
            sid = (request.metadata or {}).get("session_id", "?")
            type(self).current[sid] = type(self).current.get(sid, 0) + 1
            type(self).peak[sid] = max(
                type(self).peak.get(sid, 0), type(self).current[sid]
            )
            try:
                await asyncio.sleep(0.3)
                return await super().run(request)
            finally:
                type(self).current[sid] -= 1

    Tracking.current = {}
    Tracking.peak = {}

    db = _temp_db()
    h = Hoglah(
        config={
            "db_path": db,
            "concurrency": 4,
            "session_slots": 1,
        },
        adapter=Tracking(),
        start_worker=True,
    )
    try:
        ids = []
        for sid in ("s1", "s1", "s2", "s2"):
            ids.append(
                h.submit(
                    prompt="x",
                    model="m",
                    max_retries=0,
                    metadata={"session_id": sid},
                )
            )
        deadline = time.time() + 8
        while time.time() < deadline:
            if all(h.status(j) == JobStatus.COMPLETED for j in ids):
                break
            time.sleep(0.05)
        for j in ids:
            assert h.status(j) == JobStatus.COMPLETED
        assert Tracking.peak.get("s1", 0) == 1
        assert Tracking.peak.get("s2", 0) == 1
    finally:
        h.close()


def test_tag_slots_limit():
    db = _temp_db()
    h = Hoglah(
        config={"db_path": db, "tag_slots": {"vip": 1}, "concurrency": 3},
        start_worker=False,
    )
    row = {
        "request": {"model": "m", "metadata": {}, "tags": ["vip"]},
        "tags": ["vip"],
    }
    assert h._try_reserve_job("a", row) is True
    assert h._try_reserve_job("b", row) is False
    h._release_job_reservation("a")
    assert h._try_reserve_job("b", row) is True
    h.close()


def test_session_rate_limit_blocks_burst():
    db = _temp_db()
    h = Hoglah(
        config={
            "db_path": db,
            "session_rate_per_minute": 30,  # 0.5/sec, capacity ~5
            "concurrency": 10,
        },
        start_worker=False,
    )
    # Drain the bucket with many reserves for same session.
    allowed = 0
    for i in range(20):
        row = {
            "request": {
                "model": "m",
                "metadata": {"session_id": "burst"},
                "tags": [],
            }
        }
        if h._try_reserve_job(f"j{i}", row):
            allowed += 1
            h._release_job_reservation(f"j{i}")
        else:
            break
    # Capacity is max(1, 30/6)=5, so at most ~5 immediate starts.
    assert 1 <= allowed <= 6
    h.close()
