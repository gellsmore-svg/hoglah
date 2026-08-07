"""G10 — per-model concurrency slots."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.config import HoglahSettings
from hoglah.adapters import StubAdapter


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-slots-")
    return Path(td) / "test.db"


def test_parse_model_slots_from_env_string():
    s = HoglahSettings(model_slots="llama3.1:70b=1,gemma3:1b=2")
    assert s.model_slots == {"llama3.1:70b": 1, "gemma3:1b": 2}

    s2 = HoglahSettings(model_slots={"a": 3})
    assert s2.model_slots == {"a": 3}


def test_big_model_limited_while_small_runs():
    """With concurrency=2 and big=1, only one big job runs at a time; small may proceed."""

    class TrackingStub(StubAdapter):
        current: dict[str, int] = {}
        peak: dict[str, int] = {}
        lock = asyncio.Lock()

        async def run(self, request):
            model = request.model
            async with type(self).lock:
                type(self).current[model] = type(self).current.get(model, 0) + 1
                type(self).peak[model] = max(
                    type(self).peak.get(model, 0), type(self).current[model]
                )
            try:
                await asyncio.sleep(0.35)
                return await super().run(request)
            finally:
                async with type(self).lock:
                    type(self).current[model] -= 1

    TrackingStub.current = {}
    TrackingStub.peak = {}

    db = _temp_db()
    h = Hoglah(
        config={
            "db_path": db,
            "concurrency": 2,
            "model_slots": {"big:70b": 1, "small:1b": 2},
        },
        adapter=TrackingStub(),
        start_worker=True,
    )
    try:
        ids = []
        for _ in range(2):
            ids.append(h.submit(prompt="b", model="big:70b", max_retries=0))
        ids.append(h.submit(prompt="s", model="small:1b", max_retries=0))

        deadline = time.time() + 8
        while time.time() < deadline:
            if all(h.status(j) in (JobStatus.COMPLETED, JobStatus.FAILED) for j in ids):
                break
            time.sleep(0.05)

        for j in ids:
            assert h.status(j) == JobStatus.COMPLETED
        assert TrackingStub.peak.get("big:70b", 0) == 1
        assert TrackingStub.peak.get("small:1b", 0) >= 1
    finally:
        h.close()


def test_unlimited_when_slots_empty():
    db = _temp_db()
    h = Hoglah(
        config={"db_path": db, "concurrency": 2, "model_slots": {}},
        start_worker=False,
    )
    assert h._slot_limit_for_model("any") is None
    assert h._try_reserve_model_slot("j1", "any") is True
    assert h._try_reserve_model_slot("j2", "any") is True
    h.close()


def test_default_model_slots_applies_to_unlisted():
    db = _temp_db()
    h = Hoglah(
        config={
            "db_path": db,
            "model_slots": {"special": 2},
            "default_model_slots": 1,
        },
        start_worker=False,
    )
    assert h._slot_limit_for_model("special") == 2
    assert h._slot_limit_for_model("other") == 1
    assert h._try_reserve_model_slot("a", "other") is True
    assert h._try_reserve_model_slot("b", "other") is False
    h.close()
