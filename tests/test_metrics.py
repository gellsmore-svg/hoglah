"""G11 — Prometheus metrics exposition."""

from __future__ import annotations

import tempfile
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.metrics import MetricsRegistry, REGISTRY


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-metrics-")
    return Path(td) / "test.db"


def test_registry_render_basic():
    reg = MetricsRegistry()
    reg.inc("hoglah_jobs_submitted_total")
    reg.inc("hoglah_jobs_terminal_total", status="completed")
    reg.observe_seconds(0.5)
    reg.observe_seconds(1.5)
    text = reg.render({"counts": {"queued": 2, "completed": 1}, "total_jobs": 3})
    assert "hoglah_jobs{status=\"queued\"} 2" in text
    assert "hoglah_jobs_submitted_total 1" in text
    assert 'hoglah_jobs_terminal_total{status="completed"} 1' in text
    assert "hoglah_job_duration_seconds_count 2" in text
    assert "hoglah_process_uptime_seconds" in text


def test_client_metrics_reflect_submit_and_complete():
    db = _temp_db()
    # Isolate process counters somewhat by reading deltas via a fresh client path
    before = REGISTRY._counter_value("hoglah_jobs_submitted_total")
    h = Hoglah(config={"db_path": db, "concurrency": 1}, start_worker=True)
    try:
        jid = h.submit(prompt="hi", model="m", max_retries=0)
        res = h.wait(jid, timeout=5.0)
        assert res.status == JobStatus.COMPLETED
        text = h.metrics_text()
        assert "hoglah_jobs{" in text
        assert "hoglah_jobs_submitted_total" in text
        assert REGISTRY._counter_value("hoglah_jobs_submitted_total") >= before + 1
        assert 'hoglah_jobs_terminal_total{status="completed"}' in text or (
            REGISTRY._counter_value("hoglah_jobs_terminal_total", status="completed") >= 1
        )
    finally:
        h.close()


def test_web_metrics_endpoint():
    try:
        from fastapi.testclient import TestClient
        from hoglah.web import create_app
    except ImportError:
        import pytest

        pytest.skip("web extra not installed")

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    h.submit(prompt="x", model="m")
    h.close()

    app = create_app(db_path=db)
    with TestClient(app) as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "hoglah_jobs" in r.text
        assert "text/plain" in r.headers.get("content-type", "")
