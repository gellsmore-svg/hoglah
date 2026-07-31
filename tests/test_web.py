"""Web queue monitor (hoglah.web) — read-only dashboard over the JobStore."""

from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi", reason="web extra not installed")

from fastapi.testclient import TestClient  # noqa: E402

from hoglah.client import Hoglah  # noqa: E402
from hoglah.web import create_app  # noqa: E402


@pytest.fixture()
def seeded_db(tmp_path):
    """A store with two completed stub jobs (worker runs them synchronously fast)."""
    db_path = tmp_path / "queue.db"
    h = Hoglah(config={"db_path": db_path})
    first = h.submit(prompt="hello world", model="stub:1", step_name="greet")
    second = h.submit(prompt="second job", model="stub:2", tags=["batch"])
    h.wait(first, timeout=30)
    h.wait(second, timeout=30)
    h.close()
    return db_path


@pytest.fixture()
def client(seeded_db):
    # `with` so the lifespan runs — startup on enter, store closed on exit.
    with TestClient(create_app(db_path=seeded_db)) as test_client:
        yield test_client


def test_lifespan_closes_the_client_store_on_shutdown(seeded_db):
    """The monitor must not leak its SQLite handle on shutdown/reload (#12)."""
    import sqlite3

    app = create_app(db_path=seeded_db)
    client = app.state.client

    with TestClient(app):
        assert client.stats()["total_jobs"] == 2  # store is live while serving

    # Shutdown ran the lifespan, which closed the client and its store.
    with pytest.raises(sqlite3.ProgrammingError):
        client.stats()


def test_dashboard_renders_counts_and_jobs(client) -> None:
    r = client.get("/")
    assert r.status_code == 200
    assert "Queue" in r.text
    assert "completed" in r.text
    assert "stub:1" in r.text


def test_dashboard_status_filter(client) -> None:
    assert client.get("/?status=completed").status_code == 200
    assert client.get("/?status=bogus").status_code == 400


def test_api_summary_counts(client) -> None:
    data = client.get("/api/summary").json()
    assert data["ok"] is True
    assert data["stats"]["total_jobs"] == 2
    assert len(data["jobs"]) == 2
    assert all(j["status"] == "completed" for j in data["jobs"])


def test_api_job_and_detail_page(client) -> None:
    job_id = client.get("/api/summary").json()["jobs"][0]["job_id"]
    assert client.get(f"/api/jobs/{job_id}").json()["job"]["job_id"] == job_id
    page = client.get(f"/jobs/{job_id}")
    assert page.status_code == 200
    assert job_id[:8] in page.text


def test_detail_page_shows_full_input_step_and_trace_hint(client) -> None:
    """The queue detail doubles as a debug view: full In→Out + step + galeed link."""
    calls = client.get("/api/summary").json()["jobs"]
    greet = next(j for j in calls if j.get("step_name") == "greet")
    page = client.get(f"/jobs/{greet['job_id']}").text
    assert "hello world" in page          # full input
    assert "greet" in page                # step row
    assert "galeed trace --call" in page  # cross-family pointer


def test_dashboard_has_step_column(client) -> None:
    page = client.get("/").text
    assert "<th>Step</th>" in page
    assert "greet" in page


def test_missing_job_is_404(client) -> None:
    assert client.get("/jobs/nope").status_code == 404
    assert client.get("/api/jobs/nope").status_code == 404
