"""Store filter correctness (review M6) + count (M4)."""

from __future__ import annotations

from pathlib import Path

from hoglah.models import JobRequest, JobStatus
from hoglah.store import create_sqlite_store


def test_tag_filter_is_exact_not_like_wildcard(tmp_path: Path) -> None:
    store = create_sqlite_store(tmp_path / "jobs.db")
    store.enqueue(JobRequest(prompt="underscore", model="m", tags=["a_c"]))
    store.enqueue(JobRequest(prompt="plain", model="m", tags=["abc"]))
    store.enqueue(JobRequest(prompt="percent", model="m", tags=["%"]))

    # Old LIKE '%"a_c"%' matched "abc" because `_` is a single-char wildcard.
    assert [r["request"]["prompt"] for r in store.list(tags=["a_c"])] == ["underscore"]
    assert [r["request"]["prompt"] for r in store.list(tags=["abc"])] == ["plain"]
    # '%' must not match every tag.
    assert [r["request"]["prompt"] for r in store.list(tags=["%"])] == ["percent"]
    store.close()


def test_parent_filter_uses_json_extract(tmp_path: Path) -> None:
    store = create_sqlite_store(tmp_path / "jobs.db")
    store.enqueue(JobRequest(prompt="child", model="m", parent_job_id="parent-1"))
    store.enqueue(JobRequest(prompt="other", model="m", parent_job_id="parent-2"))
    rows = store.list(parent_job_id="parent-1")
    assert len(rows) == 1
    assert rows[0]["request"]["prompt"] == "child"
    store.close()


def test_count_and_unlimited_list(tmp_path: Path) -> None:
    store = create_sqlite_store(tmp_path / "jobs.db")
    for i in range(5):
        store.enqueue(JobRequest(prompt=str(i), model="m"))
    assert store.count(status=JobStatus.QUEUED) == 5
    assert len(store.list(status=JobStatus.QUEUED, limit=None)) == 5
    store.close()
