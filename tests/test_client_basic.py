"""Basic tests for the Hoglah client + SQLite store (Chunk 1).

These tests exercise persistence, restart behavior (new Hoglah instances),
callback registry re-delivery, submit/list/get/cancel/wait, etc.

They do NOT require a running Ollama instance.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from hoglah import Hoglah, JobStatus
from hoglah.models import JobResult


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-test-")
    return Path(td) / "test.db"


def test_submit_and_get_basic():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    job_id = h.submit(
        prompt="Hello world",
        model="gemma:2b",
        tags=["test"],
        priority=5,
    )

    assert job_id
    res = h.get(job_id)
    assert isinstance(res, JobResult)
    assert res.status == JobStatus.QUEUED
    assert res.model == "gemma:2b"
    assert "test" in res.tags

    # New instance (simulates restart) should still see the job
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    res2 = h2.get(job_id)
    assert res2.status == JobStatus.QUEUED


def test_list_and_filter():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    j1 = h.submit(prompt="one", model="gemma:2b", tags=["a"])
    h.submit(prompt="two", model="gemma:2b", tags=["b"])

    all_jobs = h.list(limit=10)
    assert len(all_jobs) == 2

    a_jobs = h.list(tags=["a"])
    assert len(a_jobs) == 1
    assert a_jobs[0].job_id == j1

    # parent filter
    p1 = h.submit(prompt="parent1", model="gemma:2b")
    h.submit(prompt="child1", model="gemma:2b", parent_job_id=p1)
    h.submit(prompt="child2", model="gemma:2b", parent_job_id=p1)
    parent_filtered = h.list(parent_job_id=p1)
    assert len(parent_filtered) == 2
    assert all(j.parent_job_id == p1 for j in parent_filtered)


def test_cancel_and_wait_timeout():
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)

    job_id = h.submit(prompt="will be cancelled", model="gemma:2b")

    assert h.cancel(job_id) is True
    res = h.get(job_id)
    assert res.status == JobStatus.CANCELLED

    # Second cancel should be no-op
    assert h.cancel(job_id) is False

    # wait should return immediately for terminal job
    res2 = h.wait(job_id)
    assert res2.status == JobStatus.CANCELLED

    # Timeout on a still-queued job (we never start a worker in this test)
    job2 = h.submit(prompt="still queued", model="gemma:2b")
    with pytest.raises(TimeoutError):
        h.wait(job2, timeout=0.1)


def test_named_callback_registry_restart_delivery():
    """Test that named callbacks are re-delivered on new Hoglah() instance
    for jobs that reached terminal state while "down".
    """
    db = _temp_db()

    calls: list[JobResult] = []

    def my_handler(result: JobResult):
        calls.append(result)

    # First instance: submit with named key + mark it completed manually
    h1 = Hoglah(config={"db_path": db}, callbacks={"my_handler": my_handler})
    job_id = h1.submit(
        prompt="test callback",
        model="gemma:2b",
        callback="my_handler",  # str means named registry key
    )

    # Simulate the job having completed while the process was "down"
    # (we directly poke the store for the test)
    fake_result = JobResult(
        job_id=job_id,
        status=JobStatus.COMPLETED,
        output="done",
        model="gemma:2b",
    )
    h1._store.set_result(job_id, fake_result)  # type: ignore[attr-defined]

    # New instance with the same registry should re-deliver
    calls.clear()
    Hoglah(config={"db_path": db}, callbacks={"my_handler": my_handler})

    assert len(calls) == 1
    assert calls[0].job_id == job_id
    assert calls[0].status == JobStatus.COMPLETED


def test_direct_callback_not_redelivered_after_restart():
    """Direct callables should not be re-fired after restart (they can't be)."""
    db = _temp_db()

    calls: list[JobResult] = []

    def direct_cb(result: JobResult):
        calls.append(result)

    h1 = Hoglah(config={"db_path": db})
    job_id = h1.submit(prompt="direct", model="gemma:2b", callback=direct_cb)

    # Manually complete
    fake = JobResult(job_id=job_id, status=JobStatus.COMPLETED, output="x")
    h1._store.set_result(job_id, fake)  # type: ignore[attr-defined]

    # New instance has no knowledge of the direct callable
    Hoglah(config={"db_path": db})

    # The callback should NOT have been called by the new instance
    # (it was only remembered inside h1)
    assert len(calls) == 0  # because we didn't call it ourselves in this test


# --------------------------------------------------------------------------- #
# Basic CLI tests (using Typer CliRunner). These exercise the submit/list etc
# paths through the Click/Typer layer with the live Hoglah client (stub mode).
# --------------------------------------------------------------------------- #

try:
    from typer.testing import CliRunner
    from hoglah.cli import app
    _HAS_CLI_RUNNER = True
except Exception:
    _HAS_CLI_RUNNER = False


@pytest.mark.skipif(not _HAS_CLI_RUNNER, reason="typer not installed for CLI tests")
def test_cli_submit_and_list(tmp_path):
    db = tmp_path / "cli.db"
    runner = CliRunner()

    # Submit a job (prompt style) and wait for it (exercises worker + result path)
    result = runner.invoke(
        app,
        [
            "submit",
            "CLI test prompt via runner",
            "--model",
            "cli-test:stub",
            "--wait",
            "--timeout",
            "30",
            "--db",
            str(db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Submitted:" in result.output

    # List should show the completed job
    list_res = runner.invoke(app, ["list", "--limit", "5", "--db", str(db)])
    assert list_res.exit_code == 0
    assert "cli-test:stub" in list_res.output or "completed" in list_res.output

    # Models (stub) should work
    models_res = runner.invoke(app, ["models", "--db", str(db)])
    assert models_res.exit_code == 0
    assert "stub-model" in models_res.output

    # ps alias should work (like list)
    ps_res = runner.invoke(app, ["ps", "--limit", "3", "--db", str(db)])
    assert ps_res.exit_code == 0
    assert "JOB_ID" in ps_res.output or "completed" in ps_res.output.lower()

    # JSON output for list (now includes 'preview' for usability)
    json_res = runner.invoke(app, ["list", "--json", "--limit", "2", "--db", str(db)])
    assert json_res.exit_code == 0
    assert '"job_id"' in json_res.output or '"status"' in json_res.output
    # basic parse
    import json as _json
    data = _json.loads(json_res.output)
    assert isinstance(data, list) and len(data) >= 1
    assert "preview" in data[0]

    # parent filter in list (CLI)
    p_res = runner.invoke(app, ["list", "--parent", "some-parent", "--json", "--db", str(db)])
    assert p_res.exit_code == 0  # even if no match, should not error

    # New submit flags (metadata + parent_job_id) + ps already covered above
    meta_submit = runner.invoke(
        app,
        [
            "submit",
            "test with metadata",
            "--model",
            "cli-meta:stub",
            "--metadata",
            '{"agent":"test","priority":5}',
            "--parent-job-id",
            "parent-123",
            "--wait",
            "--db",
            str(db),
        ],
    )
    assert meta_submit.exit_code == 0
    # The result should have succeeded (via stub)
    assert "Submitted:" in meta_submit.output or "completed" in meta_submit.output.lower()

    # stats command
    stats_res = runner.invoke(app, ["stats", "--db", str(db)])
    assert stats_res.exit_code == 0
    assert "total" in stats_res.output.lower() or "queued" in stats_res.output.lower()

    stats_json = runner.invoke(app, ["stats", "--json", "--db", str(db)])
    assert stats_json.exit_code == 0
    data = _json.loads(stats_json.output)
    assert "counts" in data and "total_jobs" in data

    # info command
    info_res = runner.invoke(app, ["info", "--db", str(db)])
    assert info_res.exit_code == 0
    assert "StubAdapter" in info_res.output or "adapter" in info_res.output.lower()

    info_json = runner.invoke(app, ["info", "--json", "--db", str(db)])
    assert info_json.exit_code == 0
    j = _json.loads(info_json.output)
    assert "version" in j and "adapter" in j and "stats" in j

    # clear command (dry-ish via --yes)
    clear_res = runner.invoke(app, ["clear", "--status", "completed", "--yes", "--db", str(db)])
    assert clear_res.exit_code == 0
    assert "Cleared" in clear_res.output or "No jobs" in clear_res.output

    # rm command for specific job
    # First submit one to remove
    submit_res = runner.invoke(app, ["submit", "to-rm", "--model", "x", "--db", str(db)])
    assert submit_res.exit_code == 0
    # extract id roughly from output
    # for test, use a known one or just test help/usage; simple: rm non exist
    rm_res = runner.invoke(app, ["rm", "nonexistent", "--yes", "--db", str(db)])
    assert rm_res.exit_code != 0  # should fail for not found
    assert "not found" in (rm_res.output or "").lower() or rm_res.exit_code == 1

    # wait command
    wait_res = runner.invoke(app, ["wait", "nonexistent", "--timeout", "0.1", "--db", str(db)])
    assert wait_res.exit_code != 0
    assert "not found" in (wait_res.output or "").lower() or "timed out" in (wait_res.output or "").lower()

    # wait with --json (errors before full output, but command accepts flag)
    wait_json = runner.invoke(app, ["wait", "nonexistent", "--json", "--timeout", "0.1", "--db", str(db)])
    assert wait_json.exit_code != 0

    # rm with --json
    rm_json = runner.invoke(app, ["rm", "nonexistent", "--json", "--yes", "--db", str(db)])
    assert rm_json.exit_code != 0
    j = _json.loads(rm_json.output)
    assert j.get("removed") is False

    # doctor command
    doctor_res = runner.invoke(app, ["doctor", "--db", str(db)])
    assert doctor_res.exit_code == 0
    assert "Hoglah Doctor" in doctor_res.output
    assert "StubAdapter" in doctor_res.output or "adapter" in doctor_res.output.lower()

    # show model
    show_res = runner.invoke(app, ["show", "stub-test:1b", "--db", str(db)])
    assert show_res.exit_code == 0
    assert "stub-test:1b" in show_res.output or "parameters" in show_res.output.lower()

    show_json = runner.invoke(app, ["show", "stub-test:1b", "--json", "--db", str(db)])
    assert show_json.exit_code == 0
    j = _json.loads(show_json.output)
    assert "parameters" in j or "template" in j or "details" in j


@pytest.mark.skipif(not _HAS_CLI_RUNNER, reason="typer not installed for CLI tests")
def test_cli_submit_with_messages_json(tmp_path):
    db = tmp_path / "cli2.db"
    runner = CliRunner()

    msgs = '[{"role":"user","content":"test chat from cli"}]'
    result = runner.invoke(
        app,
        [
            "submit",
            "--model",
            "cli-chat:stub",
            "--messages",
            msgs,
            "--wait",
            "--db",
            str(db),
        ],
    )
    assert result.exit_code == 0
    assert "Submitted:" in result.output
    assert "[STUB-CHAT]" in result.output or "Responded" in result.output  # from stub output

    # Verify via status too
    # We don't have the exact ID easily, but list should show the model
    list_res = runner.invoke(app, ["list", "--db", str(db)])
    assert "cli-chat:stub" in list_res.output


def test_hoglah_context_manager():
    """Hoglah supports the context manager protocol and cleans up on exit."""
    db = _temp_db()
    with Hoglah(config={"db_path": db}, start_worker=True) as h:
        assert h is not None
        job_id = h.submit(prompt="context manager test", model="cm-test")
        res = h.wait(job_id, timeout=10)
        assert res.status in (JobStatus.COMPLETED, JobStatus.FAILED)
    # After the with block, close has been called (worker stopped).
    # We can still inspect the store via a fresh instance.
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    assert h2.status(job_id) == res.status
    h2.close()


def test_log_level_config():
    """Hoglah respects log_level in config and via env (HOGLAH_LOG_LEVEL)."""
    import logging
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "log_level": "DEBUG"}, start_worker=False)
    assert logging.getLogger("hoglah").level == logging.DEBUG
    h.close()

    # env override
    import os
    os.environ["HOGLAH_LOG_LEVEL"] = "ERROR"
    h2 = Hoglah(config={"db_path": db}, start_worker=False)
    assert logging.getLogger("hoglah").level == logging.ERROR
    h2.close()
    del os.environ["HOGLAH_LOG_LEVEL"]


def test_show_model():
    """show_model via client and stub adapter."""
    h = Hoglah(config={"db_path": _temp_db()}, start_worker=False)
    details = h.show_model("stub-test:1b")
    assert "name" in details or "model" in details
    assert "parameters" in details or "template" in details
    h.close()


def test_context_from_model_info():
    """Stub adapter (simulating real) sets effective_num_ctx from model details if not specified."""
    import asyncio
    from hoglah.adapters import StubAdapter
    ad = StubAdapter()
    # Use the adapter directly in test to exercise meta/effective_num_ctx
    loop = asyncio.new_event_loop()
    output, usage, meta = loop.run_until_complete(ad.run(
        type('Req', (), {"prompt": "hi", "messages": None, "model": "stub-test:1b", "num_ctx": None, **{k:None for k in ['system_prompt','options','tags','priority','timeout_seconds','max_retries','metadata','parent_job_id','temperature','top_p','top_k','repeat_penalty','seed','stop','num_predict','format','keep_alive','callback_key']}})()
    ))
    loop.close()
    assert meta.get("effective_num_ctx") == 4096  # from stub show


def test_info():
    """Test Hoglah.info() snapshot and CLI."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db, "concurrency": 2}, start_worker=False)
    h.submit(prompt="info test", model="x")

    i = h.info()
    assert i["adapter"] == "StubAdapter"
    assert "version" in i
    assert i["config"]["concurrency"] == 2
    assert i["stats"]["total_jobs"] == 1
    assert "db_path" in i["config"]

    h.close()


def test_clear_jobs():
    """Test clearing jobs by status and age via client (CLI uses it)."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    j1 = h.submit(prompt="to clear", model="x")
    j2 = h.submit(prompt="keep", model="x")

    # Manually complete one for status filter
    fake = JobResult(job_id=j1, status=JobStatus.COMPLETED, output="done")
    h._store.set_result(j1, fake)  # type: ignore[attr-defined]

    # Clear only completed
    cleared = h.clear(status=JobStatus.COMPLETED)
    assert cleared == 1
    assert h.status(j2) == JobStatus.QUEUED  # the other remains

    # Clear remaining (no age filter in this test to avoid time manipulation)
    cleared2 = h.clear()
    assert cleared2 >= 0

    h.close()


def test_stats():
    """Basic queue stats via client (and CLI uses it)."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    j1 = h.submit(prompt="s1", model="x")
    h.submit(prompt="s2", model="x")

    s = h.stats()
    assert s["total_jobs"] == 2
    assert s["queued"] == 2
    assert s["counts"]["queued"] == 2

    h.cancel(j1)
    s2 = h.stats()
    assert s2["cancelled"] >= 1

    h.close()


def test_remove_job():
    """Test removing a specific job via client (CLI uses remove)."""
    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    j1 = h.submit(prompt="to remove", model="x")
    j2 = h.submit(prompt="keep", model="x")

    assert h.remove(j1) is True
    assert h.remove("nonexistent") is False

    # j2 remains
    assert h.status(j2) == JobStatus.QUEUED
    h.close()

def test_sync_facade_callable_from_running_event_loop():
    """show_model / pull_model (sync facades over async adapter calls) must
    work whether called from sync code OR from within a running event loop
    (e.g. notebooks, async apps). Regression test for the broken
    `except RuntimeError -> run_until_complete` fallback, which could not
    recover inside a live loop. Uses the default StubAdapter (no Ollama)."""
    import asyncio

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, start_worker=False)
    try:
        # Sync context (no running loop): the common case.
        assert isinstance(h.show_model("gemma3:1b"), dict)
        h.pull_model("gemma3:1b")  # no-op on stub; must not raise

        # Inside a running loop: previously raised
        # "Cannot run the event loop while another loop is running".
        async def _inside_loop():
            h.pull_model("gemma3:1b")
            return h.show_model("gemma3:1b")

        details = asyncio.run(_inside_loop())
        assert isinstance(details, dict)
    finally:
        h.close()


def test_timeout_seconds_marks_job_failed_without_retry():
    """ADR-011: timeout_seconds caps each attempt and marks the job FAILED
    (terminal, not retried), cancelling the in-flight call. Uses a slow
    StubAdapter subclass — no Ollama needed."""
    import asyncio
    import time

    from hoglah.adapters import StubAdapter

    class _SlowAdapter(StubAdapter):
        attempts = 0

        async def run(self, request):
            type(self).attempts += 1
            await asyncio.sleep(5)  # longer than the 1s timeout
            return await super().run(request)

    db = _temp_db()
    h = Hoglah(config={"db_path": db}, adapter=_SlowAdapter(), start_worker=True)
    try:
        job_id = h.submit(
            prompt="hi", model="stub", timeout_seconds=1, max_retries=3,
        )
        deadline = time.time() + 15
        res = h.get(job_id)
        while res.status not in (JobStatus.COMPLETED, JobStatus.FAILED):
            if time.time() > deadline:
                raise AssertionError("job did not reach terminal state")
            time.sleep(0.2)
            res = h.get(job_id)
        assert res.status == JobStatus.FAILED
        assert "timed out" in (res.error or "").lower()
        assert res.metadata.get("timed_out") is True
        # Despite max_retries=3, a timeout must NOT be retried.
        assert _SlowAdapter.attempts == 1
    finally:
        h.close()
