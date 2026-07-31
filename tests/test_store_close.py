"""Regression tests for the SQLiteJobStore close race (issue #13).

The suite's intermittent SIGSEGV was `close()` freeing the sqlite3 connection
while a background thread was mid-`execute` on it — `check_same_thread=False`
lets the worker and kafka_bridge's `hoglah-msg-pub-*` publishers share the
connection, so a bare `self._conn.close()` tore a running statement out from
under the C extension.

These assert the invariant rather than the crash: a test that actually
reproduced the segfault would take the whole runner down with it.
"""

from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from hoglah.store import create_sqlite_store


def test_close_is_idempotent(tmp_path) -> None:
    store = create_sqlite_store(tmp_path / "jobs.db")
    store.close()
    store.close()  # second close must not raise or re-close the connection


def test_close_waits_for_an_in_flight_statement(tmp_path) -> None:
    """close() must block on the same lock every query holds.

    That serialisation is the whole fix: without it, close() ran concurrently
    with a background thread's execute() and freed the connection underneath it.
    """
    store = create_sqlite_store(tmp_path / "jobs.db")
    released = threading.Event()
    closed = threading.Event()

    # Stand in for a background thread holding the lock inside execute().
    store._lock.acquire()

    def _close() -> None:
        store.close()
        closed.set()

    closer = threading.Thread(target=_close, name="closer", daemon=True)
    closer.start()

    time.sleep(0.2)
    assert not closed.is_set(), "close() returned while a statement held the lock"

    store._lock.release()
    released.set()
    closer.join(timeout=5.0)
    assert closed.is_set(), "close() did not complete once the lock was released"


def test_use_after_close_raises_cleanly_rather_than_crashing(tmp_path) -> None:
    """A late background publish must get a Python exception, not a segfault.

    kafka_bridge's `_publish_now` already catches Exception around
    `mark_result_published` and leaves the job for restart re-emit, so a clean
    raise here is the correct, recoverable outcome.
    """
    store = create_sqlite_store(tmp_path / "jobs.db")
    store.close()

    with pytest.raises(sqlite3.ProgrammingError):
        store.mark_result_published("job-that-arrived-late")


def test_concurrent_close_and_writes_do_not_corrupt(tmp_path) -> None:
    """Hammer the exact shape of the crash: writers racing a close."""
    store = create_sqlite_store(tmp_path / "jobs.db")
    errors: list[BaseException] = []

    def _writer() -> None:
        for i in range(200):
            try:
                store.mark_result_published(f"job-{i}")
            except sqlite3.ProgrammingError:
                return  # store closed underneath us — the expected, clean outcome
            except BaseException as exc:  # noqa: BLE001 - surface anything unexpected
                errors.append(exc)
                return

    writers = [threading.Thread(target=_writer, daemon=True) for _ in range(4)]
    for thread in writers:
        thread.start()
    time.sleep(0.05)
    store.close()
    for thread in writers:
        thread.join(timeout=5.0)
        assert not thread.is_alive()

    assert errors == [], f"unexpected errors from concurrent writers: {errors}"
