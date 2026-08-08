import threading
import time

from hoglah import SessionPriorityQueue


def _noop(*args, **kwargs):
    pass


def test_priority_and_per_key_serialization() -> None:
    # workers=0 -> no threads; drive the scheduling logic deterministically.
    q = SessionPriorityQueue(workers=0)
    q.submit(_noop, "a", priority=4, key="k1")
    q.submit(_noop, "b", priority=2, key="k2")
    q.submit(_noop, "c", priority=3, key="k1")

    assert q._claim()[4][0] == "b"  # highest priority first (k2 now busy)
    assert q._claim()[4][0] == "c"  # next highest non-busy: c (prio3) before a (prio4), k1 busy
    assert q._claim() is None  # a is k1 (busy) -> skipped; nothing else eligible
    q._busy.discard("k1")
    assert q._claim()[4][0] == "a"  # k1 freed -> a runs


def test_runs_and_serializes_same_key() -> None:
    q = SessionPriorityQueue(workers=2)
    done: list[int] = []
    lock = threading.Lock()

    def task(i):
        with lock:
            done.append(i)

    for i in range(5):
        q.submit(task, i, key="k")  # same key -> serial, FIFO

    for _ in range(60):
        if len(done) == 5:
            break
        time.sleep(0.05)
    assert done == [0, 1, 2, 3, 4]  # ran, and in submission order (serialised by key)


def test_different_keys_not_blocked() -> None:
    q = SessionPriorityQueue(workers=0)
    q.submit(_noop, "x", key="a")
    q.submit(_noop, "y", key="b")
    first = q._claim()  # claims one key
    second = q._claim()  # different key still eligible (not blocked)
    assert {first[4][0], second[4][0]} == {"x", "y"}


def test_stats_counts_failures() -> None:
    q = SessionPriorityQueue(workers=1)
    done = threading.Event()

    def boom():
        try:
            raise RuntimeError("expected failure")
        finally:
            done.set()

    assert q.stats() == {"queued": 0, "busy_keys": 0, "failures": 0}
    q.submit(boom, priority=1)
    assert done.wait(timeout=2.0)
    # Give the worker a moment to increment the counter after the exception.
    for _ in range(40):
        if q.stats()["failures"] >= 1:
            break
        time.sleep(0.05)
    stats = q.stats()
    assert stats["failures"] == 1
    assert stats["queued"] == 0
    q.close()


def test_close_stops_accepting_work_m8() -> None:
    q = SessionPriorityQueue(workers=1)
    q.close(wait=True, timeout=2.0)
    try:
        q.submit(_noop, priority=1)
        raise AssertionError("submit after close should fail")
    except RuntimeError as exc:
        assert "closed" in str(exc).lower()
