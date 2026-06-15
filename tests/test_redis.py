"""Redis Streams bridge tests (ADR-020).

The broker-neutral crash-safety logic is proven by the shared FakeTransport suite
in test_kafka_bridge.py. Here we cover:
  - the bridge's broker *selection* wiring (non-gated, no broker), and
  - a real end-to-end round-trip, poison→dead-letter, and PEL (pending-entries)
    crash recovery, gated behind RUN_REDIS_TESTS=1 (needs redis + a server at
    localhost:6379).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from types import SimpleNamespace

import pytest

from hoglah.kafka_bridge import MessageBridge

requires_redis = pytest.mark.skipif(
    os.environ.get("RUN_REDIS_TESTS") != "1",
    reason="Redis tests require RUN_REDIS_TESTS=1, redis, and a server at localhost:6379.",
)

REDIS_URL = os.environ.get("HOGLAH_REDIS_URL", "redis://localhost:6379/0")


# --------------------------------------------------------------------------- #
# Broker selection (non-gated — no connection made)
# --------------------------------------------------------------------------- #


def test_bridge_selects_redis_when_only_redis_enabled():
    cfg = SimpleNamespace(
        redis_enabled=True,
        kafka_enabled=False,
        rabbitmq_enabled=False,
        redis_input_stream="in-s",
        redis_results_stream="out-s",
    )
    bridge = MessageBridge(store=None, config=cfg, transport=None)
    assert bridge._broker == "redis"
    assert bridge._input_name == "in-s"
    assert bridge._results_dest == "out-s"


def test_bridge_prefers_kafka_over_redis_when_both_enabled():
    cfg = SimpleNamespace(
        redis_enabled=True,
        kafka_enabled=True,
        rabbitmq_enabled=False,
        kafka_input_topic="k-in",
        kafka_results_topic="k-out",
        redis_input_stream="r-in",
        redis_results_stream="r-out",
    )
    bridge = MessageBridge(store=None, config=cfg, transport=None)
    assert bridge._broker == "kafka"
    assert bridge._results_dest == "k-out"


# --------------------------------------------------------------------------- #
# Gated real-broker tests
# --------------------------------------------------------------------------- #


def _streams(run: str):
    return (f"hoglah-jobs-{run}", f"hoglah-results-{run}", f"hoglah-jobs-dlq-{run}", f"hoglah-{run}")


@requires_redis
def test_real_redis_round_trip(tmp_path):
    import redis

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_s, out_s, dlq, group = _streams(run)
    correlation_id = f"corr-{run}"

    h = Hoglah(
        config={
            "redis_enabled": True, "redis_url": REDIS_URL,
            "redis_input_stream": in_s, "redis_results_stream": out_s,
            "redis_dlq_stream": dlq, "redis_group": group, "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    r = redis.Redis.from_url(REDIS_URL)
    try:
        r.xadd(in_s, {"data": json.dumps({"correlation_id": correlation_id, "model": "stub", "prompt": "hi"}).encode()})
        deadline = time.time() + 30
        received = None
        while time.time() < deadline and received is None:
            for _id, fields in r.xrange(out_s):
                payload = json.loads(fields[b"data"])
                if payload.get("correlation_id") == correlation_id:
                    received = payload
                    break
            if received is None:
                time.sleep(0.3)
        assert received is not None, "no result on the results stream"
        assert received["status"] == "completed"
    finally:
        h.close()
        r.delete(in_s, out_s, dlq)
        r.close()


@requires_redis
def test_real_redis_poison_to_dlq(tmp_path):
    import redis

    from hoglah import Hoglah

    run = uuid.uuid4().hex[:8]
    in_s, out_s, dlq, group = _streams(run)

    h = Hoglah(
        config={
            "redis_enabled": True, "redis_url": REDIS_URL,
            "redis_input_stream": in_s, "redis_results_stream": out_s,
            "redis_dlq_stream": dlq, "redis_group": group, "db_path": str(tmp_path / "r.db"),
        },
        start_worker=True,
    )
    r = redis.Redis.from_url(REDIS_URL)
    try:
        r.xadd(in_s, {"data": b"{not valid json"})
        deadline = time.time() + 30
        dead = None
        while time.time() < deadline and dead is None:
            entries = r.xrange(dlq)
            if entries:
                dead = entries[0]
            else:
                time.sleep(0.3)
        assert dead is not None, "poison message never reached the dead-letter stream"
    finally:
        h.close()
        r.delete(in_s, out_s, dlq)
        r.close()


@requires_redis
def test_real_redis_pel_recovery():
    """An entry consumed but not acked (a crash window) is re-delivered to a new
    transport with the same consumer name from the Pending Entries List."""
    import redis

    from hoglah.redis_streams import RedisStreamsTransport

    run = uuid.uuid4().hex[:8]
    in_s, out_s, dlq, group = _streams(run)
    kw = dict(url=REDIS_URL, input_stream=in_s, results_stream=out_s, dlq_stream=dlq, group=group, consumer_name="c1")

    r = redis.Redis.from_url(REDIS_URL)
    t1 = RedisStreamsTransport(**kw)
    try:
        r.xadd(in_s, {"data": json.dumps({"correlation_id": "x", "model": "m", "prompt": "hi"}).encode()})
        m1 = t1.poll(2.0)  # read via ">" → now in this consumer's PEL, NOT acked
        assert m1 is not None
        t1.close()  # simulate a crash before ack

        # New transport, SAME consumer name → must recover the unacked entry.
        t2 = RedisStreamsTransport(**kw)
        m2 = t2.poll(2.0)
        assert m2 is not None, "unacked entry was not recovered from the PEL"
        assert m2.raw == m1.raw  # same entry redelivered
        t2.ack(m2)
        t2.close()
    finally:
        r.delete(in_s, out_s, dlq)
        r.close()


@requires_redis
def test_real_redis_delete_acked_toggle():
    """delete_acked=True (default) XDELs the input entry after ack; =False keeps
    it in the stream (for replay/audit)."""
    import redis

    from hoglah.redis_streams import RedisStreamsTransport

    run = uuid.uuid4().hex[:8]
    in_s, out_s, dlq, group = _streams(run)
    body = {"data": json.dumps({"correlation_id": "x", "model": "m", "prompt": "hi"}).encode()}

    r = redis.Redis.from_url(REDIS_URL)
    try:
        # Default (delete_acked=True): the entry is gone after ack.
        t_del = RedisStreamsTransport(
            url=REDIS_URL, input_stream=in_s, results_stream=out_s, dlq_stream=dlq,
            group=group, consumer_name="c-del",
        )
        r.xadd(in_s, body)
        m = t_del.poll(2.0)
        assert m is not None
        t_del.ack(m)
        assert r.xlen(in_s) == 0, "delete_acked=True should XDEL the acked entry"
        t_del.close()

        # delete_acked=False: the entry stays in the stream after ack.
        t_keep = RedisStreamsTransport(
            url=REDIS_URL, input_stream=in_s, results_stream=out_s, dlq_stream=dlq,
            group=group, consumer_name="c-keep", delete_acked=False,
        )
        r.xadd(in_s, body)
        m = t_keep.poll(2.0)
        assert m is not None
        t_keep.ack(m)
        assert r.xlen(in_s) == 1, "delete_acked=False should retain the acked entry"
        t_keep.close()
    finally:
        r.delete(in_s, out_s, dlq)
        r.close()
