"""Multi-backend dispatch (BackendPool) + config wiring.

Plain sync tests with asyncio.run inside (no extra pytest plugins), matching the
worker-execution tests' style.
"""

import asyncio

import pytest

from hoglah.config import HoglahSettings
from hoglah.dispatch import BackendPool


class _FakeAdapter:
    def __init__(self, host, models=None):
        self.host = host
        self._models = list(models or [])

    async def run(self, req):
        return (f"ran@{self.host}", {}, {})

    async def embed(self, req):
        return ([0.0], {}, {})

    async def list_models(self):
        return [{"model": m} for m in self._models]


def test_pool_requires_at_least_one_adapter():
    with pytest.raises(ValueError):
        BackendPool([])


def test_pool_reports_hosts_and_initial_load():
    pool = BackendPool([_FakeAdapter("a"), _FakeAdapter("b")])
    assert len(pool) == 2
    assert pool.hosts == ["a", "b"]
    assert pool.loads() == [0, 0]


def test_lease_picks_least_loaded_and_releases():
    pool = BackendPool([_FakeAdapter("a"), _FakeAdapter("b"), _FakeAdapter("c")])

    async def scenario():
        async with pool.lease() as first:          # all idle -> index 0
            assert first.host == "a"
            async with pool.lease() as second:     # 0 busy -> index 1
                assert second.host == "b"
                assert pool.loads() == [1, 1, 0]
                async with pool.lease() as third:  # -> index 2
                    assert third.host == "c"
                    assert pool.loads() == [1, 1, 1]
            assert pool.loads() == [1, 0, 0]        # b released
        assert pool.loads() == [0, 0, 0]            # all released

    asyncio.run(scenario())


def test_lease_reuses_freed_backend():
    pool = BackendPool([_FakeAdapter("a"), _FakeAdapter("b")])

    async def scenario():
        async with pool.lease() as one:
            assert one.host == "a"
        async with pool.lease() as two:   # a is free again, lowest index wins
            assert two.host == "a"

    asyncio.run(scenario())


def test_warm_affinity_routes_a_model_back_to_its_backend():
    pool = BackendPool([_FakeAdapter("a"), _FakeAdapter("b")])

    async def scenario():
        # Force A onto backend 0 and B onto backend 1 by holding A's lease open
        # (so B sees backend 0 busy and lands on backend 1).
        async with pool.lease("modelA"):          # idle -> idx 0, recent[0]={A}
            async with pool.lease("modelB"):       # 0 busy -> idx 1, recent[1]={B}
                pass
        assert pool.warm() == [["modelA"], ["modelB"]]

        # Now both idle: affinity should send each model back to its warm backend,
        # not just pile onto backend 0.
        async with pool.lease("modelA") as a:
            assert a.host == "a"
        async with pool.lease("modelB") as b:
            assert b.host == "b"            # warm beats least-loaded-by-index

    asyncio.run(scenario())


def test_unwarm_model_falls_back_to_least_loaded():
    pool = BackendPool([_FakeAdapter("a"), _FakeAdapter("b")])

    async def scenario():
        async with pool.lease("seen"):       # warms backend 0
            # a never-seen model, backend 0 busy -> least-loaded picks backend 1
            async with pool.lease("fresh") as f:
                assert f.host == "b"

    asyncio.run(scenario())


def test_available_models_dedupes_across_backends():
    pool = BackendPool([
        _FakeAdapter("a", models=["gemma4:e2b", "bge-m3:latest"]),
        _FakeAdapter("b", models=["gemma4:e2b", "qwen3.6:latest"]),
    ])
    models = asyncio.run(pool.available_models())
    assert models == ["bge-m3:latest", "gemma4:e2b", "qwen3.6:latest"]  # sorted union


def test_available_models_tolerates_an_unreachable_backend():
    class _Broken:
        host = "down"
        async def list_models(self):
            raise ConnectionError("unreachable")

    pool = BackendPool([_FakeAdapter("a", models=["m1"]), _Broken()])
    assert asyncio.run(pool.available_models()) == ["m1"]  # broken backend contributes nothing


def test_config_parses_comma_separated_hosts_from_env_string():
    cfg = HoglahSettings(ollama_hosts="http://gpu1:11434, http://gpu2:11434")
    assert cfg.ollama_hosts == ["http://gpu1:11434", "http://gpu2:11434"]
    assert cfg.to_dict()["ollama_hosts"] == ["http://gpu1:11434", "http://gpu2:11434"]


def test_client_builds_pool_for_multiple_hosts():
    from hoglah import Hoglah

    h = Hoglah(config={"ollama_hosts": ["http://a:11434", "http://b:11434"]},
               use_real=True, start_worker=False)
    try:
        assert h._pool is not None and len(h._pool) == 2
        assert h._pool.hosts == ["http://a:11434", "http://b:11434"]
    finally:
        h.close()


def test_client_single_host_has_no_pool():
    from hoglah import Hoglah

    h = Hoglah(config={"ollama_host": "http://a:11434"}, use_real=True, start_worker=False)
    try:
        assert h._pool is None
    finally:
        h.close()
