"""Multi-backend dispatch (BackendPool) + config wiring.

Plain sync tests with asyncio.run inside (no extra pytest plugins), matching the
worker-execution tests' style.
"""

import asyncio

import pytest

from hoglah.config import HoglahSettings
from hoglah.dispatch import BackendPool


class _FakeAdapter:
    def __init__(self, host):
        self.host = host

    async def run(self, req):
        return (f"ran@{self.host}", {}, {})

    async def embed(self, req):
        return ([0.0], {}, {})


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
