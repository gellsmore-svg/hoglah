from hoglah.adapters import StubAdapter
from hoglah.preload import (
    Demand,
    HostView,
    JobView,
    PlannerConfig,
    PlannerState,
    RunningModel,
    keep_alive_for,
    parse_keep_alive,
    plan_actions,
)


def test_stub_does_not_preload():
    assert StubAdapter().supports_preload() is False


def test_parse_keep_alive():
    assert parse_keep_alive("5m") == 300.0
    assert parse_keep_alive(-1) == -1.0
    assert parse_keep_alive("0") == 0.0
    assert parse_keep_alive(None) is None


def test_planner_never_sends_zero_or_minus_one():
    demand = Demand(
        next_runnable=(JobView("1", "m", keep_alive="0"),),
        soon=(JobView("2", "m", keep_alive=-1),),
    )
    ka = keep_alive_for("m", demand, PlannerConfig())
    assert ka not in (0, "0", -1, "-1")


def test_unknown_total_only_next_model():
    demand = Demand(
        next_runnable=(JobView("1", "first"),),
        soon=(JobView("2", "second"), JobView("3", "second")),
    )
    hosts = [HostView(index=0, busy_jobs=0, loaded=())]
    actions = plan_actions(demand, hosts, cfg=PlannerConfig(max_concurrent=4))
    preloads = [a for a in actions if a.op == "preload"]
    assert len(preloads) == 1
    assert preloads[0].model == "first"


def test_explicit_budget_packs_first_that_fits():
    state = PlannerState(observed_vram={"a": 6 * 1024**3, "b": 6 * 1024**3})
    cfg = PlannerConfig(vram_budget_bytes=8 * 1024**3, safety_margin=0.0, max_concurrent=4)
    demand = Demand(
        next_runnable=(JobView("1", "a"), JobView("2", "b")),
    )
    hosts = [HostView(index=0, busy_jobs=0, loaded=())]
    actions = plan_actions(demand, hosts, state=state, cfg=cfg)
    preloads = [a for a in actions if a.op == "preload"]
    assert [a.model for a in preloads] == ["a"]


def test_busy_host_gets_no_preload():
    demand = Demand(next_runnable=(JobView("1", "m"),))
    hosts = [HostView(index=0, busy_jobs=1, loaded=())]
    actions = plan_actions(demand, hosts)
    assert [a for a in actions if a.op == "preload"] == []


def test_processing_model_not_unloaded():
    demand = Demand(
        processing=(JobView("p", "keepme"),),
        store_wide_models=frozenset({"keepme"}),
    )
    hosts = [
        HostView(
            index=0,
            busy_jobs=0,
            loaded=(RunningModel(name="keepme", size_vram=1),),
        )
    ]
    actions = plan_actions(
        demand, hosts, cfg=PlannerConfig(unload_unused=True, max_concurrent=4)
    )
    assert [a for a in actions if a.op == "unload"] == []


def test_warm_host_preferred():
    demand = Demand(next_runnable=(JobView("1", "m"),))
    hosts = [
        HostView(index=0, busy_jobs=0, loaded=(), recent=()),
        HostView(index=1, busy_jobs=0, loaded=(), recent=("m",)),
    ]
    actions = plan_actions(demand, hosts)
    pre = [a for a in actions if a.op == "preload"]
    assert pre and pre[0].host_index == 1
