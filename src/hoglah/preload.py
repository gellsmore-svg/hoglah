"""Preemptive Ollama-class model loading (ADR-024).

Pure planner plus a small in-process state object. The worker supplies
snapshots; this module never touches JobStore or the job semaphore.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence


_KEEP_ALIVE_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*([smhd]?)\s*$", re.I)
_UNITS = {"": 1.0, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}


def parse_keep_alive(value: str | int | float | None) -> float | None:
    """Seconds, or None if unset. ``-1`` means forever (returned as -1.0)."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    m = _KEEP_ALIVE_RE.match(str(value))
    if not m:
        return None
    n = float(m.group(1))
    unit = (m.group(2) or "").lower()
    if n < 0:
        return -1.0
    return n * _UNITS.get(unit, 1.0)


def format_keep_alive(seconds: float) -> str | int:
    """Ollama-friendly keep_alive. Never returns 0 or -1 (planner contract)."""
    if seconds <= 0:
        seconds = 1.0
    secs = int(round(seconds))
    if secs % 3600 == 0 and secs >= 3600:
        return f"{secs // 3600}h"
    if secs % 60 == 0 and secs >= 60:
        return f"{secs // 60}m"
    return secs


@dataclass(frozen=True)
class JobView:
    job_id: str
    model: str
    kind: str = "generate"
    keep_alive: str | int | None = None
    supports_preload: bool = True


@dataclass(frozen=True)
class RunningModel:
    name: str
    size: int = 0
    size_vram: int = 0
    expires_at: str | None = None


@dataclass(frozen=True)
class HostView:
    index: int
    busy_jobs: int = 0
    preload_inflight: str | None = None
    loaded: tuple[RunningModel, ...] = ()
    recent: tuple[str, ...] = ()
    ps_unknown: bool = False  # True if ps() failed


@dataclass(frozen=True)
class Demand:
    processing: tuple[JobView, ...] = ()
    next_runnable: tuple[JobView, ...] = ()
    soon: tuple[JobView, ...] = ()
    store_wide_models: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class PreloadAction:
    op: str  # preload | refresh | unload | skip
    model: str
    host_index: int
    keep_alive: str | int | None = None
    kind: str = "generate"
    reason: str = ""


@dataclass
class PlannerConfig:
    lookahead: int = 8
    safety_margin: float = 0.20
    vram_budget_bytes: int | None = None
    disk_to_vram: float = 1.0
    unknown_size_bytes: int | None = None
    keep_alive: str = "5m"
    keep_alive_max: str = "30m"
    refresh_horizon_seconds: float = 60.0
    unload_unused: bool = False
    max_concurrent: int = 1
    spillover_busy: int = 1
    idle_only: bool = True


@dataclass
class PlannerState:
    observed_vram: dict[str, int] = field(default_factory=dict)
    disk_size: dict[str, int] = field(default_factory=dict)
    alias: dict[str, str] = field(default_factory=dict)
    hard_cap: dict[int, int] = field(default_factory=dict)
    cooldown_until: dict[tuple[str, int], float] = field(default_factory=dict)


def canonical_name(model: str, state: PlannerState) -> str:
    return state.alias.get(model, model)


def _models_of(jobs: Iterable[JobView]) -> set[str]:
    return {j.model for j in jobs if j.model and j.supports_preload}


def keep_alive_for(
    model: str,
    demand: Demand,
    cfg: PlannerConfig,
) -> str | int | None:
    """Planner keep_alive for empty-prompt calls. Never 0 or -1."""
    remain = [
        j
        for j in (*demand.next_runnable, *demand.soon)
        if j.model == model and j.supports_preload
    ]
    if not remain:
        return None
    policy_s = parse_keep_alive(cfg.keep_alive) or 300.0
    max_s = parse_keep_alive(cfg.keep_alive_max) or 1800.0
    if max_s < 0:
        max_s = 1800.0
    job_values = [parse_keep_alive(j.keep_alive) for j in remain]
    finite_job = [v for v in job_values if v is not None and v > 0]
    if len(remain) == 1:
        chosen = policy_s
        if finite_job:
            chosen = max(chosen, finite_job[0])
    else:
        chosen = policy_s
        if finite_job:
            chosen = max(chosen, max(finite_job))
    chosen = min(chosen, max_s)
    if chosen <= 0:
        chosen = policy_s
    return format_keep_alive(chosen)


def estimate_vram(model: str, state: PlannerState, cfg: PlannerConfig) -> int | None:
    canon = canonical_name(model, state)
    if canon in state.observed_vram:
        return state.observed_vram[canon]
    if model in state.observed_vram:
        return state.observed_vram[model]
    disk = state.disk_size.get(canon) or state.disk_size.get(model)
    if disk:
        return int(disk * float(cfg.disk_to_vram))
    if cfg.unknown_size_bytes is not None:
        return int(cfg.unknown_size_bytes)
    return None


def _used(host: HostView) -> int:
    return sum(m.size_vram for m in host.loaded if (m.size_vram or 0) > 0)


def _total(host: HostView, state: PlannerState, cfg: PlannerConfig) -> int | None:
    if cfg.vram_budget_bytes is not None:
        return int(cfg.vram_budget_bytes)
    if host.index in state.hard_cap:
        return int(state.hard_cap[host.index])
    return None


def _usable(host: HostView, state: PlannerState, cfg: PlannerConfig) -> int:
    total = _total(host, state, cfg)
    if total is None:
        return 0
    return max(0, math.floor(total * (1.0 - cfg.safety_margin)) - _used(host))


def _loaded_names(host: HostView, state: PlannerState) -> set[str]:
    names = {m.name for m in host.loaded if m.name}
    # Also accept alias keys that map onto those names.
    extra = {req for req, ps in state.alias.items() if ps in names}
    return names | extra


def _host_has(host: HostView, model: str, state: PlannerState) -> bool:
    names = _loaded_names(host, state)
    return model in names or canonical_name(model, state) in names


def _busy(host: HostView) -> int:
    return int(host.busy_jobs) + (1 if host.preload_inflight else 0)


def _on_cooldown(model: str, host: HostView, state: PlannerState, now: float) -> bool:
    return state.cooldown_until.get((model, host.index), 0.0) > now


def place_model(
    model: str,
    hosts: Sequence[HostView],
    *,
    state: PlannerState,
    cfg: PlannerConfig,
    in_next: bool,
    now: float = 0.0,
) -> int | None:
    """Pick an idle host for ``model``, or None to skip this tick."""
    candidates = [
        h for h in hosts if not h.ps_unknown or True  # unknown ps still placeable
    ]
    if not candidates:
        return None
    idle = [h for h in candidates if _busy(h) == 0 and not _on_cooldown(model, h, state, now)]
    warm_ps = [h for h in idle if _host_has(h, model, state)]
    warm_rec = [h for h in idle if model in h.recent]
    warm = warm_ps or warm_rec
    if warm:
        best = min(warm, key=lambda h: (h.index,))
        return best.index
    if not idle:
        # Spillover only for next-to-run onto a cold idle host — none idle.
        return None
    # Already resident somewhere idle? Prefer that (no-op load).
    resident = [h for h in candidates if _host_has(h, model, state)]
    if resident:
        idle_res = [h for h in resident if _busy(h) == 0]
        if idle_res:
            return min(idle_res, key=lambda h: h.index).index
        if in_next:
            busy_warm = min(resident, key=lambda h: (_busy(h), h.index))
            if _busy(busy_warm) >= cfg.spillover_busy and idle:
                return min(idle, key=lambda h: h.index).index
        return None
    return min(idle, key=lambda h: h.index).index


def plan_actions(
    demand: Demand,
    hosts: Sequence[HostView],
    *,
    state: PlannerState | None = None,
    cfg: PlannerConfig | None = None,
    now: float = 0.0,
) -> list[PreloadAction]:
    """Return at most ``max_concurrent`` preload/refresh/unload actions."""
    cfg = cfg or PlannerConfig()
    state = state or PlannerState()
    if not hosts:
        return []

    m_proc = _models_of(demand.processing)
    m_next = _models_of(demand.next_runnable)
    m_soon = _models_of(demand.soon)
    m_keep = m_proc | m_next | m_soon
    store_wide = set(demand.store_wide_models) | m_keep

    actions: list[PreloadAction] = []
    reserved_hosts: set[int] = set()
    preloaded_models: set[str] = set()

    def _can_issue() -> bool:
        return len([a for a in actions if a.op in ("preload", "refresh", "unload")]) < max(
            1, cfg.max_concurrent
        )

    # --- keep refresh on idle hosts ---
    for host in hosts:
        if not _can_issue():
            break
        if _busy(host) > 0:
            continue
        for running in host.loaded:
            name = running.name
            if not name or name not in m_keep and canonical_name(name, state) not in m_keep:
                # also match via alias reverse
                requested = next((r for r, p in state.alias.items() if p == name), name)
                if requested not in m_keep:
                    continue
            else:
                requested = name if name in m_keep else canonical_name(name, state)
            ka = keep_alive_for(requested, demand, cfg)
            if ka is None:
                continue
            actions.append(
                PreloadAction(
                    op="refresh",
                    model=requested,
                    host_index=host.index,
                    keep_alive=ka,
                    reason="keep-alive refresh",
                )
            )
            reserved_hosts.add(host.index)
            if not _can_issue():
                break

    # --- preload next then soon, order-preserving ---
    ordered: list[JobView] = []
    seen: set[str] = set()
    for job in (*demand.next_runnable, *demand.soon):
        if not job.supports_preload or not job.model or job.model in seen:
            continue
        seen.add(job.model)
        ordered.append(job)

    unknown_total = all(_total(h, state, cfg) is None for h in hosts)
    speculative_used = False

    for job in ordered:
        if not _can_issue():
            break
        model = job.model
        if model in preloaded_models:
            continue
        if any(_host_has(h, model, state) for h in hosts if _busy(h) == 0 or True):
            # Already resident somewhere: skip new load (refresh handled above).
            if any(_host_has(h, model, state) for h in hosts):
                continue
        in_next = model in m_next
        host_idx = place_model(
            model, hosts, state=state, cfg=cfg, in_next=in_next, now=now
        )
        if host_idx is None or host_idx in reserved_hosts:
            continue
        host = next(h for h in hosts if h.index == host_idx)
        if _busy(host) > 0:
            continue
        if _on_cooldown(model, host, state, now):
            continue
        vram = estimate_vram(model, state, cfg)
        if unknown_total:
            if speculative_used and not in_next:
                continue
            # At most one speculative load, and only the next-to-run model
            # unless this *is* the next-to-run.
            if not in_next and demand.next_runnable:
                # only the first next-runnable model may speculate
                first_next = next(
                    (j.model for j in demand.next_runnable if j.supports_preload),
                    None,
                )
                if model != first_next:
                    continue
            if speculative_used:
                continue
            speculative_used = True
        else:
            if vram is None:
                if not in_next:
                    continue
            elif vram > _usable(host, state, cfg):
                if not (in_next and model == (demand.next_runnable[0].model if demand.next_runnable else None)):
                    # first next that doesn't fit: skip (unload path below may free)
                    continue
                else:
                    continue
        ka = keep_alive_for(model, demand, cfg) or cfg.keep_alive
        actions.append(
            PreloadAction(
                op="preload",
                model=model,
                host_index=host_idx,
                keep_alive=ka,
                kind=job.kind or "generate",
                reason="next" if in_next else "soon",
            )
        )
        reserved_hosts.add(host_idx)
        preloaded_models.add(model)

    # --- optional unload of non-keep residents on idle hosts ---
    if cfg.unload_unused:
        for host in hosts:
            if not _can_issue():
                break
            if _busy(host) > 0:
                continue
            for running in host.loaded:
                name = running.name
                if not name:
                    continue
                requested = next((r for r, p in state.alias.items() if p == name), name)
                if requested in store_wide or name in store_wide or requested in m_proc or name in m_proc:
                    continue
                if host.index in reserved_hosts:
                    continue
                actions.append(
                    PreloadAction(
                        op="unload",
                        model=requested,
                        host_index=host.index,
                        keep_alive=0,
                        reason="unused",
                    )
                )
                reserved_hosts.add(host.index)
                break

    # Cap to max_concurrent real RPCs (skip ops don't count if we added any).
    real = [a for a in actions if a.op in ("preload", "refresh", "unload")]
    return real[: max(1, cfg.max_concurrent)]


def normalize_ps_entry(entry: Any) -> RunningModel | None:
    if isinstance(entry, RunningModel):
        return entry
    if isinstance(entry, dict):
        name = entry.get("model") or entry.get("name")
        if not name:
            return None
        try:
            size = int(entry.get("size") or 0)
        except (TypeError, ValueError):
            size = 0
        try:
            size_vram = int(entry.get("size_vram") or 0)
        except (TypeError, ValueError):
            size_vram = 0
        expires = entry.get("expires_at")
        return RunningModel(
            name=str(name),
            size=size,
            size_vram=size_vram,
            expires_at=str(expires) if expires else None,
        )
    name = getattr(entry, "model", None) or getattr(entry, "name", None)
    if not name:
        return None
    return RunningModel(
        name=str(name),
        size=int(getattr(entry, "size", 0) or 0),
        size_vram=int(getattr(entry, "size_vram", 0) or 0),
        expires_at=str(getattr(entry, "expires_at", None) or "") or None,
    )
