"""Batch submit helpers: named jobs, intra-batch depends_on, topological order."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from .models import new_job_id


@dataclass(frozen=True)
class BatchSubmitResult:
    """Result of ``Hoglah.submit_batch``.

    ``jobs`` maps each item's local ``name`` (or ``id``) to the persisted job id.
    ``job_ids`` is enqueue order (dependencies first).
    """

    batch_id: str
    jobs: dict[str, str]
    job_ids: tuple[str, ...]

    def __getitem__(self, name: str) -> str:
        return self.jobs[name]


def topological_names(
    names: Sequence[str],
    deps: Mapping[str, Sequence[str]],
) -> list[str]:
    """Return ``names`` in dependency-first order.

    Only edges between names in this set count (external job ids are ignored).
    Raises ValueError on a cycle or a self-reference.
    """
    name_set = set(names)
    if len(name_set) != len(names):
        raise ValueError("duplicate job names in batch")
    incoming: dict[str, int] = {n: 0 for n in names}
    edges: dict[str, list[str]] = {n: [] for n in names}
    for name in names:
        for dep in deps.get(name) or []:
            dep = str(dep).strip()
            if not dep:
                continue
            if dep == name:
                raise ValueError(f"batch job {name!r} depends on itself")
            if dep not in name_set:
                continue
            edges[dep].append(name)
            incoming[name] += 1
    ready = [n for n in names if incoming[n] == 0]
    ordered: list[str] = []
    while ready:
        n = ready.pop(0)
        ordered.append(n)
        for child in edges[n]:
            incoming[child] -= 1
            if incoming[child] == 0:
                ready.append(child)
    if len(ordered) != len(names):
        cyclic = [n for n in names if n not in ordered]
        raise ValueError(f"dependency cycle in batch involving {cyclic}")
    return ordered


def prepare_batch_items(
    items: Sequence[Mapping[str, Any]],
    *,
    batch_id: str | None = None,
) -> tuple[str, list[str], dict[str, str], dict[str, dict[str, Any]], dict[str, list[str]]]:
    """Validate items, assign job ids, resolve local depends_on names.

    Returns ``(batch_id, ordered_names, name_to_job_id, specs, resolved_deps)``.
    ``specs`` are submit-kwargs per name (name/id/depends_on stripped).
    """
    if not items:
        raise ValueError("submit_batch requires at least one item")
    bid = (batch_id or "").strip() or new_job_id()
    names: list[str] = []
    specs: dict[str, dict[str, Any]] = {}
    raw_deps: dict[str, list[str]] = {}
    used: set[str] = set()
    for i, raw in enumerate(items):
        if not isinstance(raw, Mapping):
            raise TypeError(f"batch item {i} must be a mapping")
        item = dict(raw)
        name = item.pop("name", None)
        if name is None:
            name = item.pop("id", None)
        name = str(name).strip() if name is not None else f"job-{i}"
        if not name:
            raise ValueError(f"batch item {i} has an empty name")
        if name in used:
            raise ValueError(f"duplicate batch job name {name!r}")
        used.add(name)
        deps = item.pop("depends_on", None)
        if deps is None:
            dep_list: list[str] = []
        elif isinstance(deps, str):
            dep_list = [p.strip() for p in deps.split(",") if p.strip()]
        else:
            dep_list = [str(d).strip() for d in deps if str(d).strip()]
        names.append(name)
        specs[name] = item
        raw_deps[name] = dep_list

    name_to_id = {n: new_job_id() for n in names}
    resolved: dict[str, list[str]] = {}
    for name in names:
        out: list[str] = []
        for dep in raw_deps[name]:
            if dep in name_to_id:
                out.append(name_to_id[dep])
            else:
                out.append(dep)
        resolved[name] = out

    ordered = topological_names(names, raw_deps)
    return bid, ordered, name_to_id, specs, resolved
