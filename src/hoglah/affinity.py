"""Unlabelled model-affinity reorder (ADR-023).

Labelled jobs are fixed anchors: they never move relative to each other or to
the unlabelled jobs that currently sit between them. Within each unlabelled
segment, jobs of equal priority may be reordered so currently-loaded models
run first. Aging (skip count / wait time) prevents starvation.
"""

from __future__ import annotations

import time
from typing import Any, Mapping, Sequence

from .labels import is_labelled


def affinity_reorder(
    rows: Sequence[Mapping[str, Any]],
    *,
    loaded: set[str],
    now: float | None = None,
    skip_counts: Mapping[str, int] | None = None,
    first_seen: Mapping[str, float] | None = None,
    max_skip: int = 20,
    max_wait_seconds: float = 60.0,
) -> list[dict[str, Any]]:
    """Reorder unlabelled segments; leave labelled rows as barriers.

    ``loaded`` is the set of currently resident model names (from ``ps()`` or
    the dispatch recency deque). Empty ``loaded`` is a no-op.
    """
    if not rows:
        return []
    items = [dict(r) for r in rows]
    if not loaded:
        return items

    now_f = time.monotonic() if now is None else float(now)
    skips = skip_counts or {}
    seen = first_seen or {}

    out: list[dict[str, Any]] = []
    segment: list[dict[str, Any]] = []

    def _aged(row: Mapping[str, Any]) -> bool:
        jid = str(row.get("id") or "")
        waited = now_f - float(seen.get(jid, now_f))
        return int(skips.get(jid, 0)) >= max_skip or waited >= max_wait_seconds

    def _flush() -> None:
        if not segment:
            return
        by_pri: dict[int, list[dict[str, Any]]] = {}
        for row in segment:
            by_pri.setdefault(-(int(row.get("priority") or 0)), []).append(row)
        for pri in sorted(by_pri):
            group = by_pri[pri]

            def key(row: Mapping[str, Any]) -> tuple[Any, ...]:
                req = row.get("request") or {}
                model = str(req.get("model") or "")
                return (
                    0 if _aged(row) else 1,
                    0 if model in loaded else 1,
                    row.get("created_at") or "",
                )

            out.extend(sorted(group, key=key))
        segment.clear()

    for row in items:
        if is_labelled(row.get("request") or {}):
            _flush()
            out.append(row)
        else:
            segment.append(row)
    _flush()
    return out


def note_skips(
    skip_counts: dict[str, int],
    first_seen: dict[str, float],
    queued_ids: Sequence[str],
    claimed_ids: Sequence[str],
    *,
    now: float | None = None,
) -> None:
    """Update aging bookkeeping after a poll.

    Every still-queued unlabelled job we saw but did not claim is a skip.
    Claimed jobs drop out of the maps.
    """
    now_f = time.monotonic() if now is None else float(now)
    claimed = set(claimed_ids)
    for jid in claimed:
        skip_counts.pop(jid, None)
        first_seen.pop(jid, None)
    for jid in queued_ids:
        if jid in claimed:
            continue
        first_seen.setdefault(jid, now_f)
        skip_counts[jid] = skip_counts.get(jid, 0) + 1
