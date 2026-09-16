"""Label grouping and labelled-group sequencing (ADR-022).

Jobs that share a ``label_group`` and a ``label_sequence`` form ordered
phases. A job with label rank R waits until every *existing* peer with rank
< R is COMPLETED. If an earlier peer FAILED or was CANCELLED, the job fails
(same shape as ``depends_on``). Missing earlier labels do not block (they
were never submitted).

Unlabelled jobs (no ``label`` and no ``label_group``) are not gated.
"""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from .models import new_job_id


def is_labelled(request: Mapping[str, Any] | Any) -> bool:
    if isinstance(request, Mapping):
        return bool(request.get("label") or request.get("label_group"))
    return bool(getattr(request, "label", None) or getattr(request, "label_group", None))


def coerce_sequence(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        parts = [p.strip() for p in value.split(",") if p.strip()]
        return parts or None
    seq = [str(s).strip() for s in value if str(s).strip()]
    return seq or None


def label_rank(label: str | None, sequence: Sequence[str] | None) -> int | None:
    if not label or not sequence:
        return None
    try:
        return list(sequence).index(label)
    except ValueError:
        return None


def eval_label_group(
    request: Mapping[str, Any],
    peers: Sequence[Mapping[str, Any]],
    *,
    self_id: str | None = None,
) -> tuple[str, str | None]:
    """Return ``('ready', None)``, ``('wait', None)``, or ``('blocked', error)``."""
    label = request.get("label")
    group = request.get("label_group")
    seq = coerce_sequence(request.get("label_sequence")) or []
    if not group or not seq or not label:
        return "ready", None
    rank = label_rank(str(label), seq)
    if rank is None or rank == 0:
        return "ready", None

    saw_unfinished = False
    for peer in peers:
        pid = peer.get("id")
        if self_id is not None and pid == self_id:
            continue
        preq = peer.get("request") or {}
        if (preq.get("label_group") or "") != group:
            continue
        pr = label_rank(preq.get("label"), seq)
        if pr is None or pr >= rank:
            continue
        st = str(peer.get("status") or "")
        if st in ("failed", "cancelled"):
            return (
                "blocked",
                f"label group {group!r} earlier label {preq.get('label')!r} is {st}",
            )
        if st in ("queued", "processing"):
            saw_unfinished = True
    if saw_unfinished:
        return "wait", None
    return "ready", None


def prepare_sequence_items(
    items: Sequence[Mapping[str, Any]],
    *,
    group: str | None = None,
    sequence: Sequence[str] | None = None,
) -> tuple[str, list[str], list[dict[str, Any]]]:
    """Stamp ``label`` / ``label_group`` / ``label_sequence`` onto submit kwargs.

    Returns ``(label_group, sequence, stamped_items)``.
    Sequence defaults to unique labels in first-seen order.
    Items that omit ``label`` get ``job-{i}`` and are appended to the sequence.
    """
    if not items:
        raise ValueError("submit_sequence requires at least one item")
    gid = (group or "").strip() or new_job_id()
    stamped: list[dict[str, Any]] = []
    labels: list[str] = []
    for i, raw in enumerate(items):
        if not isinstance(raw, Mapping):
            raise TypeError(f"sequence item {i} must be a mapping")
        item = dict(raw)
        label = item.get("label")
        if label is None:
            label = item.get("name") or f"job-{i}"
        label = str(label).strip()
        if not label:
            raise ValueError(f"sequence item {i} has an empty label")
        item["label"] = label
        if label not in labels:
            labels.append(label)
        stamped.append(item)
    seq = coerce_sequence(sequence) or labels
    unknown = [it["label"] for it in stamped if it["label"] not in seq]
    if unknown:
        raise ValueError(
            f"labels {unknown} are not in label_sequence {list(seq)}"
        )
    for item in stamped:
        item["label_group"] = gid
        item["label_sequence"] = list(seq)
    return gid, list(seq), stamped
