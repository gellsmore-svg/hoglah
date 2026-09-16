from hoglah.affinity import affinity_reorder, note_skips


def _row(jid, model, *, priority=0, created="2026-01-01T00:00:00+00:00", label=None):
    req = {"model": model}
    if label:
        req["label"] = label
        req["label_group"] = "g"
    return {
        "id": jid,
        "priority": priority,
        "created_at": created,
        "request": req,
    }


def test_unlabelled_warm_model_jumps_within_segment():
    rows = [
        _row("cold", "big", created="2026-01-01T00:00:00+00:00"),
        _row("hot", "tiny", created="2026-01-01T00:00:01+00:00"),
    ]
    out = affinity_reorder(rows, loaded={"tiny"}, now=0.0, first_seen={})
    assert [r["id"] for r in out] == ["hot", "cold"]


def test_labelled_jobs_are_barriers():
    rows = [
        _row("a", "cold", created="t0"),
        _row("lab", "x", created="t1", label="ingest"),
        _row("b", "hot", created="t2"),
    ]
    out = affinity_reorder(rows, loaded={"hot"}, now=0.0)
    # b may not jump ahead of the labelled barrier
    assert [r["id"] for r in out] == ["a", "lab", "b"]


def test_aging_beats_affinity():
    rows = [
        _row("old", "cold", created="t0"),
        _row("new", "hot", created="t1"),
    ]
    out = affinity_reorder(
        rows,
        loaded={"hot"},
        now=100.0,
        first_seen={"old": 0.0, "new": 99.0},
        max_wait_seconds=10.0,
    )
    assert [r["id"] for r in out] == ["old", "new"]


def test_note_skips():
    skips: dict[str, int] = {}
    seen: dict[str, float] = {}
    note_skips(skips, seen, ["a", "b"], ["a"], now=1.0)
    assert "a" not in skips
    assert skips["b"] == 1
    assert seen["b"] == 1.0
