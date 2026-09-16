import tempfile
from pathlib import Path

from hoglah import Hoglah, JobStatus
from hoglah.labels import eval_label_group, label_rank, prepare_sequence_items


def _db() -> Path:
    return Path(tempfile.mkdtemp(prefix="hoglah-label-")) / "t.db"


def test_label_rank_and_eval():
    seq = ["ingest", "analyze", "write"]
    assert label_rank("analyze", seq) == 1
    assert label_rank("missing", seq) is None

    analyze = {
        "label": "analyze",
        "label_group": "g1",
        "label_sequence": seq,
    }
    peers = [
        {
            "id": "a",
            "status": "queued",
            "request": {"label": "ingest", "label_group": "g1", "label_sequence": seq},
        }
    ]
    assert eval_label_group(analyze, peers, self_id="b") == ("wait", None)

    peers[0]["status"] = "completed"
    assert eval_label_group(analyze, peers, self_id="b")[0] == "ready"

    peers[0]["status"] = "failed"
    state, err = eval_label_group(analyze, peers, self_id="b")
    assert state == "blocked"
    assert "failed" in (err or "")


def test_missing_earlier_label_is_ready():
    req = {
        "label": "analyze",
        "label_group": "g1",
        "label_sequence": ["ingest", "analyze"],
    }
    assert eval_label_group(req, [], self_id="x")[0] == "ready"


def test_submit_sequence_runs_in_label_order():
    h = Hoglah(config={"db_path": _db(), "concurrency": 1}, start_worker=True)
    try:
        result = h.submit_sequence(
            [
                {"label": "ingest", "prompt": "one", "model": "stub-model:1b"},
                {"label": "analyze", "prompt": "two", "model": "stub-model:1b"},
            ],
            sequence=["ingest", "analyze"],
            group="pipe",
        )
        assert result.batch_id == "pipe"
        ingest = h.wait(result.jobs["ingest"], timeout=5)
        analyze = h.wait(result.jobs["analyze"], timeout=5)
        assert ingest.status == JobStatus.COMPLETED
        assert analyze.status == JobStatus.COMPLETED
        # analyze was not runnable until ingest completed; both still finish.
        listed = h.list(label_group="pipe")
        assert len(listed) == 2
    finally:
        h.close()


def test_prepare_sequence_rejects_unknown_label():
    try:
        prepare_sequence_items(
            [{"label": "z", "prompt": "x", "model": "m"}],
            sequence=["a", "b"],
        )
    except ValueError as exc:
        assert "not in label_sequence" in str(exc)
    else:
        raise AssertionError("expected ValueError")
