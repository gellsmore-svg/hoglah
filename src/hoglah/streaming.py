"""Token streaming (ADR-025 / gap G8).

Cross-process channel is an append-only NDJSON file per job. The JobStore
stays terminal-only — these files are *not* a second store of record.

File: ``<stream_dir>/<job_id>.ndjson``

    {"i": 0, "text": "Hel"}
    {"i": 1, "text": "lo"}
    {"done": true, "status": "completed"}

``hoglah serve`` tails the file over SSE. If there is no file (streaming off,
or the job already finished), SSE synthesises a single chunk from the stored
terminal output and closes.
"""

from __future__ import annotations

import asyncio
import json
import re
import threading
import time
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_TERMINAL = frozenset({"completed", "failed", "cancelled"})


def safe_job_id(job_id: str) -> str:
    """Reject path traversal and odd bytes. Job ids are uuid4 hex by default."""
    raw = str(job_id).strip()
    if not raw or raw in {".", ".."} or "/" in raw or "\\" in raw or ".." in raw:
        raise ValueError(f"unsafe job id: {job_id!r}")
    if _JOB_ID_RE.fullmatch(raw) is None:
        raise ValueError(f"unsafe job id: {job_id!r}")
    return raw


def stream_path(stream_dir: Path | str, job_id: str) -> Path:
    return Path(stream_dir) / f"{safe_job_id(job_id)}.ndjson"


def default_stream_dir() -> Path:
    return Path("~/.hoglah/streams").expanduser()


class ChunkWriter:
    """Append-only NDJSON writer. ``reset()`` truncates (retry). ``finish`` is idempotent."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._lock = threading.Lock()
        self._fh: Any | None = None
        self._index = 0
        self._closed = False

    def open(self) -> None:
        """Create/truncate the file for a new attempt."""
        with self._lock:
            self._open_unlocked()

    def _open_unlocked(self) -> None:
        if self._fh is not None:
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(self.path, "w", encoding="utf-8")  # noqa: SIM115
        self._index = 0
        self._closed = False

    def reset(self) -> None:
        """Truncate for a retry. No-op if already finished (cancel won)."""
        with self._lock:
            if self._closed:
                return
            self._open_unlocked()

    def write_chunk(self, text: str) -> None:
        if not text:
            return
        with self._lock:
            if self._closed or self._fh is None:
                return
            rec = {"i": self._index, "text": text}
            self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self._fh.flush()
            self._index += 1

    def finish(self, status: str, error: str | None = None) -> None:
        with self._lock:
            if self._closed:
                return
            if self._fh is None:
                self._open_unlocked()
            rec: dict[str, Any] = {"done": True, "status": status}
            if error:
                rec["error"] = error
            assert self._fh is not None
            self._fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self._fh.flush()
            try:
                self._fh.close()
            except Exception:
                pass
            self._fh = None
            self._closed = True

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                try:
                    self._fh.close()
                except Exception:
                    pass
                self._fh = None


class NdjsonTail:
    """Byte-offset tail of an NDJSON stream file. Detects retry truncates."""

    def __init__(self, path: Path | None) -> None:
        self.path = Path(path) if path is not None else None
        self.offset = 0
        self.buf = ""

    def read_new(self) -> list[dict[str, Any]]:
        if self.path is None or not self.path.is_file():
            return []
        try:
            size = self.path.stat().st_size
        except OSError:
            return []
        if size < self.offset:
            self.offset = 0
            self.buf = ""
        try:
            with open(self.path, encoding="utf-8") as fh:
                fh.seek(self.offset)
                data = fh.read()
                self.offset = fh.tell()
        except OSError:
            return []
        if not data:
            return []
        self.buf += data
        events: list[dict[str, Any]] = []
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(rec, dict):
                events.append(rec)
        return events


def _job_status(job: Any) -> str | None:
    if job is None:
        return None
    status = getattr(job, "status", None)
    if status is None and isinstance(job, dict):
        status = job.get("status")
    if hasattr(status, "value"):
        return str(status.value)
    if status is None:
        return None
    return str(status)


def _job_output(job: Any) -> str:
    if job is None:
        return ""
    if isinstance(job, dict):
        return str(job.get("output") or "")
    return str(getattr(job, "output", None) or "")


def _job_error(job: Any) -> str | None:
    if job is None:
        return None
    if isinstance(job, dict):
        err = job.get("error")
    else:
        err = getattr(job, "error", None)
    return str(err) if err else None


async def follow_job_events(
    *,
    job_id: str,
    stream_dir: Path | str | None,
    get_job: Callable[[str], Any],
    poll_interval: float = 0.05,
    heartbeat_every: float = 15.0,
) -> AsyncIterator[dict[str, Any]]:
    """Yield SSE-shaped dicts until the job is terminal.

    Event shapes:
      ``{"type": "chunk", "text": "...", "i": 0}``
      ``{"type": "done", "status": "completed", "error": None}``
      ``{"type": "heartbeat"}``
    """
    jid = safe_job_id(job_id)
    path = stream_path(stream_dir, jid) if stream_dir else None
    tail = NdjsonTail(path)
    emitted_text = False
    last_hb = time.monotonic()
    while True:
        for rec in tail.read_new():
            if rec.get("done"):
                yield {
                    "type": "done",
                    "status": rec.get("status") or "completed",
                    "error": rec.get("error"),
                }
                return
            text = rec.get("text")
            if text:
                emitted_text = True
                yield {"type": "chunk", "text": text, "i": rec.get("i")}
        job = get_job(jid)
        if job is None:
            yield {"type": "done", "status": "failed", "error": "job not found"}
            return
        status = _job_status(job)
        if status in _TERMINAL:
            for rec in tail.read_new():
                if rec.get("done"):
                    yield {
                        "type": "done",
                        "status": rec.get("status") or status,
                        "error": rec.get("error") or _job_error(job),
                    }
                    return
                text = rec.get("text")
                if text:
                    emitted_text = True
                    yield {"type": "chunk", "text": text, "i": rec.get("i")}
            if not emitted_text:
                output = _job_output(job)
                if output:
                    yield {"type": "chunk", "text": output, "i": 0}
            yield {
                "type": "done",
                "status": status,
                "error": _job_error(job),
            }
            return
        now = time.monotonic()
        if heartbeat_every > 0 and now - last_hb >= heartbeat_every:
            last_hb = now
            yield {"type": "heartbeat"}
        await asyncio.sleep(poll_interval)


def encode_sse(event: dict[str, Any]) -> bytes:
    """Encode one SSE frame. Heartbeats are comments so EventSource ignores them."""
    if event.get("type") == "heartbeat":
        return b": ping\n\n"
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n".encode("utf-8")
