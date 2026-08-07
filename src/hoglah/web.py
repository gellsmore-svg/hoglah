"""FastAPI app: read-only web queue monitor (the web counterpart of `hoglah monitor`).

Single-file: routes + templates + CSS, HTML built from Python f-strings
with html.escape on every interpolation — the same minimal pattern as
Mahalath's browser. No templating dependency.

The app opens the same JobStore as the CLI through a non-working client
(``Hoglah(start_worker=False)``), so it never executes jobs itself; it
only observes the queue that a separate worker process (or the
submitting application) is driving.

Security posture: bind to 127.0.0.1 by default. There is no auth; the
operator runs it locally, browser on the same host. The UI is strictly
read-only — maintenance actions stay in the CLI
(`hoglah clear/rm/cancel/requeue/dlq`). Prometheus scrape: ``GET /metrics``.

Requires the ``web`` extra: ``pip install hoglah[web]``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse

from .client import Hoglah
from .models import JobResult, JobStatus

_STATUSES = [s.value for s in JobStatus]

_CSS = """
:root {
  --bg: #f6f7f9; --surface: #ffffff; --ink: #1c2128; --muted: #667085;
  --line: #e3e6ea; --line-soft: #edf0f3; --accent: #2f5fd0; --accent-soft: #e8eefc;
  --ok-bg: #d9f0e1; --ok-ink: #14572c; --warn-bg: #fdf1cf; --warn-ink: #755600;
  --bad-bg: #fadbde; --bad-ink: #82202b; --info-bg: #d8ecf3; --info-ink: #0c5460;
  --neutral-bg: #e6e8eb; --neutral-ink: #3d434b;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #12151b; --surface: #1a1e26; --ink: #e5e8ee; --muted: #97a0af;
    --line: #2a3039; --line-soft: #232833; --accent: #7c9cff; --accent-soft: #232c45;
    --ok-bg: #14361f; --ok-ink: #7edc9f; --warn-bg: #3a3212; --warn-ink: #ecd07a;
    --bad-bg: #43191e; --bad-ink: #ff9aa4; --info-bg: #123340; --info-ink: #8ed3ea;
    --neutral-bg: #262c36; --neutral-ink: #b6bec9;
  }
}
* { box-sizing: border-box; }
body { font-family: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; margin: 0;
       line-height: 1.55; color: var(--ink); background: var(--bg); font-size: 15px; }
main { max-width: 1100px; margin: 0 auto; padding: 1.2em 1.2em 3em; }
a { color: var(--accent); }
h1 { font-size: 1.5em; margin: 0.6em 0 0.5em; }
h2 { font-size: 1.05em; margin: 1.6em 0 0.5em; text-transform: uppercase;
     letter-spacing: 0.05em; color: var(--muted); font-weight: 600; }
header { position: sticky; top: 0; z-index: 10; background: var(--surface);
         border-bottom: 1px solid var(--line); }
header nav { max-width: 1100px; margin: 0 auto; padding: 0.55em 1.2em;
             display: flex; align-items: center; gap: 0.6em; }
.brand { font-weight: 700; color: var(--ink); text-decoration: none; }
.dbname { margin-left: auto; color: var(--muted); font-size: 0.85em;
          font-family: ui-monospace, "SF Mono", Menlo, monospace; }
.pulse { font-size: 0.8em; color: var(--muted); }
table { border-collapse: collapse; width: 100%; margin: 1em 0; background: var(--surface);
        border: 1px solid var(--line); border-radius: 10px; overflow: hidden; }
th, td { text-align: left; padding: 0.55em 0.8em; border-bottom: 1px solid var(--line-soft);
         vertical-align: top; }
tr:last-child td { border-bottom: none; }
th { background: var(--line-soft); font-weight: 600; font-size: 0.85em;
     text-transform: uppercase; letter-spacing: 0.04em; color: var(--muted); }
tr:hover td { background: var(--line-soft); }
@media (max-width: 720px) { table { display: block; overflow-x: auto; } }
code, .mono { font-family: ui-monospace, "SF Mono", Menlo, monospace; font-size: 0.92em; }
code { background: var(--line-soft); padding: 0.1em 0.4em; border-radius: 4px; }
pre { background: var(--surface); border: 1px solid var(--line); border-radius: 10px;
      padding: 0.8em 1em; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }
.badge { display: inline-block; padding: 0.12em 0.6em; border-radius: 999px; font-size: 0.78em;
         font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
.badge.queued { background: var(--warn-bg); color: var(--warn-ink); }
.badge.processing { background: var(--info-bg); color: var(--info-ink); }
.badge.completed { background: var(--ok-bg); color: var(--ok-ink); }
.badge.failed { background: var(--bad-bg); color: var(--bad-ink); }
.badge.cancelled { background: var(--neutral-bg); color: var(--neutral-ink); }
.muted { color: var(--muted); }
.summary { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr));
           gap: 0.8em; }
.summary .card { background: var(--surface); border: 1px solid var(--line);
                 border-radius: 12px; padding: 0.9em 1.1em; text-decoration: none;
                 color: inherit; display: block; }
.summary .card:hover { border-color: var(--accent); }
.summary .card.active { border-color: var(--accent); background: var(--accent-soft); }
.summary .card .n { font-size: 1.8em; font-weight: 700; line-height: 1.15;
                    font-variant-numeric: tabular-nums; }
.summary .card .label { font-size: 0.83em; color: var(--muted); margin-top: 0.15em; }
.kvbox th { width: 13em; }
"""


def _age(timings: dict[str, Any] | None, now: datetime | None = None) -> str:
    """Short relative age from a job's most recent timing ('2s', '5m', '-')."""
    times = [v for v in (timings or {}).values() if hasattr(v, "timestamp")]
    if not times:
        return "-"
    latest = max(times)
    ref = now or (datetime.now(latest.tzinfo) if getattr(latest, "tzinfo", None) else datetime.now())
    try:
        secs = max(0, int((ref - latest).total_seconds()))
    except (TypeError, ValueError):
        return "-"
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    if secs < 86400:
        return f"{secs // 3600}h"
    return f"{secs // 86400}d"


def _result_to_dict(res: JobResult) -> dict[str, Any]:
    """JSON-safe JobResult dict (status/value, ISO timings, short preview)."""
    d = asdict(res)
    d["status"] = res.status.value
    timings = d.get("timings") or {}
    for key, value in list(timings.items()):
        if value is not None and hasattr(value, "isoformat"):
            timings[key] = value.isoformat()
    d["timings"] = timings
    d["age"] = _age(res.timings)
    d["step_name"] = (res.metadata or {}).get("step_name")
    if res.error:
        preview = "ERROR: " + res.error[:120]
    elif res.output:
        preview = res.output[:120] + ("…" if len(res.output) > 120 else "")
    else:
        preview = ""
    d["preview"] = preview
    return d


def _base(title: str, body: str, db_label: str, pulse: bool = False) -> str:
    pulse_html = '<span class="pulse" id="pulse"></span>' if pulse else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)} — Hoglah</title>
<style>{_CSS}</style>
</head>
<body>
<header>
  <nav>
    <a class="brand" href="/">Hoglah</a>
    <span class="muted">queue monitor</span>
    {pulse_html}
    <span class="dbname">{escape(db_label)}</span>
  </nav>
</header>
<main>
{body}
</main>
</body>
</html>"""


def _job_row(job: dict[str, Any]) -> str:
    status = job.get("status") or "unknown"
    tags = ", ".join(job.get("tags") or []) or "—"
    parent = job.get("parent_job_id")
    parent_html = (
        f'<a href="/jobs/{escape(parent)}"><code>{escape(parent[:8])}…</code></a>' if parent else '<span class="muted">—</span>'
    )
    return f"""
<tr>
  <td><a href="/jobs/{escape(job.get("job_id") or "")}"><code>{escape((job.get("job_id") or "")[:8])}…</code></a></td>
  <td><span class="badge {escape(status)}">{escape(status)}</span></td>
  <td><code>{escape(job.get("model") or "?")}</code></td>
  <td>{escape(job.get("step_name") or "—")}</td>
  <td>{escape(tags)}</td>
  <td>{parent_html}</td>
  <td>{escape(job.get("age") or "-")}</td>
  <td class="muted">{escape(job.get("preview") or "")}</td>
</tr>"""


def create_app(db_path: Path | str | None = None) -> FastAPI:
    """Build the monitor app over the given job store (default store when None)."""
    config: dict[str, Any] = {}
    if db_path:
        config["db_path"] = Path(db_path)
    client = Hoglah(config=config, start_worker=False)
    db_label = str(client.config.db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Close the client (and its SQLite handle) when the server stops.

        Without this the store leaked a connection on every uvicorn reload. It
        matters beyond tidiness: the store is shared with background threads, so
        an unclosed handle is the same footgun that produced the #13 segfault.
        """
        yield
        client.close()

    app = FastAPI(
        title="Hoglah",
        description="Read-only queue monitor",
        lifespan=lifespan,
    )
    app.state.client = client

    def _summary(status: str | None, limit: int) -> dict[str, Any]:
        status_filter = JobStatus(status) if status else None
        stats = client.stats()
        jobs = client.list(status=status_filter, limit=limit)
        return {
            "ok": True,
            "stats": stats,
            "jobs": [_result_to_dict(job) for job in jobs],
            "status": status,
            "limit": limit,
        }

    @app.get("/api/summary")
    def api_summary(status: str | None = None, limit: int = 25) -> dict[str, Any]:
        if status and status not in _STATUSES:
            raise HTTPException(400, f"unknown status: {status!r}")
        return _summary(status, limit)

    @app.get("/api/jobs/{job_id}")
    def api_job(job_id: str) -> dict[str, Any]:
        try:
            job = client.get(job_id)
        except KeyError:
            raise HTTPException(404, f"job not found: {job_id}")
        return {"ok": True, "job": _result_to_dict(job)}

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> str:
        """Prometheus scrape endpoint (gap G11). No auth — keep bind local."""
        return client.metrics_text()

    @app.get("/", response_class=HTMLResponse)
    def dashboard(request: Request, status: str | None = None, limit: int = 25) -> str:
        if status and status not in _STATUSES:
            raise HTTPException(400, f"unknown status: {status!r}")
        summary = _summary(status, limit)
        counts = summary["stats"].get("counts", {})
        total = summary["stats"].get("total_jobs", 0)

        def card(label: str, n: int, href: str, key: str | None) -> str:
            active = " active" if key == status else ""
            return (
                f'<a class="card{active}" href="{escape(href)}">'
                f'<div class="n">{n}</div><div class="label">{escape(label)}</div></a>'
            )

        cards = card("total", total, "/", None) + "".join(
            card(s, counts.get(s, 0), f"/?status={s}", s) for s in _STATUSES
        )
        rows = "".join(_job_row(job) for job in summary["jobs"])
        scope = f" · {escape(status)}" if status else ""
        body = f"""
<h1>Queue{scope}</h1>
<div class="summary" id="cards">{cards}</div>
<h2>Recent jobs</h2>
<table>
<thead><tr><th>Job</th><th>Status</th><th>Model</th><th>Step</th><th>Tags</th><th>Parent</th><th>Age</th><th>Preview</th></tr></thead>
<tbody id="jobs">
{rows or '<tr><td colspan="8" class="muted">(no jobs)</td></tr>'}
</tbody>
</table>
<script>
const STATUSES = {_STATUSES!r};
const FILTER = {(status or "")!r};
const LIMIT = {limit};
function esc(s) {{
  return String(s ?? '').replace(/[&<>"]/g, c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}})[c]);
}}
function jobRow(j) {{
  const parent = j.parent_job_id
    ? '<a href="/jobs/' + esc(j.parent_job_id) + '"><code>' + esc(j.parent_job_id.slice(0, 8)) + '…</code></a>'
    : '<span class="muted">—</span>';
  return '<tr>' +
    '<td><a href="/jobs/' + esc(j.job_id) + '"><code>' + esc(j.job_id.slice(0, 8)) + '…</code></a></td>' +
    '<td><span class="badge ' + esc(j.status) + '">' + esc(j.status) + '</span></td>' +
    '<td><code>' + esc(j.model || '?') + '</code></td>' +
    '<td>' + esc(j.step_name || '—') + '</td>' +
    '<td>' + esc((j.tags || []).join(', ') || '—') + '</td>' +
    '<td>' + parent + '</td>' +
    '<td>' + esc(j.age || '-') + '</td>' +
    '<td class="muted">' + esc(j.preview || '') + '</td>' +
    '</tr>';
}}
function card(label, n, href, key) {{
  const active = key === FILTER ? ' active' : '';
  return '<a class="card' + active + '" href="' + href + '">' +
    '<div class="n">' + n + '</div><div class="label">' + esc(label) + '</div></a>';
}}
async function refresh() {{
  try {{
    const qs = new URLSearchParams({{limit: LIMIT}});
    if (FILTER) qs.set('status', FILTER);
    const r = await fetch('/api/summary?' + qs);
    if (!r.ok) return;
    const data = await r.json();
    const counts = data.stats.counts || {{}};
    document.getElementById('cards').innerHTML =
      card('total', data.stats.total_jobs || 0, '/', null) +
      STATUSES.map(s => card(s, counts[s] || 0, '/?status=' + s, s)).join('');
    document.getElementById('jobs').innerHTML =
      data.jobs.map(jobRow).join('') ||
      '<tr><td colspan="8" class="muted">(no jobs)</td></tr>';
    document.getElementById('pulse').textContent =
      'updated ' + new Date().toLocaleTimeString();
  }} catch (e) {{
    document.getElementById('pulse').textContent = 'refresh failed';
  }}
}}
setInterval(refresh, 4000);
</script>
"""
        return _base("Queue", body, db_label, pulse=True)

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(request: Request, job_id: str) -> str:
        try:
            job = client.get(job_id)
        except KeyError:
            raise HTTPException(404, f"job not found: {job_id}")
        d = _result_to_dict(job)
        # The full input lives in the persisted request (package-internal store
        # access — this monitor ships inside hoglah).
        stored = client._store.get(job_id) or {}
        stored_request = stored.get("request") or {}

        timing_rows = "".join(
            f"<tr><th>{escape(str(k))}</th><td class='mono'>{escape(str(v))}</td></tr>"
            for k, v in (d.get("timings") or {}).items()
            if v is not None
        )
        usage = d.get("usage") or {}
        usage_html = (
            " · ".join(f"{escape(str(k))}: {v}" for k, v in usage.items()) if usage else "—"
        )
        parent = d.get("parent_job_id")
        parent_html = (
            f'<a href="/jobs/{escape(parent)}"><code>{escape(parent)}</code></a>' if parent else '<span class="muted">—</span>'
        )
        truncation_html = ""
        if d.get("truncated"):
            truncation_html = (
                f'<tr><th>Truncated</th><td><span class="badge failed">yes</span> '
                f'<span class="muted">{escape(d.get("truncation_reason") or "")}</span></td></tr>'
            )
        embedding_html = ""
        if d.get("embedding_dim"):
            embedding_html = f"<tr><th>Embedding</th><td>{d['embedding_dim']} dimensions</td></tr>"

        # Full input (the debugging view's "In"): prompt or the messages list.
        input_html = ""
        messages = stored_request.get("messages")
        if messages:
            blocks = "".join(
                f'<p class="muted mono">[{escape(str(m.get("role", "?")))}]</p>'
                f"<pre>{escape(str(m.get('content', '')))}</pre>"
                for m in messages
            )
            input_html = f"<h2>Input</h2>{blocks}"
        elif stored_request.get("prompt"):
            input_html = f"<h2>Input</h2><pre>{escape(str(stored_request['prompt']))}</pre>"

        step_html = ""
        if d.get("step_name"):
            step_html = f"<tr><th>Step</th><td>{escape(d['step_name'])}</td></tr>"

        output_html = ""
        if d.get("error"):
            output_html = f"<h2>Error</h2><pre>{escape(d['error'])}</pre>"
        elif d.get("output"):
            output_html = f"<h2>Output</h2><pre>{escape(d['output'])}</pre>"
        elif d.get("embedding_dim"):
            output_html = '<h2>Output</h2><p class="muted">(embedding vector — not rendered)</p>'
        else:
            output_html = '<h2>Output</h2><p class="muted">(no output yet)</p>'

        # G9: failed jobs stay read-only here; requeue is a CLI maintenance action.
        requeue_hint = ""
        if d.get("status") == "failed":
            jid = escape(d["job_id"])
            requeue_hint = (
                f'<p class="muted">Failed job — requeue via CLI: '
                f'<code>hoglah requeue {jid}</code> · '
                f'<a href="/?status=failed">all failed</a></p>'
            )

        body = f"""
<h1>Job <code>{escape(d["job_id"])}</code></h1>
<p><a href="/">&larr; back to queue</a></p>
{requeue_hint}
<table class="kvbox">
<tr><th>Status</th><td><span class="badge {escape(d["status"])}">{escape(d["status"])}</span>
  <span class="muted">({escape(d.get("age") or "-")} ago)</span></td></tr>
<tr><th>Model</th><td><code>{escape(d.get("model") or "?")}</code></td></tr>
{step_html}
<tr><th>Tags</th><td>{escape(", ".join(d.get("tags") or []) or "—")}</td></tr>
<tr><th>Parent job</th><td>{parent_html}</td></tr>
<tr><th>Usage</th><td>{usage_html}</td></tr>
{truncation_html}
{embedding_html}
{timing_rows}
</table>
{input_html}
{output_html}
<p class="muted">Cross-family trace view: <code>galeed trace --call {escape(d["job_id"])}</code></p>
"""
        return _base(f"Job {d['job_id'][:8]}", body, db_label)

    return app
