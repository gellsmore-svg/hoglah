"""CLI for Hoglah.

Provides inspection (list/status/cancel), submission, model discovery, and
a foreground worker runner (`run`).

By default uses the safe StubAdapter (no real LLM calls). To drive real
Ollama inference from the CLI, pass --real (or set HOGLAH_USE_REAL_ADAPTER=1)
and ensure an Ollama server is reachable (see Hoglah( adapter=OllamaAdapter(...) )
for library usage of the real path).
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import typer

from dataclasses import asdict

from . import Hoglah, __version__
from .models import JobResult, JobStatus

app = typer.Typer(
    name="hoglah",
    help="Lightweight local-first job queue for Ollama.",
    add_completion=False,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"hoglah {__version__}")
        raise typer.Exit()


@app.callback()
def _cli(
    version: bool = typer.Option(
        None,
        "--version",
        "-V",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """Hoglah CLI root callback (for --version etc.)."""
    pass


def _result_to_dict(res: JobResult) -> dict:
    """Convert JobResult to JSON-serializable dict (handle enums, datetimes).
    Adds a 'preview' field for quick scripting (output or error snippet).
    """
    d = asdict(res)
    d["status"] = res.status.value
    timings = d.get("timings") or {}
    for k, v in list(timings.items()):
        if v is not None and hasattr(v, "isoformat"):
            timings[k] = v.isoformat()
    d["timings"] = timings

    # Add useful preview for JSON consumers
    if res.error:
        preview = "ERROR: " + res.error[:120]
    elif res.output:
        preview = res.output[:120] + ("..." if len(res.output) > 120 else "")
    else:
        preview = ""
    d["preview"] = preview
    return d


def _get_hoglah(
    db: Path | None = None,
    *,
    real: bool = False,
    ollama_host: str | None = None,
) -> Hoglah:
    """Factory used by CLI commands. Respects --real / HOGLAH_USE_REAL_ADAPTER."""
    cfg: dict[str, Any] = {}
    if db:
        cfg["db_path"] = db
    if ollama_host:
        cfg["ollama_host"] = ollama_host

    # use_real= is the clean way; adapter= can still be passed for advanced cases
    return Hoglah(config=cfg, use_real=real)


@app.command()
def version() -> None:
    """Show version."""
    print(f"hoglah {__version__}")


@app.command("list")
def list_jobs(
    status: str | None = typer.Option(None, "--status", "-s", help="Filter by status (queued,processing,completed,...)"),
    parent: str | None = typer.Option(None, "--parent", "-p", help="Filter by parent job ID"),
    limit: int = typer.Option(20, "--limit", "-l"),
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """List recent jobs."""
    h = _get_hoglah(db)
    st = JobStatus(status) if status else None
    jobs = h.list(status=st, parent_job_id=parent, limit=limit)
    if not jobs:
        if json_out:
            print("[]")
        else:
            print("No jobs found.")
        return

    if json_out:
        data = [_result_to_dict(j) for j in jobs]
        print(json.dumps(data, indent=2, default=str))
        return

    # Simple aligned output (no extra dependencies)
    # Include PARENT column if any jobs have a parent (for chaining visibility)
    has_parent = any(j.parent_job_id for j in jobs)
    if has_parent:
        print(f"{'JOB_ID':<38}  {'STATUS':<12}  {'MODEL':<18}  {'PARENT':<12}  TAGS")
        print("-" * 92)
        for j in jobs:
            model = (j.model or "?")[:18]
            parent = (j.parent_job_id or "")[:12] if j.parent_job_id else "-"
            tags = ",".join(j.tags) if j.tags else "-"
            print(f"{j.job_id:<38}  {j.status.value:<12}  {model:<18}  {parent:<12}  {tags}")
    else:
        print(f"{'JOB_ID':<38}  {'STATUS':<12}  {'MODEL':<18}  TAGS")
        print("-" * 80)
        for j in jobs:
            model = (j.model or "?")[:18]
            tags = ",".join(j.tags) if j.tags else "-"
            print(f"{j.job_id:<38}  {j.status.value:<12}  {model:<18}  {tags}")


@app.command("ps", help="List recent jobs (ps alias, like process listing).")
def ps_jobs(
    status: str | None = typer.Option(None, "--status", "-s", help="Filter by status (queued,processing,completed,...)"),
    parent: str | None = typer.Option(None, "--parent", "-p", help="Filter by parent job ID"),
    limit: int = typer.Option(20, "--limit", "-l"),
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """Alias for 'list' (convenience for queue 'ps')."""
    list_jobs(status=status, parent=parent, limit=limit, db=db, json_out=json_out)


@app.command()
def stats(
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """Show queue statistics (counts by status, totals)."""
    h = _get_hoglah(db)
    s = h.stats()
    if json_out:
        print(json.dumps(s, indent=2))
        return

    print("Hoglah Queue Stats")
    print("-" * 30)
    for k, v in s["counts"].items():
        print(f"{k:12} : {v}")
    print("-" * 30)
    print(f"{'total':12} : {s['total_jobs']}")


@app.command()
def info(
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """Show instance info: config, adapter in use, and queue stats."""
    h = _get_hoglah(db)
    i = h.info()
    if json_out:
        print(json.dumps(i, indent=2))
        return

    print("Hoglah Info")
    print("-" * 30)
    print(f"version     : {i.get('version', 'unknown')}")
    print(f"adapter     : {i['adapter']}")
    print(f"db_path     : {i['config']['db_path']}")
    print(f"concurrency : {i['config']['concurrency']}")
    print(f"log_level   : {i['config'].get('log_level', 'INFO')}")
    print(f"ollama_host : {i['config'].get('ollama_host') or 'default'}")
    print("\nStats:")
    for k, v in i["stats"]["counts"].items():
        print(f"  {k:12} : {v}")
    print(f"  {'total':12} : {i['stats']['total_jobs']}")


@app.command()
def clear(
    status: str | None = typer.Option(
        None, "--status", "-s", help="Only clear jobs with this status (e.g. completed, failed)"
    ),
    older_than: int | None = typer.Option(
        None, "--older-than", help="Only clear jobs last updated more than N days ago"
    ),
    db: Path | None = typer.Option(None, "--db"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Remove old or terminal jobs from the database (maintenance)."""
    h = _get_hoglah(db)
    st = JobStatus(status) if status else None
    count = h.clear(status=st, older_than_days=older_than)
    if count == 0:
        print("No jobs matched the clear criteria.")
        return
    if not yes:
        confirm = typer.confirm(f"Delete {count} job(s)?", default=False)
        if not confirm:
            typer.secho("Clear cancelled.", fg=typer.colors.YELLOW)
            return
    print(f"Cleared {count} job(s).")


@app.command("rm")
def rm_job(
    job_id: str = typer.Argument(..., help="Job ID to remove"),
    db: Path | None = typer.Option(None, "--db"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON result instead of human text"),
) -> None:
    """Remove a specific job by ID (maintenance)."""
    h = _get_hoglah(db)
    if not yes:
        confirm = typer.confirm(f"Delete job {job_id}?", default=False)
        if not confirm:
            typer.secho("Cancelled.", fg=typer.colors.YELLOW)
            return
    removed = h.remove(job_id)
    if json_out:
        print(json.dumps({"job_id": job_id, "removed": removed}, indent=2))
        if not removed:
            raise typer.Exit(1)
        return
    if removed:
        typer.secho(f"Removed {job_id}", fg=typer.colors.GREEN)
    else:
        typer.secho(f"Job not found: {job_id}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def pull(
    model: str = typer.Argument(..., help="Model name to pull (e.g. gemma3:1b)"),
    real: bool = typer.Option(False, "--real", help="Use real Ollama (default is stub which does nothing)"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
) -> None:
    """Ensure a model is pulled (useful before submit with --real)."""
    if not real:
        typer.secho("pull: --real not specified; stub does nothing. Use --real to pull from Ollama.", fg=typer.colors.YELLOW)
        return

    from .adapters import OllamaAdapter
    adapter = OllamaAdapter(host=ollama_host)
    import asyncio
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(adapter.pull_model(model))
        loop.close()
        typer.secho(f"Pulled (or already present): {model}", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Pull failed for {model}: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def doctor(
    db: Path | None = typer.Option(None, "--db"),
    real: bool = typer.Option(False, "--real", help="Check real Ollama connectivity"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
) -> None:
    """Diagnose Hoglah setup and connectivity (useful for real Ollama/llama.cpp)."""
    import os
    print("Hoglah Doctor")
    print("-" * 30)
    print(f"Version: {__version__}")

    use_real = real or os.environ.get("HOGLAH_USE_REAL_ADAPTER") == "1"
    cfg = {}
    if db: cfg["db_path"] = db
    if ollama_host: cfg["ollama_host"] = ollama_host

    try:
        h = Hoglah(config=cfg, use_real=use_real, start_worker=False)
        i = h.info()
        print(f"Adapter: {i['adapter']}")
        print(f"DB: {i['config']['db_path']}")
        print(f"Log level: {i['config'].get('log_level', 'INFO')}")
        print(f"Concurrency: {i['config']['concurrency']}")
        print("Instance created OK.")
    except Exception as e:
        typer.secho(f"Failed to create Hoglah: {e}", fg=typer.colors.RED)
        raise typer.Exit(1)

    if use_real:
        print("\nReal adapter checks (llama.cpp via Ollama):")
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            models = loop.run_until_complete(h.adapter.list_models())
            loop.close()
            print(f"  Reachable. {len(models)} model(s) visible.")
            if models:
                print(f"  Example: {models[0].get('name')}")
        except Exception as e:
            typer.secho(f"  Connectivity issue: {e}", fg=typer.colors.RED)
            print("  Tip: Ensure Ollama is running and listening (OLLAMA_HOST or default :11434)")
            print("  In WSL: make sure it's bound to 0.0.0.0 if needed from other contexts.")

    print("\nDoctor complete.")


@app.command()
def status(
    job_id: str,
    db: Path | None = typer.Option(None, "--db"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """Show status and basic info for a job."""
    h = _get_hoglah(db)
    try:
        res = h.get(job_id)
    except KeyError:
        typer.secho(f"Job not found: {job_id}", fg=typer.colors.RED)
        raise typer.Exit(1)

    if json_out:
        print(json.dumps(_result_to_dict(res), indent=2, default=str))
        return

    print(f"ID:     {res.job_id}")
    print(f"Status: {res.status.value}")
    print(f"Model:  {res.model or '-'}")
    if res.error:
        print(f"Error:  {res.error}")
    if res.output:
        preview = res.output[:200].replace("\n", " ")
        print(f"Output: {preview}..." if len(res.output) > 200 else f"Output: {res.output}")


@app.command()
def cancel(job_id: str, db: Path | None = typer.Option(None, "--db")) -> None:
    """Cancel a job (best-effort)."""
    h = _get_hoglah(db)
    if h.cancel(job_id):
        typer.secho(f"Cancelled {job_id}", fg=typer.colors.GREEN)
    else:
        typer.secho(f"Could not cancel {job_id} (already terminal or not found)", fg=typer.colors.YELLOW)


@app.command()
def wait(
    job_id: str = typer.Argument(..., help="Job ID to wait for"),
    timeout: float | None = typer.Option(None, "--timeout", "-t", help="Max seconds to wait"),
    db: Path | None = typer.Option(None, "--db"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON result instead of human text"),
) -> None:
    """Block until the job reaches a terminal state, then print result (or error)."""
    h = _get_hoglah(db)
    try:
        res = h.wait(job_id, timeout=timeout)
        if json_out:
            print(json.dumps(_result_to_dict(res), indent=2, default=str))
            return
        if res.status == JobStatus.COMPLETED:
            if res.output:
                print(res.output)
            if res.truncated:
                typer.secho(f"[note: truncated, reason={res.truncation_reason}]", fg=typer.colors.YELLOW)
        else:
            typer.secho(f"Job {res.status.value}", fg=typer.colors.YELLOW)
            if res.error:
                print("Error:", res.error)
    except TimeoutError:
        typer.secho(f"Timed out waiting for {job_id}", fg=typer.colors.RED)
        raise typer.Exit(1)
    except KeyError:
        typer.secho(f"Job not found: {job_id}", fg=typer.colors.RED)
        raise typer.Exit(1)


@app.command()
def submit(
    prompt: str | None = typer.Argument(None, help="Prompt text (generate style). Provide --messages-json for chat style."),
    model: str = typer.Option(..., "--model", "-m", help="Model name, e.g. gemma3:1b, llama3.2"),
    system_prompt: str | None = typer.Option(None, "--system", "-s", help="System prompt / instructions"),
    messages_json: str | None = typer.Option(None, "--messages", "--messages-json", help='Chat messages as JSON array, e.g. \'[{"role":"user","content":"hi"}]\'' ),
    tags: str | None = typer.Option(None, "--tag", "-t", help="Comma-separated tags, e.g. research,example"),
    # Generation / sampling flags (passed through to Ollama)
    temperature: float | None = typer.Option(None, "--temperature"),
    top_p: float | None = typer.Option(None, "--top-p"),
    top_k: int | None = typer.Option(None, "--top-k"),
    num_ctx: int | None = typer.Option(None, "--num-ctx", help="Context window size in tokens"),
    num_predict: int | None = typer.Option(None, "--num-predict", help="Max tokens to generate"),
    seed: int | None = typer.Option(None, "--seed", help="For reproducible output"),
    repeat_penalty: float | None = typer.Option(None, "--repeat-penalty"),
    format: str | None = typer.Option(None, "--format", help='e.g. "json"'),
    keep_alive: str | None = typer.Option(None, "--keep-alive", help='Model keep-alive, e.g. "5m" or -1'),
    # Additional traceability / user data (from full submit API)
    metadata: str | None = typer.Option(None, "--metadata", help="User metadata as JSON object, e.g. '{\"key\":\"value\"}'"),
    parent_job_id: str | None = typer.Option(None, "--parent-job-id", help="Parent job ID for chaining/traceability"),
    # CLI control
    db: Path | None = typer.Option(None, "--db"),
    real: bool = typer.Option(False, "--real", help="Use real Ollama (requires server); default is safe stub"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Block and print final output (or error)"),
    timeout: float = typer.Option(180.0, "--timeout", help="Max seconds to wait when --wait is used"),
) -> None:
    """Submit a job and immediately print its ID. Use --wait to see the result.

    Examples:
        hoglah submit "Tell me about Hoglah" --model gemma3:1b --wait
        hoglah submit --model llama3.2 --messages '[{"role":"user","content":"hi"}]' --temperature 0.7
        hoglah submit "..." --model x --metadata '{"source":"agent1"}' --parent-job-id abc-123
    """
    tag_list = [t.strip() for t in tags.split(",")] if tags else None

    # Parse messages if provided
    messages: list[dict[str, Any]] | None = None
    if messages_json:
        try:
            parsed = json.loads(messages_json)
            if not isinstance(parsed, list):
                raise ValueError("messages must be a JSON array")
            messages = parsed
        except Exception as e:
            typer.secho(f"Invalid --messages JSON: {e}", fg=typer.colors.RED)
            raise typer.Exit(1)

    # Parse metadata if provided
    meta_dict: dict[str, Any] | None = None
    if metadata:
        try:
            parsed_meta = json.loads(metadata)
            if not isinstance(parsed_meta, dict):
                raise ValueError("metadata must be a JSON object")
            meta_dict = parsed_meta
        except Exception as e:
            typer.secho(f"Invalid --metadata JSON: {e}", fg=typer.colors.RED)
            raise typer.Exit(1)

    # Basic validation: we need either prompt or messages
    if not prompt and not messages:
        typer.secho("Error: provide a PROMPT argument or --messages (JSON).", fg=typer.colors.RED)
        raise typer.Exit(1)

    h = _get_hoglah(db, real=real, ollama_host=ollama_host)

    job_id = h.submit(
        prompt=prompt,
        messages=messages,
        model=model,
        system_prompt=system_prompt,
        tags=tag_list,
        temperature=temperature,
        top_p=top_p,
        top_k=top_k,
        num_ctx=num_ctx,
        num_predict=num_predict,
        seed=seed,
        repeat_penalty=repeat_penalty,
        format=format,
        keep_alive=keep_alive,
        metadata=meta_dict,
        parent_job_id=parent_job_id,
    )
    typer.secho(f"Submitted: {job_id}", fg=typer.colors.GREEN)

    if wait:
        try:
            res = h.wait(job_id, timeout=timeout)
            if res.status == JobStatus.COMPLETED:
                if res.output:
                    print(res.output)
                if res.truncated:
                    typer.secho(f"[note: truncated, reason={res.truncation_reason}]", fg=typer.colors.YELLOW)
            else:
                typer.secho(f"Job {res.status.value}", fg=typer.colors.YELLOW)
                if res.error:
                    print("Error:", res.error)
        except TimeoutError:
            typer.secho(f"Timed out waiting for {job_id}", fg=typer.colors.RED)
            raise typer.Exit(1)


@app.command()
def run(
    db: Path | None = typer.Option(None, "--db"),
    real: bool = typer.Option(False, "--real", help="Use the real Ollama adapter"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
    concurrency: int | None = typer.Option(None, "--concurrency", "-c"),
) -> None:
    """Run the background worker in the foreground (blocks until interrupted).

    Useful for dedicated queue processor processes or during development.
    """
    cfg: dict[str, Any] = {}
    if db:
        cfg["db_path"] = db
    if concurrency is not None:
        cfg["concurrency"] = concurrency
    if ollama_host:
        cfg["ollama_host"] = ollama_host

    h = Hoglah(config=cfg, use_real=real)

    typer.secho("Hoglah worker running (foreground). Press Ctrl-C to stop.", fg=typer.colors.BLUE)
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        typer.secho("\nShutting down...", fg=typer.colors.YELLOW)
    finally:
        h.close()


@app.command()
def models(
    db: Path | None = typer.Option(None, "--db", help="(unused for models but accepted for uniformity)"),
    real: bool = typer.Option(False, "--real", help="Query real Ollama server for models"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """List models known to the adapter (stub by default; --real for Ollama)."""
    h = _get_hoglah(db, real=real, ollama_host=ollama_host)

    # list_models is sync wrapper around async in adapter; run it
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        model_list = loop.run_until_complete(h.adapter.list_models())
        loop.close()
    except Exception as exc:
        typer.secho(f"Failed to list models: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1)

    if not model_list:
        print("No models reported (stub adapter or empty server response).")
        return

    if json_out:
        print(json.dumps(model_list, indent=2, default=str))
        return

    for m in model_list:
        name = m.get("name") or m.get("model") or str(m)
        size = m.get("size")
        size_str = f" ({size} bytes)" if size else ""
        print(f"{name}{size_str}")


@app.command()
def show(
    model: str = typer.Argument(..., help="Model name to inspect (e.g. gemma3:1b)"),
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
    real: bool = typer.Option(False, "--real", help="Query real Ollama server"),
    ollama_host: str | None = typer.Option(None, "--ollama-host"),
    json_out: bool = typer.Option(False, "--json", help="Emit JSON instead of human text"),
) -> None:
    """Show details for a model (context size, template, etc.)."""
    h = _get_hoglah(db, real=real, ollama_host=ollama_host)

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        details = loop.run_until_complete(h.adapter.show_model(model))
        loop.close()
    except Exception as exc:
        typer.secho(f"Failed to show model {model}: {exc}", fg=typer.colors.RED)
        raise typer.Exit(1)

    if json_out:
        print(json.dumps(details, indent=2, default=str))
        return

    print(f"Model: {details.get('name') or details.get('model') or model}")
    for key in ("size", "digest", "parameters", "template"):
        if key in details:
            val = details[key]
            if isinstance(val, dict):
                print(f"{key}: {val}")
            else:
                print(f"{key}: {val}")
    if "details" in details:
        print(f"details: {details['details']}")


def main() -> None:
    """Entry point for the console script."""
    app()


if __name__ == "__main__":
    main()