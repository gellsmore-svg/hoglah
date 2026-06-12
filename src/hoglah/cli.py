"""Basic CLI for hoglah (Chunk 1).

Uses the library for basic inspection commands. Execution-related
commands (submit with real work, models, etc.) will be fleshed out
once the worker + Ollama adapter are implemented.
"""

from __future__ import annotations

from pathlib import Path

import typer

from . import Hoglah, __version__
from .models import JobStatus

app = typer.Typer(
    name="hoglah",
    help="Lightweight local-first job queue for Ollama.",
    add_completion=False,
)


def _get_hoglah(db: Path | None = None) -> Hoglah:
    cfg = {}
    if db:
        cfg["db_path"] = db
    return Hoglah(config=cfg)


@app.command()
def version() -> None:
    """Show version."""
    print(f"hoglah {__version__}")


@app.command("list")
def list_jobs(
    status: str | None = typer.Option(None, "--status", "-s", help="Filter by status (queued,processing,completed,...)"),
    limit: int = typer.Option(20, "--limit", "-l"),
    db: Path | None = typer.Option(None, "--db", help="Override database path"),
) -> None:
    """List recent jobs."""
    h = _get_hoglah(db)
    st = JobStatus(status) if status else None
    jobs = h.list(status=st, limit=limit)
    if not jobs:
        print("No jobs found.")
        return

    for j in jobs:
        print(f"{j.job_id}  {j.status.value:12}  model={j.model or '?'}  tags={j.tags}")


@app.command()
def status(job_id: str, db: Path | None = typer.Option(None, "--db")) -> None:
    """Show status and basic info for a job."""
    h = _get_hoglah(db)
    try:
        res = h.get(job_id)
    except KeyError:
        typer.secho(f"Job not found: {job_id}", fg=typer.colors.RED)
        raise typer.Exit(1)

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


def main() -> None:
    """Entry point for the console script."""
    app()


if __name__ == "__main__":
    main()