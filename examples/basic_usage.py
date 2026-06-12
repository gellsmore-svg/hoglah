#!/usr/bin/env python3
"""
Hoglah basic usage example.

This script exercises the main library surface using the safe default
StubAdapter (no Ollama server required).

Run:
    python examples/basic_usage.py

For real Ollama inference (when a server is available):
    HOGLAH_USE_REAL_ADAPTER=1 python examples/basic_usage.py
    # or
    python -c '
    from hoglah import Hoglah
    h = Hoglah(use_real=True)  # requires Ollama running
    ...
    '
"""

from __future__ import annotations

from pathlib import Path
import tempfile

from hoglah import Hoglah, JobResult, JobStatus


def _temp_db() -> Path:
    td = tempfile.mkdtemp(prefix="hoglah-example-")
    return Path(td) / "example.db"


def main() -> None:
    db = _temp_db()

    # ------------------------------------------------------------------ #
    # Use context manager for automatic worker shutdown (recommended)
    # ------------------------------------------------------------------ #
    with Hoglah(config={"db_path": db}, start_worker=True) as h:
        job_id = h.submit(
            prompt="Explain in one sentence why Hoglah was named after a biblical figure.",
            model="gemma3:1b",   # any name works with the stub; real adapter uses this literally
            tags=["example", "bible"],
            temperature=0.2,
            num_ctx=2048,
        )
        print("Submitted (prompt style):", job_id)
        print("  status immediately:", h.status(job_id))

        # ------------------------------------------------------------------ #
        # Wait for result (polling helper)
        # ------------------------------------------------------------------ #
        result = h.wait(job_id, timeout=60)
        print("  final status:", result.status)
        print("  output (truncated):", (result.output or "")[:120], "...")
        if result.truncated:
            print("  (was truncated:", result.truncation_reason, ")")

        # ------------------------------------------------------------------ #
        # Chat-style submission using messages
        # ------------------------------------------------------------------ #
        chat_job = h.submit(
            messages=[
                {"role": "system", "content": "You are a very terse assistant."},
                {"role": "user", "content": "What is the capital of France?"},
            ],
            model="gemma3:1b",
            tags=["chat"],
        )
        chat_res = h.wait(chat_job)
        print("\nChat job result:")
        print("  ", (chat_res.output or "")[:140])

        # ------------------------------------------------------------------ #
        # Callback (direct callable, lives only for this process)
        # ------------------------------------------------------------------ #
        received: list[JobResult] = []

        def on_done(res: JobResult) -> None:
            received.append(res)
            print(f"\n[callback] Job {res.job_id} completed with status {res.status.value}")

        cb_job = h.submit(
            prompt="Short fact about the tribe of Manasseh.",
            model="gemma3:1b",
            callback=on_done,
            tags=["callback-demo"],
        )
        h.wait(cb_job)
        assert len(received) == 1

        # ------------------------------------------------------------------ #
        # Named callback registry (survives restart simulation)
        # ------------------------------------------------------------------ #
        calls: list[JobResult] = []

        def my_named_handler(res: JobResult) -> None:
            calls.append(res)

        with Hoglah(
            config={"db_path": db},
            callbacks={"my_handler": my_named_handler},
            start_worker=True,
        ) as h2:
            named_job = h2.submit(
                prompt="Another quick fact.",
                model="gemma3:1b",
                callback="my_handler",   # string = lookup in the registry
            )
            h2.wait(named_job)
            print("\nNamed callback registry delivered:", len(calls) == 1)

        # ------------------------------------------------------------------ #
        # Inspection
        # ------------------------------------------------------------------ #
        print("\nRecent jobs (via list):")
        for j in h.list(limit=6):
            print(f"  {j.job_id[:8]}...  {j.status.value:12}  model={j.model}  tags={j.tags}")

        # Cancel example (on a still-queued job would work; here most are done)
        # h.cancel(some_id)

        print("\nExample completed successfully (using", type(h.adapter).__name__ + ").")


if __name__ == "__main__":
    main()