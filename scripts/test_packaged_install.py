#!/usr/bin/env python3
"""
Smoke test for a freshly packaged Hoglah installation (v0.2.1).

This script is meant to be run from a **clean, non-editable** environment
after installing the built wheel (or from a GitHub release / PyPI).

It proves that the packaged artifact installs correctly and that
core functionality works end-to-end using the installed entry points
and the public Python API.

Usage example (stub, always works):
    python -m venv /tmp/hoglah-smoke
    /tmp/hoglah-smoke/bin/pip install dist/hoglah-0.2.1-py3-none-any.whl[cli]
    /tmp/hoglah-smoke/bin/python scripts/test_packaged_install.py

With your local working Ollama (for full V1 real-path validation):
    RUN_OLLAMA_TESTS=1 /tmp/hoglah-smoke/bin/python scripts/test_packaged_install.py

    # or
    HOGLAH_USE_REAL_ADAPTER=1 /tmp/hoglah-smoke/bin/python scripts/test_packaged_install.py

The script will automatically switch to a real model (gemma3:1b) and exercise
the real adapter (show_model, pull if needed, context auto-detection from model,
full submit + wait, etc.).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

TEST_DB = Path(tempfile.mkdtemp(prefix="hoglah-packaged-test-")) / "test.db"


def run_cli(*args, check=True, capture=False, needs_db: bool = True) -> subprocess.CompletedProcess:
    """Run the installed 'hoglah' console script.
    
    Set needs_db=False for commands that don't take --db (e.g. version).
    """
    hoglah_cmd = str(Path(sys.executable).parent / "hoglah")
    cmd = [hoglah_cmd] + list(args)
    if needs_db:
        cmd += ["--db", str(TEST_DB)]
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONUNBUFFERED": "1"},
    )
    if result.stdout.strip():
        print("    stdout:", result.stdout.strip()[:400])
    if result.stderr.strip():
        print("    stderr:", result.stderr.strip()[:400])
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed with exit {result.returncode}: {cmd}")
    return result


def main() -> None:
    print("=== Hoglah Packaged Install Smoke Test (v0.2.1) ===")
    print(f"Python: {sys.executable}")
    print(f"Test DB: {TEST_DB}")
    print()

    # 1. Import the installed package
    print("1. Importing installed package...")
    import hoglah
    from hoglah import Hoglah, JobStatus
    print(f"   Installed version: {hoglah.__version__}")
    assert hoglah.__version__ == "0.2.1", "Version mismatch in packaged install!"

    # 2. Create instance
    print("\n2. Creating Hoglah instance...")
    use_real = bool(os.environ.get("RUN_OLLAMA_TESTS") == "1" or os.environ.get("HOGLAH_USE_REAL_ADAPTER"))
    # Always start the worker in the test so library submits get processed (important for real mode)
    h = Hoglah(config={"db_path": TEST_DB, "log_level": "WARNING"}, start_worker=True, use_real=use_real)
    print(f"   Adapter: {type(h.adapter).__name__}")
    info = h.info()
    print(f"   Version via .info(): {info.get('version')}")
    print(f"   Using real Ollama: {use_real}")

    if use_real:
        print("\n   [Real mode] Direct library submit + wait to test real adapter + context auto-detect...")
        real_job = h.submit(
            prompt="What is 2 + 2? Reply with just the number.",
            model=model,
            max_retries=0,
            # deliberately omit num_ctx to test auto from model via show_model
        )
        real_res = h.wait(real_job, timeout=60)
        print(f"     Real library job status: {real_res.status}")
        print(f"     effective_num_ctx: {real_res.effective_num_ctx}")
        print(f"     truncated: {real_res.truncated}")
        assert real_res.status == JobStatus.COMPLETED
        print("     ✓ Real library submit+wait + context handling succeeded")

    # 3. CLI submit + --wait (CLI starts its own worker — most realistic fresh-install path)
    print("\n3. CLI submit with --wait (exercises installed CLI entry point + worker)...")
    model = "gemma3:1b" if use_real else "stub-test:1b"
    submit_result = run_cli(
        "submit", "This is a packaged smoke test prompt.",
        "--model", model,
        "--tag", "smoke,packaged",
        "--wait", "--timeout", "30" if use_real else "15",
        capture=True,
    )
    print(f"   Submit + wait via installed CLI succeeded (model={model}).")

    # Capture job ID from output for verification (works in both stub and real)
    submitted_id = None
    for line in (submit_result.stdout or "").splitlines():
        if "Submitted:" in line:
            submitted_id = line.split("Submitted:")[-1].strip()
            break
    if submitted_id:
        print(f"   Captured job ID: {submitted_id}")
        if use_real:
            # Verify via the library that the real adapter set effective_num_ctx
            res = h.get(submitted_id)
            print(f"   Real result effective_num_ctx: {res.effective_num_ctx}")
            if res.effective_num_ctx and res.effective_num_ctx > 0:
                print("   ✓ Real adapter (llama.cpp via Ollama) populated effective_num_ctx from model info")

    # 4. Submit with parent via CLI, then filter list by parent
    print("\n4. Parent/child via CLI + list --parent filter...")
    run_cli("submit", "Parent task for filter test", "--model", model, "--wait")
    # We don't capture the exact parent ID easily here, so just exercise the filter path
    run_cli("list", "--parent", "nonexistent-parent-123", "--json")
    print("   list --parent via CLI executed successfully.")

    # 5. Direct library API (stats, info, show_model)
    print("\n5. Library API smoke (stats, info, show_model)...")
    stats = h.stats()
    model_info = h.show_model(model)
    print(f"   stats total_jobs: {stats['total_jobs']}")
    print(f"   show_model has keys: {list(model_info.keys())[:4]}")

    if use_real:
        print("   Real Ollama (llama.cpp) checks:")
        params = str(model_info.get("parameters", "") or model_info.get("details", ""))
        print(f"     Model info parameters/details snippet: {params[:150]}...")
        if "num_ctx" in params.lower() or "context" in params.lower():
            print("     ✓ Model reports context size info (used for auto num_ctx if not specified)")
        print("     (Previous real submit without num_ctx should have effective_num_ctx populated from this)")

    # 6. CLI inspection commands (installed entry point)
    print("\n6. CLI inspection commands (installed 'hoglah' binary)...")
    run_cli("version", needs_db=False)
    run_cli("info", "--json")
    run_cli("stats", "--json")
    run_cli("models")
    run_cli("show", model, "--json")

    # 7. rm and clear via CLI
    print("\n7. Cleanup via rm and clear (CLI)...")
    run_cli("rm", "nonexistent-demo-id", "--yes", check=False)
    run_cli("clear", "--status", "completed", "--yes")
    print("   rm + clear via installed CLI succeeded.")

    # 8. wait command (standalone)
    print("\n8. Standalone wait command (CLI)...")
    # Use a non-existent ID so it fails fast with clear error
    result = run_cli("wait", "nonexistent-for-wait-test", "--timeout", "0.5", check=False, capture=True)
    assert result.returncode != 0
    print("   wait command executed (expected non-zero for non-existent job).")

    print("\n=== Packaged smoke test PASSED ===")
    print(f"Installed hoglah version: {hoglah.__version__}")
    print(f"Test artifacts left in: {TEST_DB.parent} (safe to delete)")


if __name__ == "__main__":
    main()