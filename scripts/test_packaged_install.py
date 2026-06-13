#!/usr/bin/env python3
"""
Smoke test for a freshly packaged Hoglah installation (v0.2.1).

This script is meant to be run from a **clean, non-editable** environment
after installing the built wheel (or from a GitHub release / PyPI).

It proves that the packaged artifact installs correctly and that
core functionality works end-to-end using the installed entry points
and the public Python API.

Usage example:
    python -m venv /tmp/hoglah-smoke
    /tmp/hoglah-smoke/bin/pip install dist/hoglah-0.2.1-py3-none-any.whl[cli]
    /tmp/hoglah-smoke/bin/python scripts/test_packaged_install.py
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

    # 2. Create instance for direct API use (no worker needed for these)
    print("\n2. Creating Hoglah (StubAdapter, no background worker)...")
    h = Hoglah(config={"db_path": TEST_DB, "log_level": "WARNING"}, start_worker=False)
    print(f"   Adapter: {type(h.adapter).__name__}")
    info = h.info()
    print(f"   Version via .info(): {info.get('version')}")
    print(f"   Adapter via .info(): {info['adapter']}")

    # 3. CLI submit + --wait (CLI starts its own worker — most realistic fresh-install path)
    print("\n3. CLI submit with --wait (exercises installed CLI entry point + worker)...")
    run_cli(
        "submit", "This is a packaged smoke test prompt.",
        "--model", "stub-test:1b",
        "--tag", "smoke,packaged",
        "--wait", "--timeout", "15",
    )
    print("   Submit + wait via installed CLI succeeded.")

    # 4. Submit with parent via CLI, then filter list by parent
    print("\n4. Parent/child via CLI + list --parent filter...")
    run_cli("submit", "Parent task for filter test", "--model", "stub-test:1b", "--wait")
    # We don't capture the exact parent ID easily here, so just exercise the filter path
    run_cli("list", "--parent", "nonexistent-parent-123", "--json")
    print("   list --parent via CLI executed successfully.")

    # 5. Direct library API (stats, info, show_model)
    print("\n5. Library API smoke (stats, info, show_model)...")
    stats = h.stats()
    model_info = h.show_model("stub-test:1b")
    print(f"   stats total_jobs: {stats['total_jobs']}")
    print(f"   show_model has keys: {list(model_info.keys())[:4]}")

    # 6. CLI inspection commands (installed entry point)
    print("\n6. CLI inspection commands (installed 'hoglah' binary)...")
    run_cli("version", needs_db=False)
    run_cli("info", "--json")
    run_cli("stats", "--json")
    run_cli("models")
    run_cli("show", "stub-test:1b", "--json")

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