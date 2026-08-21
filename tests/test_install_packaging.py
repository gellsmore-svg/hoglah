"""Packaging: CLI works without the [cli] extra; __main__ exists."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cli_import_does_not_need_extra():
    import hoglah.cli as cli

    assert callable(cli.main)


def test_main_module_exists():
    import hoglah.__main__ as mainmod

    assert callable(mainmod.main)


def test_python_m_hoglah_version():
    proc = subprocess.run(
        [sys.executable, "-m", "hoglah", "--version"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "hoglah" in (proc.stdout + proc.stderr).lower()


def test_install_sh_help_and_from_path():
    script = ROOT / "scripts" / "install.sh"
    assert script.is_file()
    text = script.read_text(encoding="utf-8")
    assert "pipx" in text
    assert "--purge" in text
    assert "pipx install --force" in text
    assert 'PATH="$ORIG_PATH"' in text
    proc = subprocess.run(
        ["bash", str(script), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert "--from" in proc.stdout
    assert "--purge" in proc.stdout
    proc = subprocess.run(
        ["bash", str(script), "--from", "/no/such/hoglah-artifact"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "not found" in (proc.stdout + proc.stderr).lower()


def test_windows_builder_derives_stdlib_and_pins_sha():
    script = ROOT / "scripts" / "build-windows-portable.sh"
    text = script.read_text(encoding="utf-8")
    assert "NODOT" in text
    assert "--no-compile" in text
    assert "if not defined HOGLAH_HOME" in text
    assert "DEFAULT_EMBED_SHA256" in text
    assert "dist-win" in text
    assert "python${NODOT}.zip" in text
    assert "python312.zip" not in text


def test_release_keeps_win_zip_out_of_pypi_dist():
    text = (ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")
    assert "dist-win" in text
    assert "gh-action-pypi-publish" in text
    assert "dist-win/*.zip" in text
    assert "files: dist/*" not in text
