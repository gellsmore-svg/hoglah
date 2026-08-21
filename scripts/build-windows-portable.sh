#!/usr/bin/env bash
# Assemble a portable Windows zip on Linux using the official embeddable
# CPython and Windows wheels. Does not freeze an .exe (no PyInstaller).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PY_VER="${HOGLAH_EMBED_PYTHON:-3.12.10}"
# Official CPython SBOM checksum for python-3.12.10-embed-amd64.zip
# (SPDXRef-PACKAGE-cpython in python-3.12.10-embed-amd64.zip.spdx.json).
DEFAULT_EMBED_SHA256="4acbed6dd1c744b0376e3b1cf57ce906f9dc9e95e68824584c8099a63025a3c3"
EMBED_URL="https://www.python.org/ftp/python/${PY_VER}/python-${PY_VER}-embed-amd64.zip"
WHEEL="${1:-}"
OUT_DIR="${2:-$ROOT/dist-win}"

if [[ -z "$WHEEL" ]]; then
  echo "usage: $0 dist/hoglah-<ver>-py3-none-any.whl [outdir]" >&2
  echo "Build the wheel first: python -m build --wheel" >&2
  exit 2
fi
if [[ ! -f "$WHEEL" ]]; then
  echo "wheel not found: $WHEEL" >&2
  exit 1
fi
WHEEL="$(cd "$(dirname "$WHEEL")" && pwd)/$(basename "$WHEEL")"

MM="${PY_VER%.*}"
NODOT="${MM//./}"
if [[ ! "$MM" =~ ^[0-9]+\.[0-9]+$ || ! "$NODOT" =~ ^[0-9]+$ ]]; then
  echo "unusable HOGLAH_EMBED_PYTHON=${PY_VER} (expected e.g. 3.12.10)" >&2
  exit 1
fi

VER="$(python3 - <<'PY'
from pathlib import Path
import re
text = Path("pyproject.toml").read_text(encoding="utf-8")
m = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
print(m.group(1) if m else "0.0.0")
PY
)"

STAGE="$(mktemp -d "${TMPDIR:-/tmp}/hoglah-win64-XXXXXX")"
cleanup() { rm -rf "$STAGE"; }
trap cleanup EXIT

mkdir -p "$STAGE/hoglah-win64/python"
echo "Fetching ${EMBED_URL}"
curl -fsSL "$EMBED_URL" -o "$STAGE/embed.zip"

python3 - <<PY
import hashlib
import os
import sys
from pathlib import Path

blob = Path("$STAGE/embed.zip").read_bytes()
actual = hashlib.sha256(blob).hexdigest()
py_ver = "$PY_VER"
expected = os.environ.get("HOGLAH_EMBED_SHA256", "").strip()
default = "$DEFAULT_EMBED_SHA256"
if not expected:
    if py_ver == "3.12.10":
        expected = default
    else:
        print(
            f"warning: no sha256 pin for Python {py_ver}; "
            "set HOGLAH_EMBED_SHA256",
            file=sys.stderr,
        )
        raise SystemExit(0)
if actual != expected:
    print(f"embed zip sha256 mismatch: {actual} != {expected}", file=sys.stderr)
    raise SystemExit(1)
print("embed zip sha256 ok")
PY

python3 - <<PY
import zipfile
from pathlib import Path
root = Path("$STAGE") / "hoglah-win64" / "python"
with zipfile.ZipFile("$STAGE/embed.zip") as zf:
    zf.extractall(root)
pth = next(root.glob("python*._pth"))
stdlib = "python${NODOT}.zip"
pth.write_text(f"{stdlib}\n.\nLib\\\\site-packages\nimport site\n", encoding="utf-8")
print("wrote", pth, "stdlib", stdlib)
if not (root / stdlib).is_file():
    raise SystemExit(f"stdlib zip missing after extract: {stdlib}")
PY

mkdir -p "$STAGE/hoglah-win64/python/Lib/site-packages"
python3 -m pip install \
  --python-version "$MM" \
  --platform win_amd64 \
  --implementation cp \
  --abi "cp${NODOT}" \
  --abi none \
  --only-binary=:all: \
  --no-compile \
  --target "$STAGE/hoglah-win64/python/Lib/site-packages" \
  "$WHEEL"

cat > "$STAGE/hoglah-win64/hoglah.cmd" <<'EOF'
@echo off
setlocal
if not defined HOGLAH_HOME set "HOGLAH_HOME=%USERPROFILE%\.hoglah"
if not defined HOGLAH_DB_PATH set "HOGLAH_DB_PATH=%HOGLAH_HOME%\hoglah.db"
"%~dp0python\python.exe" -m hoglah %*
EOF

cat > "$STAGE/hoglah-win64/hoglah.ps1" <<'EOF'
if (-not $env:HOGLAH_HOME) { $env:HOGLAH_HOME = Join-Path $env:USERPROFILE ".hoglah" }
if (-not $env:HOGLAH_DB_PATH) { $env:HOGLAH_DB_PATH = Join-Path $env:HOGLAH_HOME "hoglah.db" }
& "$PSScriptRoot\python\python.exe" -m hoglah @args
EOF

cat > "$STAGE/hoglah-win64/install.ps1" <<'EOF'
# Optional: add this folder to the current user's PATH. No admin.
# Never touches %USERPROFILE%\.hoglah unless -Purge is given with the exact path.
param(
  [switch]$Purge,
  [string]$PurgePath = ""
)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$data = Join-Path $env:USERPROFILE ".hoglah"
if ($Purge) {
  if ($PurgePath -ne $data) {
    Write-Error "-Purge requires -PurgePath '$data'"
    exit 1
  }
  Remove-Item -Recurse -Force $data
  Write-Host "purged $data"
  exit 0
}
$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
$hereNorm = $here.TrimEnd('\')
$parts = @()
if ($userPath) {
  $parts = @(
    $userPath.Split(';') | ForEach-Object { $_.TrimEnd('\') } | Where-Object { $_ }
  )
}
if ($parts -notcontains $hereNorm) {
  $prefix = if ($userPath) { "$here;$userPath" } else { $here }
  [Environment]::SetEnvironmentVariable("Path", $prefix, "User")
  Write-Host "Added $here to user PATH. Open a new terminal."
} else {
  Write-Host "Already on user PATH."
}
Write-Host "Data directory is $data (not modified)."
EOF

cat > "$STAGE/hoglah-win64/README.txt" <<EOF
Hoglah ${VER} portable (Windows amd64)
=====================================

No system Python and no Ollama daemon are required.

1. Keep this folder named hoglah-win64 (stable name for upgrades).
2. Run hoglah.cmd --version
3. Optional: powershell -File install.ps1  (user PATH, no admin)
4. Data lives in %USERPROFILE%\\.hoglah  — upgrades must not delete it.
5. Upgrade: extract a newer zip OVER this hoglah-win64 folder.
6. Real Ollama inference is later: hoglah.cmd run --real

Unsupported: win32, ARM64, unsigned frozen .exe.
EOF

mkdir -p "$OUT_DIR"
ZIP="$OUT_DIR/hoglah-${VER}-win64.zip"
rm -f "$ZIP"
python3 - <<PY
import zipfile
from pathlib import Path
root = Path("$STAGE") / "hoglah-win64"
zip_path = Path("$ZIP")
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in root.rglob("*"):
        if path.is_file():
            zf.write(path, Path("hoglah-win64") / path.relative_to(root))
print("wrote", zip_path, "bytes", zip_path.stat().st_size)
PY
echo "Windows portable zip: $ZIP"
echo "Top-level folder is always hoglah-win64/ for extract-over upgrades."
echo "Do not upload this zip to PyPI; it is a GitHub Release asset only."
