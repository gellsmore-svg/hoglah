#!/usr/bin/env bash
# POSIX installer for Hoglah on Linux/macOS.
# Primary docs still say: pipx install hoglah
# This script is the fallback that prefers pipx, else an isolated venv.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/install.sh [--from pypi|PATH] [--extra NAME]... [--force-new] [--purge DATA_DIR]

  --from pypi     Install from PyPI (default)
  --from PATH     Install a wheel, sdist, or git checkout
  --extra NAME    Optional extra (web, mongo, ...). Repeatable. CLI is always included.
  --force-new     Install a new copy even if hoglah is already on PATH
  --purge DIR     Delete DIR only if it exactly matches the typed data dir
  --help          This help

Never requires the Ollama daemon. Never deletes ~/.hoglah unless --purge is given
with the exact path. Uninstall does not imply data wipe.
EOF
}

FROM="pypi"
EXTRAS=()
FORCE_NEW=0
PURGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)
      FROM="${2:-}"
      shift 2
      ;;
    --extra)
      EXTRAS+=("${2:-}")
      shift 2
      ;;
    --force-new)
      FORCE_NEW=1
      shift
      ;;
    --purge)
      PURGE="${2:-}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

DATA_DIR="${HOGLAH_HOME:-$HOME/.hoglah}"
XDG_DATA="${XDG_DATA_HOME:-$HOME/.local/share}"
VENV_DIR="${XDG_DATA}/hoglah/venv"
BIN_DIR="${HOME}/.local/bin"
ORIG_PATH="${PATH}"
BIN_ON_PATH=0
case ":${ORIG_PATH}:" in
  *":${BIN_DIR}:"*) BIN_ON_PATH=1 ;;
esac

if [[ -z "$FROM" ]]; then
  echo "--from requires a value (pypi or a path)" >&2
  exit 2
fi

if [[ "$FROM" != "pypi" ]]; then
  if [[ ! -e "$FROM" ]]; then
    echo "--from path not found: $FROM" >&2
    exit 1
  fi
  FROM="$(cd "$(dirname "$FROM")" && pwd)/$(basename "$FROM")"
fi

joined=""
if [[ ${#EXTRAS[@]} -gt 0 ]]; then
  joined=$(IFS=,; echo "${EXTRAS[*]}")
fi

if [[ "$FROM" == "pypi" ]]; then
  if [[ -n "$joined" ]]; then
    SPEC="hoglah[${joined}]"
  else
    SPEC="hoglah"
  fi
else
  if [[ -n "$joined" ]]; then
    SPEC="${FROM}[${joined}]"
  else
    SPEC="$FROM"
  fi
fi

if [[ -n "$PURGE" ]]; then
  if [[ "$PURGE" != "$DATA_DIR" ]]; then
    echo "--purge must be typed exactly as ${DATA_DIR}" >&2
    exit 1
  fi
  rm -rf -- "$DATA_DIR"
  echo "purged ${DATA_DIR}"
  exit 0
fi

python_ok() {
  command -v python3 >/dev/null 2>&1 || return 1
  python3 - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

have_pipx_app() {
  command -v pipx >/dev/null 2>&1 && pipx list --short 2>/dev/null | grep -q '^hoglah '
}

resolve_path() {
  python3 -c 'import os, sys; print(os.path.realpath(sys.argv[1]))' "$1" 2>/dev/null || echo "$1"
}

finish() {
  local how="$1"
  local uninstall="$2"
  if ! PATH="${BIN_DIR}:${ORIG_PATH}" command -v hoglah >/dev/null 2>&1; then
    echo "hoglah not found after install (looked in ${BIN_DIR})" >&2
    exit 1
  fi
  PATH="${BIN_DIR}:${ORIG_PATH}" hoglah --version
  echo "Installed via ${how}. Uninstall: ${uninstall}"
  echo "Data dir: ${DATA_DIR} (not touched; uninstall does not wipe it)"
  echo "Ollama daemon is optional. Use: hoglah run --real"
  if [[ "$BIN_ON_PATH" -eq 0 ]]; then
    echo "Add ${BIN_DIR} to PATH (e.g. export PATH=\"${BIN_DIR}:\$PATH\")"
  fi
}

inject_pipx_extras() {
  local extra
  for extra in "${EXTRAS[@]+"${EXTRAS[@]}"}"; do
    [[ -n "$extra" ]] || continue
    pipx inject hoglah "hoglah[${extra}]"
  done
}

existing="$(PATH="$ORIG_PATH" command -v hoglah || true)"
existing_real=""
if [[ -n "$existing" ]]; then
  existing_real="$(resolve_path "$existing")"
fi
venv_hoglah="$(resolve_path "$VENV_DIR/bin/hoglah")"
our_venv=0
if [[ -n "$existing_real" && "$existing_real" == "$venv_hoglah" ]]; then
  our_venv=1
fi

if [[ -n "$existing" && "$FORCE_NEW" -eq 0 ]]; then
  if have_pipx_app; then
    if [[ "$FROM" == "pypi" ]]; then
      echo "Found existing pipx app: $existing"
      echo "Upgrading pipx app (injected extras are kept)."
      pipx upgrade hoglah
      inject_pipx_extras
      finish "pipx" "pipx uninstall hoglah"
      exit 0
    fi
    echo "Found existing pipx app: $existing"
    echo "Reinstalling from ${FROM} (honors --from)."
    pipx install --force "$SPEC"
    finish "pipx" "pipx uninstall hoglah"
    exit 0
  fi
  if [[ "$our_venv" -eq 1 ]]; then
    echo "Found existing XDG venv install: $existing"
    "$VENV_DIR/bin/python" -m pip install -U pip
    "$VENV_DIR/bin/python" -m pip install -U "$SPEC"
    ln -sfn "$VENV_DIR/bin/hoglah" "$BIN_DIR/hoglah"
    finish "venv ${VENV_DIR}" "rm -rf ${VENV_DIR} ${BIN_DIR}/hoglah"
    exit 0
  fi
  shebang="$(head -n 1 "$existing" 2>/dev/null || true)"
  echo "Found existing hoglah: $existing"
  echo "Not a pipx app or this script's venv. Upgrade the environment that owns this binary."
  echo "Shebang: ${shebang}"
  echo "If this is a project venv: python -m pip install -U hoglah"
  echo "To install a second copy anyway: $0 --force-new"
  exit 1
fi

if command -v pipx >/dev/null 2>&1; then
  if have_pipx_app; then
    pipx install --force "$SPEC"
  else
    pipx install "$SPEC"
  fi
  finish "pipx" "pipx uninstall hoglah"
  exit 0
fi

if ! python_ok; then
  echo "Need Python 3.11+ on PATH, or install pipx." >&2
  echo "See docs/install.md" >&2
  exit 1
fi

mkdir -p "$VENV_DIR" "$BIN_DIR"
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
  python3 -m venv "$VENV_DIR"
fi
"$VENV_DIR/bin/python" -m pip install -U pip
"$VENV_DIR/bin/python" -m pip install -U "$SPEC"
ln -sfn "$VENV_DIR/bin/hoglah" "$BIN_DIR/hoglah"
finish "venv ${VENV_DIR}" "rm -rf ${VENV_DIR} ${BIN_DIR}/hoglah"
