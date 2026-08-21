# Installing Hoglah

Requires **Python 3.11+** except the Windows portable zip, which vendors
embeddable CPython 3.12. The Ollama **daemon** is optional. The `ollama`
Python wheel is a hard dependency (even the stub adapter imports it).

Data always lives in `~/.hoglah/` (Windows: `%USERPROFILE%\.hoglah\`).
Installers and uninstallers **do not** delete that directory.

## Primary: pipx (Linux, macOS, Windows with Python)

This is the [PyPA path for standalone CLIs](https://packaging.python.org/en/latest/guides/installing-stand-alone-command-line-tools/).

```bash
pipx install hoglah
hoglah doctor
hoglah --version
```

Upgrade / uninstall:

```bash
pipx upgrade hoglah
pipx uninstall hoglah          # does not wipe ~/.hoglah
```

Optional extras (Mongo, web monitor, …) stay optional:

```bash
pipx install 'hoglah[web]'
pipx inject hoglah 'hoglah[mongo]'
```

`pipx upgrade hoglah` keeps injected extras. Use `pipx reinstall hoglah` only
to repair a broken venv, not as the normal upgrade.

`hoglah[cli]` remains a no-op extra so old commands keep working. Typer is a
core dependency; `pip install hoglah` yields a working `hoglah` command.

## Linux / macOS fallback: `scripts/install.sh`

Use when pipx is not on PATH. Prefers pipx if present; otherwise an isolated
venv at `$XDG_DATA_HOME/hoglah/venv` and a symlink in `~/.local/bin`.

```bash
./scripts/install.sh                 # from PyPI
./scripts/install.sh --from .        # this checkout
./scripts/install.sh --extra web
```

It upgrades the **existing** install when `hoglah` is already on PATH:
`pipx upgrade` for a pipx app from PyPI, `pipx install --force` when
`--from` is a local path, or `pip install -U` into the XDG venv this
script created. Pass `--force-new` to ignore a foreign binary.
`--purge ~/.hoglah` is the only way it will delete the data directory, and
the path must match exactly.

Do not treat `curl | bash` as the primary install. pipx is.

## Windows without Python: portable zip

GitHub Releases attach `hoglah-<version>-win64.zip` (this zip is **not**
uploaded to PyPI — twine would treat `.zip` as an sdist). Inside, the
top-level folder is always **`hoglah-win64/`** so upgrades can extract over
the same directory.

Built on Linux CI from the official
[Windows embeddable package](https://www.python.org/downloads/windows/) plus
Windows wheels (`pip install --platform win_amd64 --only-binary=:all:`).
**Not** a PyInstaller/Nuitka frozen `.exe` (unsigned onefile binaries are
widely AV-flagged and cannot be cross-compiled from Linux).

```text
hoglah-win64\
  python\          embeddable CPython 3.12
  hoglah.cmd       python -m hoglah
  install.ps1      optional user PATH (no admin)
  README.txt
```

```bat
hoglah.cmd --version
hoglah.cmd doctor
```

Upgrade: extract a newer zip **over** `hoglah-win64`. Never delete
`%USERPROFILE%\.hoglah`. win32 and ARM64 are unsupported until wheels exist.

## Library in a project venv

```bash
python -m pip install hoglah
python -m pip install -U hoglah
```

## What we do not ship as “bulletproof”

- Unsigned PyInstaller `--onefile` (especially a Linux-built `.exe`)
- Snap / Flatpak / AppImage / distro `.deb` as the upgrade channel
- Bundling the Ollama **daemon** or Kafka/Mongo extras in the portable zip
- An installer that wipes the SQLite queue
