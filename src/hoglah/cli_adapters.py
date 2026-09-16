"""CLI execution adapters (claude, codex, gemini, grok, kiro) — ADR-021.

These wrap **headless** invocations of coding-agent CLIs. They never become
the default path: the worker only uses them when a job's ``backend`` names
one of the presets (and the instance is running with real adapters).

Permission defaults prefer *don't hang* over *auto-approve*:
``cli_yolo=False`` uses each tool's non-interactive deny/don't-ask mode.
``HOGLAH_CLI_YOLO=1`` / ``cli_yolo=True`` auto-approves tools.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import signal
from dataclasses import dataclass
from typing import Any, Callable

from .models import JobRequest


def _flatten_messages(request: JobRequest) -> str:
    """Turn prompt + chat messages into one CLI prompt (system is a spec flag)."""
    parts: list[str] = []
    if request.messages:
        for msg in request.messages:
            if not isinstance(msg, dict):
                parts.append(str(msg))
                continue
            role = str(msg.get("role") or "user")
            content = msg.get("content")
            if content is None:
                continue
            parts.append(f"{role}: {content}")
    elif request.prompt:
        parts.append(request.prompt)
    return "\n\n".join(p for p in parts if p)


@dataclass(frozen=True)
class CliSpec:
    """How to invoke one CLI in headless mode."""

    name: str
    binary: str
    # Build argv given (binary, prompt, model, yolo, system_prompt, extra).
    build_argv: Callable[..., list[str]]
    aliases: tuple[str, ...] = ()


def _claude_argv(
    binary: str,
    prompt: str,
    model: str,
    *,
    yolo: bool,
    system_prompt: str | None,
    extra: list[str],
) -> list[str]:
    argv = [
        binary,
        "-p",
        "--output-format",
        "text",
        "--permission-mode",
        "bypassPermissions" if yolo else "dontAsk",
    ]
    if model:
        argv.extend(["--model", model])
    if system_prompt:
        argv.extend(["--append-system-prompt", system_prompt])
    argv.extend(extra)
    argv.append(prompt)
    return argv


def _codex_argv(
    binary: str,
    prompt: str,
    model: str,
    *,
    yolo: bool,
    system_prompt: str | None,
    extra: list[str],
) -> list[str]:
    argv = [binary, "exec", "--skip-git-repo-check"]
    if model:
        argv.extend(["-m", model])
    if yolo:
        argv.append("--dangerously-bypass-approvals-and-sandbox")
    else:
        argv.extend(["--sandbox", "read-only"])
    argv.extend(extra)
    text = prompt
    if system_prompt:
        text = f"{system_prompt}\n\n{prompt}"
    argv.append(text)
    return argv


def _gemini_argv(
    binary: str,
    prompt: str,
    model: str,
    *,
    yolo: bool,
    system_prompt: str | None,
    extra: list[str],
) -> list[str]:
    text = prompt
    if system_prompt:
        text = f"{system_prompt}\n\n{prompt}"
    argv = [binary, "-p", text, "--output-format", "text"]
    if model:
        argv.extend(["-m", model])
    if yolo:
        argv.append("--yolo")
    argv.extend(extra)
    return argv


def _grok_argv(
    binary: str,
    prompt: str,
    model: str,
    *,
    yolo: bool,
    system_prompt: str | None,
    extra: list[str],
) -> list[str]:
    argv = [binary, "-p", prompt, "--output-format", "plain"]
    if model:
        argv.extend(["-m", model])
    if yolo:
        argv.append("--always-approve")
    if system_prompt:
        argv.extend(["--rules", system_prompt])
    argv.extend(extra)
    return argv


def _kiro_argv(
    binary: str,
    prompt: str,
    model: str,
    *,
    yolo: bool,
    system_prompt: str | None,
    extra: list[str],
) -> list[str]:
    text = prompt
    if system_prompt:
        text = f"{system_prompt}\n\n{prompt}"
    argv = [binary, "chat", "--no-interactive"]
    if yolo:
        argv.append("--trust-all-tools")
    if model:
        argv.extend(["--model", model])
    argv.extend(extra)
    argv.append(text)
    return argv


CLI_PRESETS: dict[str, CliSpec] = {
    "claude": CliSpec("claude", "claude", _claude_argv, aliases=("claude-cli",)),
    "codex": CliSpec("codex", "codex", _codex_argv, aliases=("codex-cli",)),
    "gemini": CliSpec("gemini", "gemini", _gemini_argv, aliases=("gemini-cli",)),
    "grok": CliSpec("grok", "grok", _grok_argv, aliases=("grok-cli",)),
    "kiro": CliSpec("kiro", "kiro-cli", _kiro_argv, aliases=("kiro-cli",)),
}


def resolve_binary(spec: CliSpec, binaries: dict[str, str] | None = None) -> str | None:
    """Path to the CLI binary, or None if not found.

    Lookup order: explicit map (by canonical name, then binary name) → PATH
    (canonical binary, then aliases).
    """
    mapping = binaries or {}
    for key in (spec.name, spec.binary, *spec.aliases):
        explicit = mapping.get(key)
        if explicit:
            return explicit
    for name in (spec.binary, spec.name, *spec.aliases):
        found = shutil.which(name)
        if found:
            return found
    return None


class CliAdapter:
    """Run a headless coding-agent CLI as a Hoglah job executor."""

    def __init__(
        self,
        spec: CliSpec,
        *,
        yolo: bool = False,
        binaries: dict[str, str] | None = None,
        default_workdir: str | None = None,
    ) -> None:
        self.spec = spec
        self.backend_name = spec.name
        self.yolo = yolo
        self.binaries = dict(binaries or {})
        self.default_workdir = default_workdir
        self._procs: set[asyncio.subprocess.Process] = set()

    def supports_preload(self) -> bool:
        return False

    async def ps(self) -> list[dict[str, Any]]:
        return []

    async def preload(self, model: str, *, keep_alive: Any = None, kind: str = "generate") -> None:
        return None

    async def unload(self, model: str) -> None:
        return None

    async def list_models(self) -> list[dict[str, Any]]:
        return []

    async def pull_model(self, model: str, force: bool = False) -> None:
        return None

    async def show_model(self, model: str, force: bool = False) -> dict[str, Any]:
        return {"name": model, "backend": self.backend_name}

    async def embed(self, request: JobRequest) -> tuple[list[float], dict[str, int], dict[str, Any]]:
        raise NotImplementedError(
            f"{self.backend_name} CLI adapter does not support embedding jobs"
        )

    def build_argv(self, request: JobRequest, *, binary: str) -> list[str]:
        prompt = _flatten_messages(request)
        extra = list(request.cli_args or [])
        # System prompt is consumed by the spec builder; don't double-apply
        # for CLIs that have a dedicated flag (claude/grok).
        return self.spec.build_argv(
            binary,
            prompt,
            request.model or "",
            yolo=self.yolo,
            system_prompt=request.system_prompt,
            extra=extra,
        )

    async def run(self, request: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
        binary = resolve_binary(self.spec, self.binaries)
        if not binary:
            raise FileNotFoundError(
                f"{self.spec.name} CLI not found on PATH "
                f"(looked for {self.spec.binary!r} / {self.spec.name!r})"
            )
        argv = self.build_argv(request, binary=binary)
        workdir = request.workdir or self.default_workdir
        env = os.environ.copy()
        # Headless: never wait on a TTY prompt.
        env.setdefault("TERM", "dumb")
        kwargs: dict[str, Any] = dict(
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        if workdir:
            kwargs["cwd"] = workdir
        if os.name != "nt":
            kwargs["start_new_session"] = True
        proc = await asyncio.create_subprocess_exec(*argv, **kwargs)
        self._procs.add(proc)
        try:
            out_b, err_b = await proc.communicate()
        except asyncio.CancelledError:
            await self._kill(proc)
            raise
        finally:
            self._procs.discard(proc)

        stdout = (out_b or b"").decode("utf-8", errors="replace")
        stderr = (err_b or b"").decode("utf-8", errors="replace")
        if proc.returncode not in (0, None):
            snippet = (stderr or stdout).strip()[:800]
            raise RuntimeError(
                f"{self.spec.name} CLI exited {proc.returncode}: {snippet or 'no output'}"
            )
        usage = {
            "prompt_tokens": len(_flatten_messages(request).split()),
            "completion_tokens": len(stdout.split()),
            "total": 0,
        }
        usage["total"] = usage["prompt_tokens"] + usage["completion_tokens"]
        meta = {
            "backend": self.backend_name,
            "argv0": binary,
            "returncode": proc.returncode,
        }
        if stderr.strip():
            meta["stderr"] = stderr[-2000:]
        return stdout, usage, meta

    async def abort(self) -> None:
        for proc in list(self._procs):
            await self._kill(proc)

    async def _kill(self, proc: asyncio.subprocess.Process) -> None:
        if proc.returncode is not None:
            return
        try:
            if os.name != "nt" and proc.pid:
                os.killpg(proc.pid, signal.SIGTERM)
            else:
                proc.terminate()
        except (ProcessLookupError, PermissionError, OSError):
            try:
                proc.kill()
            except (ProcessLookupError, OSError):
                return
        try:
            await asyncio.wait_for(proc.wait(), timeout=2.0)
        except (asyncio.TimeoutError, ProcessLookupError):
            try:
                proc.kill()
            except (ProcessLookupError, OSError):
                pass


def build_cli_adapters(
    *,
    yolo: bool = False,
    binaries: dict[str, str] | None = None,
    default_workdir: str | None = None,
    names: list[str] | None = None,
) -> dict[str, CliAdapter]:
    """Construct CliAdapter instances for the requested preset names."""
    wanted = names or list(CLI_PRESETS)
    out: dict[str, CliAdapter] = {}
    for name in wanted:
        spec = CLI_PRESETS.get(name)
        if spec is None:
            continue
        out[name] = CliAdapter(
            spec, yolo=yolo, binaries=binaries, default_workdir=default_workdir
        )
    return out
