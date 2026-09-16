import asyncio

import pytest

from hoglah.cli_adapters import CLI_PRESETS, CliAdapter, resolve_binary
from hoglah.models import JobRequest


def test_claude_argv_dont_ask_by_default():
    spec = CLI_PRESETS["claude"]
    adapter = CliAdapter(spec, yolo=False)
    req = JobRequest(prompt="hi", model="sonnet", system_prompt="be brief")
    argv = adapter.build_argv(req, binary="/bin/claude")
    assert argv[0] == "/bin/claude"
    assert "-p" in argv
    assert "--permission-mode" in argv
    assert "dontAsk" in argv
    assert "bypassPermissions" not in argv
    assert argv[-1] == "hi"
    assert "--model" in argv and "sonnet" in argv
    assert "--append-system-prompt" in argv
    assert "be brief" in argv


def test_claude_argv_yolo():
    adapter = CliAdapter(CLI_PRESETS["claude"], yolo=True)
    argv = adapter.build_argv(JobRequest(prompt="x", model="m"), binary="claude")
    assert "bypassPermissions" in argv


def test_grok_gemini_codex_kiro_argv():
    req = JobRequest(prompt="hello", model="m1")
    grok = CliAdapter(CLI_PRESETS["grok"]).build_argv(req, binary="grok")
    assert grok[:3] == ["grok", "-p", "hello"]
    assert "--always-approve" not in grok

    gem = CliAdapter(CLI_PRESETS["gemini"], yolo=True).build_argv(req, binary="gemini")
    assert "-p" in gem and "--yolo" in gem

    codex = CliAdapter(CLI_PRESETS["codex"]).build_argv(req, binary="codex")
    assert codex[:2] == ["codex", "exec"]
    assert "--sandbox" in codex and "read-only" in codex

    kiro = CliAdapter(CLI_PRESETS["kiro"], yolo=True).build_argv(req, binary="kiro-cli")
    assert "chat" in kiro and "--no-interactive" in kiro and "--trust-all-tools" in kiro
    assert kiro[-1] == "hello"


def test_missing_binary_raises():
    adapter = CliAdapter(CLI_PRESETS["claude"], binaries={"claude": ""})

    async def _run():
        with pytest.raises(FileNotFoundError):
            # Force resolve to fail by using a spec whose binary cannot exist.
            adapter.binaries = {"claude": "/no/such/hoglah-claude-bin"}
            # which() still finds real claude; stub resolve by patching spec binary
            adapter.spec = type(adapter.spec)(
                name="nope",
                binary="hoglah-definitely-missing-cli",
                build_argv=adapter.spec.build_argv,
            )
            await adapter.run(JobRequest(prompt="x", model="m"))

    asyncio.run(_run())


def test_resolve_binary_explicit_map():
    spec = CLI_PRESETS["grok"]
    assert resolve_binary(spec, {"grok": "/opt/grok"}) == "/opt/grok"
