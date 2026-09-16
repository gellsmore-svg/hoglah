from hoglah.routing import KNOWN_BACKENDS, normalize_backend


def test_aliases():
    assert normalize_backend("claude-cli") == "claude"
    assert normalize_backend("vllm") == "openai"
    assert normalize_backend("llama.cpp") == "openai"
    assert normalize_backend("kiro-cli") == "kiro"
    assert normalize_backend("GROK") == "grok"
    assert normalize_backend("auto") is None
    assert normalize_backend("") is None
    assert normalize_backend("custom-foo") == "custom-foo"


def test_known_set():
    assert {"ollama", "openai", "claude", "codex", "gemini", "grok", "kiro", "stub"} <= KNOWN_BACKENDS
