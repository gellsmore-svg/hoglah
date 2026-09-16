"""Execution-backend names, aliases, and per-job adapter routing (ADR-021)."""

from __future__ import annotations

from typing import Any, Iterable

# Canonical backend -> aliases. First name in each tuple is canonical.
_ALIAS_GROUPS: tuple[tuple[str, ...], ...] = (
    ("stub", "mock", "fake"),
    ("ollama",),
    ("openai", "openai-compat", "openai_compat", "vllm", "llamacpp", "llama.cpp", "lmstudio", "groq"),
    ("claude", "claude-cli", "anthropic-cli", "anthropic"),
    ("codex", "codex-cli", "openai-codex"),
    ("gemini", "gemini-cli", "google-gemini"),
    ("grok", "grok-cli", "grok-tui"),
    ("kiro", "kiro-cli"),
)

BACKEND_ALIASES: dict[str, str] = {
    alias: group[0] for group in _ALIAS_GROUPS for alias in group
}

CLI_BACKENDS = frozenset({"claude", "codex", "gemini", "grok", "kiro"})
HTTP_BACKENDS = frozenset({"openai"})
LOCAL_MEMORY_BACKENDS = frozenset({"ollama"})

KNOWN_BACKENDS = frozenset(BACKEND_ALIASES.values())


def normalize_backend(name: str | None) -> str | None:
    """Return canonical backend name, or None if empty.

    Unknown names are returned lowercased (custom CLI presets / future backends)
    rather than rejected here — the registry decides whether it can route them.
    """
    if name is None:
        return None
    raw = str(name).strip().lower()
    if not raw or raw in ("auto", "default"):
        return None
    return BACKEND_ALIASES.get(raw, raw)


class AdapterRegistry:
    """Per-job backend router. Explicit ``Hoglah(adapter=)`` bypasses this."""

    def __init__(
        self,
        default_backend: str,
        adapters: dict[str, Any],
        *,
        ollama_pool: Any | None = None,
    ) -> None:
        self.default_backend = normalize_backend(default_backend) or "ollama"
        self._adapters = dict(adapters)
        self.ollama_pool = ollama_pool

    @property
    def default(self) -> Any:
        adapter = self._adapters.get(self.default_backend)
        if adapter is None:
            # Fall back to any registered adapter rather than crashing submit.
            if self._adapters:
                return next(iter(self._adapters.values()))
            raise RuntimeError("AdapterRegistry has no adapters")
        return adapter

    def get(self, backend: str | None) -> Any | None:
        name = normalize_backend(backend) or self.default_backend
        return self._adapters.get(name)

    def for_request(self, request: Any) -> Any:
        name = normalize_backend(getattr(request, "backend", None))
        adapter = self.get(name)
        if adapter is None:
            wanted = name or self.default_backend
            known = ", ".join(sorted(self._adapters)) or "(none)"
            raise RuntimeError(
                f"no adapter registered for backend {wanted!r}; have: {known}"
            )
        return adapter

    def ollama_members(self) -> list[Any]:
        if self.ollama_pool is not None:
            return list(self.ollama_pool._adapters)
        adapter = self._adapters.get("ollama")
        return [adapter] if adapter is not None else []

    def supports_preload(self) -> bool:
        return any(
            getattr(a, "supports_preload", lambda: False)()
            for a in self.ollama_members()
        )

    def backends(self) -> list[str]:
        return sorted(self._adapters)

    def items(self) -> Iterable[tuple[str, Any]]:
        return self._adapters.items()
