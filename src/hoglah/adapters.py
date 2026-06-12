"""Execution adapters for Hoglah.

Real Ollama calls are currently paused ("Ollama in use atm").

Default is StubAdapter which simulates execution without touching the Ollama server.
This lets us continue developing the queue, worker, callbacks, retries, truncation
reporting, etc. without contending for local model resources.

When ready, we can implement (or enable) a real OllamaAdapter that uses the
official `ollama` package (sync or async client).
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

from .models import JobRequest


class BaseAdapter(ABC):
    """Common interface for job executors (generate or chat style)."""

    @abstractmethod
    async def run(self, request: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
        """
        Execute the request and return:
            (output_text, usage_dict, metadata_dict)

        usage_dict should contain at least:
            {"prompt_tokens": int, "completion_tokens": int, "total": int}

        metadata can carry "truncated", "truncation_reason", etc.
        """
        raise NotImplementedError


class StubAdapter(BaseAdapter):
    """
    Safe no-op / simulation adapter.

    - Does NOT call Ollama.
    - Returns deterministic fake output.
    - Simulates truncation reporting when the request looks "large" relative to num_ctx.
    - Small artificial delay so the worker loop has something to do.
    """

    async def run(self, request: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
        # Tiny simulated "thinking" time
        await asyncio.sleep(0.03)

        if request.messages:
            # chat style
            last = request.messages[-1] if request.messages else {}
            content = last.get("content", "") if isinstance(last, dict) else str(last)
            base = f"[STUB-CHAT] Responded to: {content[:60]}..."
        else:
            prompt = request.prompt or ""
            base = f"[STUB] Generated response for: {prompt[:60]}..."

        # crude token estimation
        prompt_tokens = len((request.prompt or str(request.messages or "")).split())
        num_ctx = request.num_ctx or 4096

        completion_tokens = 25
        total = prompt_tokens + completion_tokens

        output = base
        meta: dict[str, Any] = {}

        # Simulate the truncation behavior requested in the spec
        if num_ctx and prompt_tokens > num_ctx * 0.9:
            meta["truncated"] = True
            meta["truncation_reason"] = "simulated_context_limit_in_stub"
            output = base[:80] + " ... [truncated in stub for testing]"

        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total": total,
        }

        return output, usage, meta


# Placeholder for when we re-enable real calls.
# class OllamaAdapter(BaseAdapter):
#     def __init__(self, host: str | None = None):
#         self.host = host
#     async def run(self, request: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
#         ...  # would use ollama.AsyncClient here
#         raise NotImplementedError("Real OllamaAdapter not active (Ollama currently in use)")
