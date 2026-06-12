"""Execution adapters for Hoglah.

Default is StubAdapter (no network calls) — safe for resource-constrained or
shared environments.

Real execution is available via OllamaAdapter (uses the official `ollama` package).
Pass adapter=OllamaAdapter(host=...) to Hoglah(...) or configure via CLI flags
when supported.

The BaseAdapter protocol also exposes list_models() for discovery.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any

import ollama  # official client (declared dep)

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

    async def list_models(self) -> list[dict[str, Any]]:
        """Return available models. Default empty; adapters should override."""
        return []


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

    async def list_models(self) -> list[dict[str, Any]]:
        return [
            {"name": "stub-model:1b", "size": 123456, "digest": "stub", "details": {"family": "stub"}},
            {"name": "stub-model:7b", "size": 4567890, "digest": "stub", "details": {"family": "stub"}},
        ]


class OllamaAdapter(BaseAdapter):
    """
    Real adapter using the official ollama Python client against a local/remote
    Ollama server.

    - Supports both prompt (generate) and messages (chat) submission styles.
    - Maps common generation params (temperature, num_ctx, etc.) into the
      request options.
    - Returns usage counts from Ollama (prompt_eval_count / eval_count) when
      present.
    - Context truncation: if Ollama errors with context-related messages we
      surface them; otherwise we succeed and let the model/Ollama decide.
      Callers always get a result (per ADR-009) unless the error is fatal.
    """

    def __init__(self, host: str | None = None):
        self.host = host
        self._client: ollama.AsyncClient | None = None

    def _get_client(self) -> ollama.AsyncClient:
        if self._client is None:
            self._client = ollama.AsyncClient(host=self.host)
        return self._client

    def _build_options(self, request: JobRequest) -> dict[str, Any]:
        opts: dict[str, Any] = {}
        if request.options:
            opts.update(request.options)
        for key in (
            "temperature",
            "top_p",
            "top_k",
            "repeat_penalty",
            "seed",
            "num_predict",
            "stop",
            "num_ctx",
        ):
            val = getattr(request, key, None)
            if val is not None:
                opts[key] = val
        return opts or {}

    async def run(self, request: JobRequest) -> tuple[str, dict[str, int], dict[str, Any]]:
        client = self._get_client()
        options = self._build_options(request)
        meta: dict[str, Any] = {}
        usage: dict[str, int] = {}

        try:
            if request.messages:
                # chat path
                resp = await client.chat(
                    model=request.model,
                    messages=request.messages or [],
                    options=options if options else None,
                    format=request.format,
                    keep_alive=request.keep_alive,
                )
                # resp is ChatResponse-like (has .message or dict access)
                msg = getattr(resp, "message", None) or (resp.get("message") if isinstance(resp, dict) else None)
                if msg:
                    output = msg.get("content") if isinstance(msg, dict) else getattr(msg, "content", "")
                else:
                    output = str(resp)
                # token counts (chat responses use these fields)
                prompt_tokens = int(getattr(resp, "prompt_eval_count", 0) or (resp.get("prompt_eval_count", 0) if isinstance(resp, dict) else 0))
                completion_tokens = int(getattr(resp, "eval_count", 0) or (resp.get("eval_count", 0) if isinstance(resp, dict) else 0))
            else:
                # generate path
                resp = await client.generate(
                    model=request.model,
                    prompt=request.prompt or "",
                    system=request.system_prompt,
                    options=options if options else None,
                    format=request.format,
                    keep_alive=request.keep_alive,
                )
                output = getattr(resp, "response", None) or (resp.get("response") if isinstance(resp, dict) else str(resp))
                prompt_tokens = int(getattr(resp, "prompt_eval_count", 0) or (resp.get("prompt_eval_count", 0) if isinstance(resp, dict) else 0))
                completion_tokens = int(getattr(resp, "eval_count", 0) or (resp.get("eval_count", 0) if isinstance(resp, dict) else 0))

            total = prompt_tokens + completion_tokens
            usage = {
                "prompt_tokens": prompt_tokens or 0,
                "completion_tokens": completion_tokens or 0,
                "total": total,
            }

            # Best-effort truncation / completion info from real Ollama responses
            done_reason = getattr(resp, "done_reason", None) or (resp.get("done_reason") if isinstance(resp, dict) else None)
            if done_reason == "length":
                meta["truncated"] = True
                meta["truncation_reason"] = "length"  # hit max tokens / context window
            elif done_reason:
                meta["done_reason"] = done_reason

            # Fallback heuristic for cases where done_reason not present
            if not meta.get("truncated") and ("context" in str(output).lower() or "truncat" in str(output).lower()):
                meta["truncated"] = True
                meta["truncation_reason"] = "possible_context_truncation_from_model"

            return str(output or ""), usage, meta

        except Exception:
            # Let the caller (client._execute_with_retries) classify as transient/permanent
            # and decide on retries / final failure. We re-raise so error path is exercised.
            raise

    async def list_models(self) -> list[dict[str, Any]]:
        client = self._get_client()
        resp = await client.list()
        # resp is usually ListResponse with .models list of Model objects or dicts
        models = getattr(resp, "models", None) or (resp.get("models") if isinstance(resp, dict) else [])
        result = []
        for m in models or []:
            if isinstance(m, dict):
                result.append(m)
            else:
                # Try to turn object into dict
                d = {}
                for attr in ("name", "model", "size", "digest", "details", "modified_at"):
                    if hasattr(m, attr):
                        d[attr] = getattr(m, attr)
                    elif isinstance(m, dict) and attr in m:
                        d[attr] = m[attr]
                if d:
                    result.append(d)
        return result
