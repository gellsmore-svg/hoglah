"""OpenAI-compatible HTTP adapter (vLLM, llama.cpp server, LM Studio, Groq, …).

Uses stdlib urllib so the extra is not required. Endpoint is
``{base_url}/chat/completions`` (generate/chat) and ``{base_url}/embeddings``.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from .models import JobRequest


class OpenAICompatAdapter:
    """HTTP chat/embeddings against an OpenAI-compatible ``/v1`` server."""

    backend_name = "openai"

    def __init__(
        self,
        *,
        base_url: str = "https://api.openai.com/v1",
        api_key: str | None = None,
        timeout_seconds: float = 120.0,
    ) -> None:
        self.base_url = (base_url or "https://api.openai.com/v1").rstrip("/")
        self.api_key = api_key
        self.timeout_seconds = float(timeout_seconds)

    def supports_preload(self) -> bool:
        return False

    async def ps(self) -> list[dict[str, Any]]:
        return []

    async def preload(self, model: str, *, keep_alive: Any = None, kind: str = "generate") -> None:
        return None

    async def unload(self, model: str) -> None:
        return None

    async def pull_model(self, model: str, force: bool = False) -> None:
        return None

    async def show_model(self, model: str, force: bool = False) -> dict[str, Any]:
        return {"name": model, "backend": "openai"}

    def _headers(self, request: JobRequest | None = None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        key = self.api_key
        if request and request.metadata:
            key = request.metadata.get("api_key") or key
        if key:
            headers["Authorization"] = f"Bearer {key}"
        return headers

    def _base_url(self, request: JobRequest) -> str:
        if request.options and request.options.get("base_url"):
            return str(request.options["base_url"]).rstrip("/")
        if request.metadata and request.metadata.get("base_url"):
            return str(request.metadata["base_url"]).rstrip("/")
        return self.base_url

    def _timeout(self, request: JobRequest) -> float:
        if request.timeout_seconds and request.timeout_seconds > 0:
            return float(request.timeout_seconds)
        return self.timeout_seconds

    def _post(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        headers: dict[str, str],
        timeout: float,
    ) -> dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")[:800]
            except Exception:
                pass
            err = RuntimeError(f"HTTP {exc.code} from {url}: {body or exc.reason}")
            setattr(err, "status_code", int(exc.code))
            raise err from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(f"connection error talking to {url}: {exc.reason}") from exc
        try:
            parsed = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"non-JSON response from {url}: {raw[:200]!r}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError(f"unexpected JSON from {url}")
        return parsed

    def _messages(self, request: JobRequest) -> list[dict[str, str]]:
        if request.messages:
            out: list[dict[str, str]] = []
            for msg in request.messages:
                if isinstance(msg, dict) and "content" in msg:
                    out.append(
                        {
                            "role": str(msg.get("role") or "user"),
                            "content": str(msg.get("content") or ""),
                        }
                    )
            if request.system_prompt:
                out.insert(0, {"role": "system", "content": request.system_prompt})
            return out
        messages: list[dict[str, str]] = []
        if request.system_prompt:
            messages.append({"role": "system", "content": request.system_prompt})
        messages.append({"role": "user", "content": request.prompt or ""})
        return messages

    def _sampling(self, request: JobRequest) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if request.temperature is not None:
            body["temperature"] = request.temperature
        if request.top_p is not None:
            body["top_p"] = request.top_p
        if request.num_predict is not None:
            body["max_tokens"] = request.num_predict
        if request.stop:
            body["stop"] = request.stop
        if request.seed is not None:
            body["seed"] = request.seed
        if request.format == "json":
            body["response_format"] = {"type": "json_object"}
        if request.options:
            for key in ("temperature", "top_p", "max_tokens", "stop", "seed", "response_format"):
                if key in request.options and key not in body:
                    body[key] = request.options[key]
        return body

    def _post_sse(
        self,
        url: str,
        payload: dict[str, Any],
        *,
        headers: dict[str, str],
        timeout: float,
        on_chunk: Callable[[str], None],
    ) -> tuple[str, dict[str, int], dict[str, Any]]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers=headers, method="POST")
        parts: list[str] = []
        usage_raw: dict[str, Any] = {}
        finish: Any = None
        resp_id: Any = None
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                for raw in resp:
                    line = raw.decode("utf-8", errors="replace").strip()
                    if not line:
                        continue
                    if not line.startswith("data:"):
                        continue
                    body = line[5:].strip()
                    if body == "[DONE]":
                        break
                    try:
                        obj = json.loads(body)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(obj, dict):
                        continue
                    if obj.get("id"):
                        resp_id = obj.get("id")
                    if obj.get("usage"):
                        usage_raw = obj.get("usage") or usage_raw
                    choices = obj.get("choices") or []
                    if not choices:
                        continue
                    choice = choices[0] or {}
                    if choice.get("finish_reason"):
                        finish = choice.get("finish_reason")
                    delta = choice.get("delta") or {}
                    piece = delta.get("content") or ""
                    if not piece:
                        msg = choice.get("message") or {}
                        piece = msg.get("content") or ""
                    if piece:
                        parts.append(str(piece))
                        on_chunk(str(piece))
        except urllib.error.HTTPError as exc:
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")[:800]
            except Exception:
                pass
            err = RuntimeError(f"HTTP {exc.code} from {url}: {body or exc.reason}")
            setattr(err, "status_code", int(exc.code))
            raise err from exc
        except urllib.error.URLError as exc:
            raise ConnectionError(f"connection error talking to {url}: {exc.reason}") from exc
        output = "".join(parts)
        prompt_tokens = int(usage_raw.get("prompt_tokens") or 0)
        completion_tokens = int(usage_raw.get("completion_tokens") or 0)
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total": int(usage_raw.get("total_tokens") or (prompt_tokens + completion_tokens)),
        }
        meta: dict[str, Any] = {"backend": "openai", "id": resp_id, "stream": True}
        if finish:
            meta["done_reason"] = finish
            if finish == "length":
                meta["truncated"] = True
                meta["truncation_reason"] = "length"
        return output, usage, meta

    async def run(
        self,
        request: JobRequest,
        *,
        on_chunk: Callable[[str], None] | None = None,
    ) -> tuple[str, dict[str, int], dict[str, Any]]:
        url = f"{self._base_url(request)}/chat/completions"
        payload = {
            "model": request.model,
            "messages": self._messages(request),
            **self._sampling(request),
        }
        if on_chunk is not None:
            payload["stream"] = True
            return self._post_sse(
                url,
                payload,
                headers=self._headers(request),
                timeout=self._timeout(request),
                on_chunk=on_chunk,
            )
        data = self._post(
            url, payload, headers=self._headers(request), timeout=self._timeout(request)
        )
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError("OpenAI-compatible response had no choices")
        msg = (choices[0] or {}).get("message") or {}
        output = msg.get("content") or ""
        usage_raw = data.get("usage") or {}
        prompt_tokens = int(usage_raw.get("prompt_tokens") or 0)
        completion_tokens = int(usage_raw.get("completion_tokens") or 0)
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total": int(usage_raw.get("total_tokens") or (prompt_tokens + completion_tokens)),
        }
        finish = (choices[0] or {}).get("finish_reason")
        meta: dict[str, Any] = {"backend": "openai", "id": data.get("id")}
        if finish:
            meta["done_reason"] = finish
            if finish == "length":
                meta["truncated"] = True
                meta["truncation_reason"] = "length"
        return str(output), usage, meta

    async def embed(self, request: JobRequest) -> tuple[list[float], dict[str, int], dict[str, Any]]:
        url = f"{self._base_url(request)}/embeddings"
        payload = {"model": request.model, "input": request.prompt or ""}
        data = self._post(
            url, payload, headers=self._headers(request), timeout=self._timeout(request)
        )
        items = data.get("data") or []
        if not items:
            raise RuntimeError("OpenAI-compatible embeddings response had no data")
        vector = [float(x) for x in (items[0].get("embedding") or [])]
        if not vector:
            raise RuntimeError(f"empty embedding from {request.model}")
        usage_raw = data.get("usage") or {}
        prompt_tokens = int(usage_raw.get("prompt_tokens") or 0)
        usage = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": 0,
            "total": int(usage_raw.get("total_tokens") or prompt_tokens),
        }
        return vector, usage, {"backend": "openai", "embedding_dim": len(vector)}

    async def list_models(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}/models"
        headers = self._headers()
        req = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw) if raw else {}
        except Exception:
            return []
        out: list[dict[str, Any]] = []
        for item in data.get("data") or []:
            if isinstance(item, dict) and item.get("id"):
                out.append({"name": item["id"], "model": item["id"]})
        return out

    async def abort(self) -> None:
        return None
