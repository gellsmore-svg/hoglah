import asyncio
import json
from unittest.mock import patch

from hoglah.http_adapter import OpenAICompatAdapter
from hoglah.models import JobRequest


class _Resp:
    def __init__(self, payload, status=200):
        self._raw = json.dumps(payload).encode()
        self.status = status

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_chat_completions_and_usage():
    adapter = OpenAICompatAdapter(base_url="http://localhost:8000/v1", api_key="sk")
    payload = {
        "id": "chatcmpl-1",
        "choices": [
            {"message": {"role": "assistant", "content": "pong"}, "finish_reason": "stop"}
        ],
        "usage": {"prompt_tokens": 3, "completion_tokens": 1, "total_tokens": 4},
    }

    def fake_urlopen(req, timeout=None):
        body = json.loads(req.data.decode())
        assert body["model"] == "gpt-4o"
        assert body["messages"][0]["role"] == "user"
        assert "Authorization" in req.headers
        return _Resp(payload)

    with patch("hoglah.http_adapter.urllib.request.urlopen", fake_urlopen):
        out, usage, meta = asyncio.run(
            adapter.run(JobRequest(prompt="ping", model="gpt-4o"))
        )
    assert out == "pong"
    assert usage["total"] == 4
    assert meta["backend"] == "openai"


def test_embeddings():
    adapter = OpenAICompatAdapter(base_url="http://localhost:8000/v1")
    payload = {"data": [{"embedding": [0.1, 0.2, 0.3]}], "usage": {"prompt_tokens": 2}}

    with patch(
        "hoglah.http_adapter.urllib.request.urlopen",
        lambda req, timeout=None: _Resp(payload),
    ):
        vec, usage, meta = asyncio.run(
            adapter.embed(JobRequest(prompt="hi", model="emb", kind="embed"))
        )
    assert vec == [0.1, 0.2, 0.3]
    assert meta["embedding_dim"] == 3
