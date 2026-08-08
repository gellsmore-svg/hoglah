"""Worker-process metrics exporter (review F2)."""

from __future__ import annotations

import urllib.request

from hoglah.metrics_server import start_metrics_server


def test_metrics_server_serves_body() -> None:
    server = start_metrics_server(lambda: "hoglah_jobs{status=\"total\"} 1\n", port=0)
    try:
        host, port = server.server_address[:2]
        with urllib.request.urlopen(f"http://{host}:{port}/metrics", timeout=2) as resp:
            body = resp.read().decode()
        assert "hoglah_jobs" in body
        assert "1" in body
    finally:
        server.shutdown()
