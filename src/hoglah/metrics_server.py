"""Tiny Prometheus scrape server for the worker process (review F2).

`hoglah metrics` and `hoglah serve` run with ``start_worker=False``, so their
process-local counters stay zero. Scrapers that need live counters should hit
the exporter started by ``hoglah run --metrics-port N``.
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable

logger = logging.getLogger("hoglah")


def start_metrics_server(
    metrics_text: Callable[[], str],
    *,
    host: str = "127.0.0.1",
    port: int,
) -> ThreadingHTTPServer:
    """Serve ``GET /metrics`` (and ``/``) from a background daemon thread."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path.split("?", 1)[0] not in ("/metrics", "/"):
                self.send_error(404)
                return
            body = metrics_text().encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
            logger.debug("metrics-server: " + format, *args)

    server = ThreadingHTTPServer((host, int(port)), Handler)
    thread = threading.Thread(
        target=server.serve_forever,
        name=f"hoglah-metrics-{port}",
        daemon=True,
    )
    thread.start()
    logger.info("Prometheus metrics exporter listening on http://%s:%s/metrics", host, port)
    return server
