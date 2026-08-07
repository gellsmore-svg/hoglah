"""Prometheus-compatible metrics for Hoglah (gap G11).

No extra dependency: exposition is plain text 0.0.4 format that Prometheus
scrapes. Gauges are derived from the JobStore on scrape; counters/histograms
are process-local (reset on restart).
"""

from __future__ import annotations

import threading
import time
from typing import Any


def _escape_label(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace('"', '\\"')
    )


def _labels(**kwargs: str | int | float | None) -> str:
    parts = []
    for k, v in kwargs.items():
        if v is None:
            continue
        parts.append(f'{k}="{_escape_label(str(v))}"')
    if not parts:
        return ""
    return "{" + ",".join(parts) + "}"


class MetricsRegistry:
    """Process-local counters + latency samples; gauges from a stats callable."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        # latency samples in seconds, capped
        self._latencies: list[float] = []
        self._latency_max = 512
        self._started = time.time()

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        key = (name, tuple(sorted((k, str(v)) for k, v in labels.items())))
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + amount

    def observe_seconds(self, seconds: float) -> None:
        if seconds < 0:
            return
        with self._lock:
            self._latencies.append(float(seconds))
            if len(self._latencies) > self._latency_max:
                self._latencies = self._latencies[-self._latency_max :]

    def render(self, stats: dict[str, Any] | None = None) -> str:
        """Return Prometheus text exposition."""
        lines: list[str] = []
        stats = stats or {}
        counts = stats.get("counts") or {}

        lines.append("# HELP hoglah_jobs Gauge of jobs by status in the store.")
        lines.append("# TYPE hoglah_jobs gauge")
        for status in ("queued", "processing", "completed", "failed", "cancelled"):
            n = int(counts.get(status, 0))
            lines.append(f"hoglah_jobs{_labels(status=status)} {n}")
        total = int(stats.get("total_jobs", sum(int(v) for v in counts.values()) if counts else 0))
        lines.append(f"hoglah_jobs{_labels(status='total')} {total}")

        lines.append("# HELP hoglah_jobs_submitted_total Jobs accepted by submit().")
        lines.append("# TYPE hoglah_jobs_submitted_total counter")
        lines.append(
            f"hoglah_jobs_submitted_total {self._counter_value('hoglah_jobs_submitted_total')}"
        )

        lines.append("# HELP hoglah_jobs_terminal_total Jobs reaching a terminal status.")
        lines.append("# TYPE hoglah_jobs_terminal_total counter")
        for status in ("completed", "failed", "cancelled"):
            lines.append(
                f"hoglah_jobs_terminal_total{_labels(status=status)} "
                f"{self._counter_value('hoglah_jobs_terminal_total', status=status)}"
            )

        lines.append("# HELP hoglah_job_requeues_total Operator or bulk requeues.")
        lines.append("# TYPE hoglah_job_requeues_total counter")
        lines.append(
            f"hoglah_job_requeues_total {self._counter_value('hoglah_job_requeues_total')}"
        )

        lines.append("# HELP hoglah_lease_reclaims_total Stale PROCESSING leases reclaimed.")
        lines.append("# TYPE hoglah_lease_reclaims_total counter")
        lines.append(
            f"hoglah_lease_reclaims_total {self._counter_value('hoglah_lease_reclaims_total')}"
        )

        lines.append(
            "# HELP hoglah_job_duration_seconds Processing duration for completed/failed jobs."
        )
        lines.append("# TYPE hoglah_job_duration_seconds summary")
        with self._lock:
            samples = list(self._latencies)
        if samples:
            s = sorted(samples)
            count = len(s)
            total_sum = sum(s)
            lines.append(f"hoglah_job_duration_seconds_count {count}")
            lines.append(f"hoglah_job_duration_seconds_sum {total_sum:.6f}")
            for q in (0.5, 0.9, 0.99):
                idx = min(count - 1, max(0, int(q * count) - 1 if q < 1 else count - 1))
                # standard nearest-rank style
                idx = min(count - 1, int(round(q * (count - 1))))
                lines.append(
                    f"hoglah_job_duration_seconds{_labels(quantile=f'{q:.2f}')} {s[idx]:.6f}"
                )
        else:
            lines.append("hoglah_job_duration_seconds_count 0")
            lines.append("hoglah_job_duration_seconds_sum 0")

        lines.append("# HELP hoglah_process_uptime_seconds Seconds since this process started.")
        lines.append("# TYPE hoglah_process_uptime_seconds gauge")
        lines.append(f"hoglah_process_uptime_seconds {time.time() - self._started:.3f}")

        lines.append("")  # trailing newline for scrapers
        return "\n".join(lines)

    def _counter_value(self, name: str, **labels: str) -> float:
        key = (name, tuple(sorted((k, str(v)) for k, v in labels.items())))
        with self._lock:
            return float(self._counters.get(key, 0.0))


# Module singleton — one registry per process is enough for a local worker.
REGISTRY = MetricsRegistry()
