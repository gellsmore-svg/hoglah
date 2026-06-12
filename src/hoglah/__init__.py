"""Hoglah — lightweight local-first Ollama job queue manager.

A simple, persistent job queue and orchestration layer for running LLM
inference (via Ollama) in resource-constrained environments.

See docs/requirements-v1.0.md and the README for the full specification.
"""

__version__ = "0.1.0"

from .client import Hoglah, HoglahConfig
from .models import JobResult, JobStatus, JobRequest

__all__ = [
    "__version__",
    "Hoglah",
    "HoglahConfig",
    "JobResult",
    "JobStatus",
    "JobRequest",
]
