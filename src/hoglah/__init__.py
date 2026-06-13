"""Hoglah — lightweight local-first Ollama job queue manager.

A simple, persistent job queue and orchestration layer for running LLM
inference (via Ollama) in resource-constrained environments.

See docs/requirements-v1.0.md and the README for the full specification.
"""

__version__ = "0.2.1"

from .adapters import BaseAdapter, OllamaAdapter, StubAdapter
from .client import Hoglah, HoglahConfig
from .models import JobResult, JobStatus, JobRequest

__all__ = [
    "__version__",
    "BaseAdapter",
    "Hoglah",
    "HoglahConfig",
    "JobResult",
    "JobStatus",
    "JobRequest",
    "OllamaAdapter",
    "StubAdapter",
]
