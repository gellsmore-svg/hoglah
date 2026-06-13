"""Hoglah — lightweight local-first Ollama job queue manager.

A simple, persistent job queue and orchestration layer for running LLM
inference (via Ollama) in resource-constrained environments.

See docs/requirements-v1.0.md and the README for the full specification.
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from .adapters import BaseAdapter, OllamaAdapter, StubAdapter
from .client import Hoglah, HoglahConfig
from .models import JobResult, JobStatus, JobRequest

# Single source of truth is pyproject.toml; read it from the installed
# package metadata so __version__ can never drift from the wheel again.
try:
    __version__ = _pkg_version("hoglah")
except PackageNotFoundError:  # not installed (e.g. raw source tree)
    __version__ = "0.0.0+source"
del _pkg_version, PackageNotFoundError

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
