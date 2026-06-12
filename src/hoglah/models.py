"""Core data models for Hoglah (initial stub).

These will be expanded during implementation. The shapes are guided
directly by the requirements in docs/requirements-v1.0.md.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True)
class JobResult:
    """The value returned for a finished (or failed) job.

    Per operator guidance:
    - Always attempt to produce a result even if context truncation occurred.
    - Include explicit truncation metadata so callers know the supplied
      prompt/context was (or may have been) truncated by the model or the
      queue manager.
    """

    job_id: str
    status: JobStatus
    output: str | None = None
    model: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, int] = field(default_factory=dict)  # prompt_tokens, completion_tokens, total
    timings: dict[str, datetime | None] = field(default_factory=dict)
    error: str | None = None
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    parent_job_id: str | None = None

    # Context / truncation handling (ADR-009)
    truncated: bool = False
    truncation_reason: str | None = None  # e.g. "model_context_limit", "num_ctx_exceeded", "prompt_too_long"
    estimated_prompt_tokens: int | None = None
    effective_num_ctx: int | None = None


# Type alias for user callbacks
JobCallback = Callable[[JobResult], None]


def new_job_id() -> str:
    """Generate a new job identifier (UUID4 string)."""
    return str(uuid.uuid4())
