"""Core data models for Hoglah.

Guided by the initial requirements in docs/requirements-v1.0.md and decisions
in docs/architecture-decisions.md (esp. ADRs 006, 009, 010, 012).
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
    """Public result for a finished (or failed) job.

    Per ADR-009:
    - Always attempt to produce a result even if context truncation occurred.
    - Include explicit truncation metadata so callers know when the supplied
      prompt/context was (or may have been) truncated.
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

    # Embedding jobs (ADR-013). For kind="embed" the result carries the vector
    # here instead of text in `output`; `output` stays None. `embedding_dim` is
    # len(embedding), recorded so vectors from different models are never
    # compared by accident.
    embedding: list[float] | None = None
    embedding_dim: int | None = None


@dataclass
class JobRequest:
    """Internal representation of a submission request (persisted for execution/retry).

    Captures everything needed to (re)execute the job later. Individual
    generation params (temperature etc.) are kept separate from the raw
    `options` dict so the worker can apply them cleanly.
    """

    # Job kind (ADR-013): "generate" (prompt/chat -> text) or "embed"
    # (prompt holds the input text -> embedding vector). Kept as a plain str
    # so older persisted requests without the field default cleanly.
    kind: str = "generate"

    prompt: str | None = None
    messages: list[dict[str, Any]] | None = None
    model: str = ""
    system_prompt: str | None = None
    num_ctx: int | None = None
    options: dict[str, Any] | None = None
    tags: list[str] | None = None
    priority: int = 0
    timeout_seconds: int | None = None
    max_retries: int = 2
    metadata: dict[str, Any] | None = None
    parent_job_id: str | None = None

    # Generation params (flattened for convenience; merged into options by worker if needed)
    temperature: float | None = None
    top_p: float | None = None
    top_k: int | None = None
    repeat_penalty: float | None = None
    seed: int | None = None
    stop: list[str] | None = None
    num_predict: int | None = None
    format: str | None = None
    keep_alive: str | int | None = None

    # Callback handling (ADR-006)
    callback_key: str | None = None  # if using named registry instead of direct callable

    # Outbound HTTP callback (ADR-015). If set, the worker POSTs the terminal
    # JobResult (as JSON) to this URL — lets a decoupled submitter be pushed
    # the result instead of (or alongside) polling the output folder.
    callback_url: str | None = None


# Type alias for user callbacks (can be passed directly to submit or via registry)
JobCallback = Callable[[JobResult], None]


def new_job_id() -> str:
    """Generate a new job identifier (UUID4 string)."""
    return str(uuid.uuid4())


def normalize_request(**kwargs: Any) -> JobRequest:
    """Helper to build a clean JobRequest from submit() kwargs.

    Strips None values for optional fields where sensible and ensures model is present.
    """
    # Remove keys that are not part of JobRequest
    known_fields = {f.name for f in JobRequest.__dataclass_fields__.values()}
    data = {k: v for k, v in kwargs.items() if k in known_fields and v is not None}
    return JobRequest(**data)
