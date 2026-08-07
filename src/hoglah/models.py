"""Core data models for Hoglah.

Guided by the initial requirements in docs/requirements-v1.0.md and decisions
in docs/architecture-decisions.md (esp. ADRs 006, 009, 010, 012).
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable


class JobStatus(str, Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# Named error classes for RetryPolicy.retry_on (gap G2).
RETRY_ON_CLASSES = frozenset(
    {
        "transient",  # connection | timeout-in-message | rate_limit | server
        "connection",
        "timeout",  # error-message timeouts *and* optional job timeout_seconds retries
        "rate_limit",
        "server",  # 5xx / unavailable
        "oom",  # CUDA / host OOM — not in default "transient"
        "all",  # any exception (still respects max_retries)
        "none",  # never retry
    }
)


@dataclass(frozen=True)
class RetryPolicy:
    """Per-job retry policy (gap G2).

    ``max_retries`` is the number of *additional* attempts after the first
    (total attempts = max_retries + 1). ``0`` means try once and fail.

    ``retry_on`` is a list of named classes (see ``RETRY_ON_CLASSES``). Default
    ``("transient",)`` matches the historical worker behaviour. ``oom`` is
    intentionally separate so large-model OOM is not retried by default.
    Job-level ``timeout_seconds`` failures are only retried when ``timeout`` or
    ``all`` is listed (ADR-011 default: terminal).
    """

    max_retries: int = 2
    base_delay: float = 1.0
    backoff_factor: float = 2.0
    max_delay: float = 10.0
    jitter: float = 0.0  # 0..1 equal-jitter fraction; 0 = deterministic
    retry_on: tuple[str, ...] = ("transient",)

    def to_dict(self) -> dict[str, Any]:
        return {
            "max_retries": int(self.max_retries),
            "base_delay": float(self.base_delay),
            "backoff_factor": float(self.backoff_factor),
            "max_delay": float(self.max_delay),
            "jitter": float(self.jitter),
            "retry_on": list(self.retry_on),
        }

    @classmethod
    def from_any(
        cls,
        value: RetryPolicy | dict[str, Any] | None = None,
        *,
        max_retries: int | None = None,
    ) -> RetryPolicy:
        """Build a policy from a RetryPolicy, dict, or max_retries-only legacy.

        When ``value`` is a full ``RetryPolicy``, it wins. When ``value`` is a
        dict, ``max_retries`` fills in only if the dict omits that key. When
        ``value`` is None, ``max_retries`` (default 2) builds a default policy.
        """
        if isinstance(value, cls):
            policy = value
        elif isinstance(value, dict):
            retry_on = value.get("retry_on", ("transient",))
            if isinstance(retry_on, str):
                retry_on = [p.strip() for p in retry_on.split(",") if p.strip()]
            mr = value.get("max_retries", max_retries if max_retries is not None else 2)
            policy = cls(
                max_retries=int(mr),
                base_delay=float(value.get("base_delay", 1.0)),
                backoff_factor=float(value.get("backoff_factor", 2.0)),
                max_delay=float(value.get("max_delay", 10.0)),
                jitter=float(value.get("jitter", 0.0)),
                retry_on=tuple(retry_on),
            )
        elif value is None:
            policy = cls(max_retries=2 if max_retries is None else int(max_retries))
        else:
            raise TypeError(
                f"retry_policy must be RetryPolicy, dict, or None; got {type(value).__name__}"
            )

        if policy.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if policy.base_delay < 0 or policy.max_delay < 0:
            raise ValueError("base_delay and max_delay must be >= 0")
        if policy.backoff_factor <= 0:
            raise ValueError("backoff_factor must be > 0")
        if not (0.0 <= policy.jitter <= 1.0):
            raise ValueError("jitter must be between 0 and 1 inclusive")
        unknown = [c for c in policy.retry_on if c not in RETRY_ON_CLASSES]
        if unknown:
            raise ValueError(
                f"unknown retry_on class(es) {unknown!r}; "
                f"allowed: {sorted(RETRY_ON_CLASSES)}"
            )
        return policy

    def delay_for_attempt(self, attempt: int, *, rng: random.Random | None = None) -> float:
        """Seconds to sleep after a failed attempt (0-based attempt index).

        Matches the historical ``min(2 ** attempt, max_delay)`` curve when
        defaults are used (base_delay=1, backoff_factor=2).
        """
        if attempt < 0:
            attempt = 0
        raw = min(
            self.base_delay * (self.backoff_factor ** attempt),
            self.max_delay,
        )
        if self.jitter > 0:
            r = rng if rng is not None else random
            # Equal jitter: delay * (1 - j + 2*j*U) → uniform in [(1-j)d, (1+j)d]
            j = self.jitter
            raw = raw * (1.0 - j + 2.0 * j * r.random())
        return max(0.0, float(raw))

    def should_retry(self, exc: Exception, *, job_timeout: bool = False) -> bool:
        """Whether this exception is retryable under the policy."""
        classes = set(self.retry_on)
        if "none" in classes:
            return False
        if "all" in classes:
            return True
        if job_timeout:
            # ADR-011 default: wall-clock timeout is terminal unless opted in.
            return "timeout" in classes
        matched = classify_error(exc)
        return bool(matched & classes)


def classify_error(exc: Exception) -> set[str]:
    """Map an exception to named retry classes."""
    msg = str(exc).lower()
    found: set[str] = set()

    if any(
        x in msg
        for x in (
            "connection",
            "connect",
            "refused",
            "reset by peer",
            "unreachable",
            "network is unreachable",
            "name or service not known",
        )
    ):
        found.add("connection")

    if "timeout" in msg or type(exc).__name__.lower().endswith("timeout"):
        found.add("timeout")

    if any(x in msg for x in ("rate limit", "rate_limit", "429", "too many requests", "throttl")):
        found.add("rate_limit")

    if any(
        x in msg
        for x in (
            " 500",
            "500 ",
            "502",
            "503",
            "504",
            "5xx",
            "server error",
            "internal server",
            "unavailable",
            "service unavailable",
        )
    ) or msg.strip().startswith("500"):
        found.add("server")

    if any(
        x in msg
        for x in (
            "out of memory",
            "oom",
            "cuda out of memory",
            "enomem",
            "memory allocation",
            "cannot allocate memory",
        )
    ):
        found.add("oom")

    # Historical "transient" umbrella — excludes oom (opt-in only).
    if found & {"connection", "timeout", "rate_limit", "server"}:
        found.add("transient")

    # Context / validation style errors are never transient.
    if any(x in msg for x in ("context length", "context window", "invalid", "bad request", "400")):
        found.discard("transient")

    return found


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
    max_retries: int = 2  # legacy; kept in sync with retry_policy.max_retries
    # Full policy (gap G2), stored as a plain dict for JSON persistence.
    # Use RetryPolicy / submit(retry_policy=…) to set; worker reads via
    # effective_retry_policy().
    retry_policy: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None
    parent_job_id: str | None = None

    # Delayed / scheduled enqueue (gap G1). ISO-8601 UTC timestamp; the worker
    # will not claim a QUEUED job until `run_at` is in the past (or null = due
    # immediately). Prefer submit(delay_seconds=…) / submit(run_at=…) over
    # setting this field by hand — those helpers normalise the value.
    run_at: str | None = None

    # Client-side idempotent submit (gap G6). Distinct from messaging
    # correlation_id (bridge redelivery). When set, a second submit with the
    # same key returns the existing job id and does not create a new row.
    idempotency_key: str | None = None

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


def effective_retry_policy(request: JobRequest | dict[str, Any] | Any) -> RetryPolicy:
    """Resolve the policy stored on a JobRequest (or dict-like)."""
    if isinstance(request, dict):
        raw = request.get("retry_policy")
        max_r = request.get("max_retries")
    else:
        raw = getattr(request, "retry_policy", None)
        max_r = getattr(request, "max_retries", None)
    if raw is not None:
        return RetryPolicy.from_any(raw)
    if max_r is None:
        return RetryPolicy()
    # Preserve max_retries=0 (do not treat as falsy → 2).
    return RetryPolicy.from_any(None, max_retries=int(max_r))


def new_job_id() -> str:
    """Generate a new job identifier (UUID4 string)."""
    return str(uuid.uuid4())


def normalize_request(**kwargs: Any) -> JobRequest:
    """Helper to build a clean JobRequest from submit() kwargs.

    Strips None values for optional fields where sensible and ensures model is present.
    Coerces ``retry_policy`` from ``RetryPolicy`` to a dict for persistence.
    """
    # Remove keys that are not part of JobRequest
    known_fields = {f.name for f in JobRequest.__dataclass_fields__.values()}
    data = {k: v for k, v in kwargs.items() if k in known_fields and v is not None}
    if "retry_policy" in data and isinstance(data["retry_policy"], RetryPolicy):
        data["retry_policy"] = data["retry_policy"].to_dict()
    return JobRequest(**data)


def resolve_run_at(
    *,
    run_at: datetime | str | None = None,
    delay_seconds: float | int | None = None,
    now: datetime | None = None,
) -> str | None:
    """Normalise schedule inputs to a UTC ISO-8601 string (or None = run now).

    Pass at most one of `run_at` / `delay_seconds`. Naive datetimes are treated
    as UTC. Strings are parsed with ``datetime.fromisoformat`` (``Z`` accepted).
    """
    if run_at is not None and delay_seconds is not None:
        raise ValueError("pass only one of run_at or delay_seconds")
    if delay_seconds is not None:
        if float(delay_seconds) < 0:
            raise ValueError("delay_seconds must be >= 0")
        base = now or datetime.now(timezone.utc)
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        return (base + timedelta(seconds=float(delay_seconds))).astimezone(timezone.utc).isoformat()
    if run_at is None:
        return None
    if isinstance(run_at, datetime):
        dt = run_at if run_at.tzinfo is not None else run_at.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    s = str(run_at).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"run_at is not a valid ISO-8601 datetime: {run_at!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()
