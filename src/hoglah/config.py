"""Configuration for Hoglah.

Follows the project scaffold and decisions:
- Pydantic for validation and env support (ADR-012, follow scaffold).
- Lightweight defaults suitable for resource-constrained environments.
- Callbacks registry is passed at runtime to the client (not part of persisted config)
  to support the named callback registry for restart re-delivery (ADR-006).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class HoglahSettings(BaseSettings):
    """Runtime settings for a Hoglah instance.

    Environment variables (all optional):
        HOGLAH_DB_PATH=~/.hoglah/hoglah.db
        HOGLAH_CONCURRENCY=1
        HOGLAH_OLLAMA_HOST=http://localhost:11434
        HOGLAH_LOG_LEVEL=INFO

    Constructor overrides take precedence over env / defaults.
    """

    model_config = SettingsConfigDict(
        env_prefix="HOGLAH_",
        env_file=None,  # explicit config file support can be added later
        extra="ignore",
    )

    # Persistence (SQLite by default per ADR-002)
    backend: str = Field(
        default="sqlite",
        description="Persistence backend: 'sqlite' (default, single-file) or 'mongo' "
        "(MongoDB server — multi-process/multi-machine workers, external queue visibility).",
    )
    db_path: Path = Field(
        default=Path("~/.hoglah/hoglah.db").expanduser(),
        description="Path to the SQLite database file (backend='sqlite'). Parent dir created if needed.",
    )
    # MongoDB backend connection (used when backend='mongo'). pymongo is an
    # optional dependency: install with `pip install 'hoglah[mongo]'`.
    mongo_uri: str = Field(
        default="mongodb://localhost:27017",
        description="MongoDB connection URI (backend='mongo').",
    )
    mongo_db: str = Field(default="hoglah", description="MongoDB database name (backend='mongo').")
    mongo_collection: str = Field(default="jobs", description="MongoDB collection name (backend='mongo').")

    # Kafka bridge (ADR-018) — transport adapter, NOT a storage backend. When
    # enabled, Hoglah consumes job-request messages from an input topic into the
    # JobStore and produces result messages back to Kafka. Off by default; needs
    # the optional extra `pip install "hoglah[kafka]"`. See docs/kafka-bridge-design.md.
    kafka_enabled: bool = Field(
        default=False,
        description="Enable the Kafka bridge (consume job requests + produce results). Off by default.",
    )
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated Kafka bootstrap servers (e.g. 'broker1:9092,broker2:9092').",
    )
    kafka_input_topic: str = Field(
        default="hoglah-jobs",
        description="Topic Hoglah consumes job requests from.",
    )
    kafka_results_topic: str = Field(
        default="hoglah-results",
        description="Default topic Hoglah produces results to (overridable per-message by reply_to).",
    )
    kafka_dlt_topic: str = Field(
        default="hoglah-jobs-dlt",
        description="Dead-letter topic for un-processable ('poison') input messages.",
    )
    kafka_group_id: str = Field(
        default="hoglah",
        description="Kafka consumer group id (members share the input-topic partitions).",
    )

    # RabbitMQ bridge (ADR-019) — second messaging transport, same crash-safe
    # MessageBridge as Kafka. Off by default; needs `pip install "hoglah[rabbitmq]"`.
    # Enable at most one of kafka_enabled / rabbitmq_enabled per instance.
    rabbitmq_enabled: bool = Field(
        default=False,
        description="Enable the RabbitMQ bridge (consume job requests + produce results). Off by default.",
    )
    rabbitmq_url: str = Field(
        default="amqp://guest:guest@localhost:5672/",
        description="RabbitMQ connection URL (AMQP).",
    )
    rabbitmq_input_queue: str = Field(
        default="hoglah-jobs",
        description="Queue Hoglah consumes job requests from.",
    )
    rabbitmq_results_queue: str = Field(
        default="hoglah-results",
        description="Default queue Hoglah produces results to (overridable per-message by reply_to).",
    )
    rabbitmq_dlx: str = Field(
        default="hoglah-dlx",
        description="Dead-letter exchange for un-processable ('poison') input messages.",
    )
    rabbitmq_dlq: str = Field(
        default="hoglah-jobs-dlq",
        description="Dead-letter queue bound to the dead-letter exchange.",
    )
    rabbitmq_prefetch: int = Field(
        default=1,
        ge=1,
        description="Max unacknowledged messages a consumer holds at once (backpressure).",
    )
    rabbitmq_declare_topology: bool = Field(
        default=True,
        description="Declare the input/results/dead-letter queues + DLX on startup (idempotent). "
        "Turn off if the operator pre-provisions them on a locked-down cluster.",
    )

    # Redis Streams bridge (ADR-020) — third messaging transport, same crash-safe
    # MessageBridge. Off by default; needs `pip install "hoglah[redis]"`.
    # Enable at most one of kafka_enabled / rabbitmq_enabled / redis_enabled.
    redis_enabled: bool = Field(
        default=False,
        description="Enable the Redis Streams bridge (consume job requests + produce results). Off by default.",
    )
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL.",
    )
    redis_input_stream: str = Field(
        default="hoglah-jobs",
        description="Stream Hoglah consumes job requests from.",
    )
    redis_results_stream: str = Field(
        default="hoglah-results",
        description="Default stream Hoglah produces results to (overridable per-message by reply_to).",
    )
    redis_dlq_stream: str = Field(
        default="hoglah-jobs-dlq",
        description="Dead-letter stream for un-processable ('poison') input messages.",
    )
    redis_group: str = Field(
        default="hoglah",
        description="Redis Streams consumer group (members share the input stream).",
    )
    redis_consumer_name: str = Field(
        default="hoglah-1",
        description="Consumer name within the group. Stable across restarts so a crashed "
        "consumer's pending (unacked) messages are recovered. Use distinct names per process.",
    )

    # Concurrency control (ADR-003)
    concurrency: int = Field(
        default=1,
        ge=1,
        description="Maximum number of concurrent Ollama generations (default 1 for low-resource setups).",
    )

    # Ollama connection
    ollama_host: str | None = Field(
        default=None,
        description="Ollama server URL. If None, the official ollama client defaults are used (usually http://localhost:11434).",
    )

    # Result delivery — output folder (ADR-014). When set, the worker writes
    # each terminal job's full result to `<output_dir>/<job_id>.json` so a
    # decoupled submitter (e.g. a separate process feeding the shared queue)
    # can poll for it. None = disabled.
    output_dir: Path | None = Field(
        default=None,
        description="If set, terminal job results are written to <output_dir>/<job_id>.json for polling. Dir created if needed.",
    )

    # Outbound callback delivery (ADR-015). Applied to per-job callback_url POSTs.
    callback_timeout_seconds: float = Field(
        default=10.0,
        gt=0,
        description="Per-attempt timeout for outbound callback_url POSTs.",
    )
    callback_max_retries: int = Field(
        default=3,
        ge=1,
        description="Number of attempts for an outbound callback POST before giving up (output file remains as fallback).",
    )

    # Logging (ADR-007 / DX)
    log_level: str = Field(
        default="INFO",
        description="Logging level for the 'hoglah' logger (DEBUG, INFO, WARNING, ERROR, CRITICAL).",
    )

    @field_validator("db_path", mode="before")
    @classmethod
    def _expand_db_path(cls, v: Any) -> Path:
        if isinstance(v, (str, os.PathLike)):
            p = Path(v).expanduser()
            return p
        return v

    @field_validator("output_dir", mode="before")
    @classmethod
    def _expand_output_dir(cls, v: Any) -> Any:
        if isinstance(v, (str, os.PathLike)):
            return Path(v).expanduser()
        return v

    def ensure_dirs(self) -> None:
        """Create parent directory for db_path (and output_dir if set)."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        if self.output_dir is not None:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable view (useful for debugging / result metadata)."""
        return {
            "db_path": str(self.db_path),
            "concurrency": self.concurrency,
            "ollama_host": self.ollama_host,
            "output_dir": str(self.output_dir) if self.output_dir else None,
            "log_level": self.log_level,
        }


# Backwards-compatible alias used by client
HoglahConfig = HoglahSettings