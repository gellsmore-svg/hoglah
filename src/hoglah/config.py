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