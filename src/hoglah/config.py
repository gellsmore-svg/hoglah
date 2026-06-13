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
    db_path: Path = Field(
        default=Path("~/.hoglah/hoglah.db").expanduser(),
        description="Path to the SQLite database file. Parent dir will be created if needed.",
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

    def ensure_dirs(self) -> None:
        """Create parent directory for db_path if it doesn't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def to_dict(self) -> dict[str, Any]:
        """Return a serializable view (useful for debugging / result metadata)."""
        return {
            "db_path": str(self.db_path),
            "concurrency": self.concurrency,
            "ollama_host": self.ollama_host,
            "log_level": self.log_level,
        }


# Backwards-compatible alias used by client
HoglahConfig = HoglahSettings