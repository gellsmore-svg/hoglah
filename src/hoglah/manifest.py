"""Hoglah's Keturah manifest — its LLM-consumable interfaces."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _pkg_version

from keturah import Manifest, capability, manifest


def _version() -> str:
    try:
        return _pkg_version("hoglah")
    except PackageNotFoundError:
        return "0.0.0+source"


def build_manifest() -> Manifest:
    return manifest(
        "hoglah",
        version=_version(),
        description="Local-first durable job queue for LLM/tool execution (Ollama-backed).",
        capabilities=[
            capability(
                "submit_job",
                "Enqueue a prompt/tool job for durable, model-backed execution. Returns a job id and "
                "status; the result is retrievable once the worker completes it.",
                input_schema={
                    "type": "object",
                    "properties": {
                        "prompt": {"type": "string"},
                        "model": {"type": "string"},
                        "priority": {"type": "integer", "description": "lower = higher priority"},
                    },
                    "required": ["prompt"],
                },
                output_schema={
                    "type": "object",
                    "properties": {
                        "job_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["queued", "running", "completed", "failed"]},
                    },
                },
                tags=["queue", "execution"],
            ),
            capability(
                "job_result",
                "Fetch the result/status of a previously submitted job by id.",
                input_schema={"type": "object", "properties": {"job_id": {"type": "string"}}, "required": ["job_id"]},
                output_schema={"type": "object", "properties": {"status": {"type": "string"}, "result": {}}},
                tags=["queue"],
            ),
        ],
    )
