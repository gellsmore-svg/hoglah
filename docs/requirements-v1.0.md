# Hoglah Project Requirements Specification

**Project Name**: Hoglah  
**Version**: 1.0 (Initial)  
**Theme**: Named after one of the daughters of Zelophehad (Numbers 26/27/36, Joshua 17), continuing the Old Testament women's names pattern (Mahlah, Tirzah, etc.).

**Captured**: 2026-06-12 (verbatim from initial operator submission)

---

This document captures the operator's initial Hoglah requirements **verbatim**. Subsequent requirement versions should be filed alongside this one (`requirements-v1.1.md`, etc.) rather than overwriting it, so the evolution of the spec stays inspectable.

## 1. Description / Overview
Hoglah is a lightweight, local-first job queue manager and Ollama wrapper designed for resource-constrained environments. It enables applications to submit LLM inference requests asynchronously (or with controlled concurrency), receive a job ID immediately, monitor progress, retrieve results, and receive completion callbacks. 

The tool addresses the common challenge of running multiple background AI/agent requests when hardware only supports serial (or very limited parallel) execution. It acts as an Ollama orchestration layer, supporting models like Gemma, Qwen, Mistral, DeepSeek, and others via Ollama.

**Core Value Proposition**:
- Simple Python-native interface for internal use.
- Reliable queuing with persistence.
- Smart handling of context windows and model capabilities.
- Extensible to web APIs, webhooks, and distributed backends in future versions.
- Fully local, privacy-focused, zero-cloud dependency.

**Target Users**: Developers building multi-agent systems, background task processors, or local AI tools that need to queue and manage LLM calls safely.

## 2. Goals
- Provide a clean, reliable abstraction over Ollama for queuing.
- Support configurable concurrency (default: 1 for low-resource setups).
- Handle model discovery, context calibration, and resource awareness.
- Enable fire-and-forget + callback patterns for workflow orchestration.
- Be easy to integrate into existing Python applications.
- Survive restarts with persistent job state.
- Keep V1 simple, focused, and production-ready for local use.

## 3. Non-Goals (V1)
- Full distributed orchestration or high-availability clustering.
- Built-in web UI (deferred to V2).
- Advanced authentication/multi-tenancy (local use only).
- Support for non-Ollama backends (can be added later).
- Real-time streaming UI (file/callback sufficient for V1).

## 4. Functional Requirements

### 4.1 Core Job Management
- **Submit Job**: Accept a request and return a unique Job ID (UUID) immediately.
- **Job Status**: Query status (queued, processing, completed, failed, cancelled).
- **Retrieve Result**: Get full output, metadata, and stats once complete.
- **List Jobs**: Filter by status, tags, etc.
- **Cancel Job**: Best-effort cancellation of a job by ID.
- **Persistence**: Jobs and state survive process restarts.

### 4.2 Job Parameters (Submit API)
```python
job_id = hoglah.submit(
    prompt: str | None = None,                    # or messages for chat
    messages: list[dict] | None = None,           # OpenAI-style chat history
    model: str,                                   # e.g. "gemma:7b", "mistral"
    system_prompt: str | None = None,
    num_ctx: int | None = None,                   # Context window size
    options: dict | None = None,                  # Passthrough for llama.cpp params
    callback: Callable[[JobResult], None] | None = None,  # Python callable
    callback_url: str | None = None,              # V2: HTTP webhook
    tags: list[str] | None = None,
    priority: int = 0,                            # Higher = earlier
    timeout_seconds: int | None = None,
    max_retries: int = 2,
    metadata: dict | None = None,                 # User-defined data
    parent_job_id: str | None = None,             # For chaining/dependencies
    # Additional important parameters:
    temperature: float | None = None,
    top_p: float | None = None,
    top_k: int | None = None,
    repeat_penalty: float | None = None,
    seed: int | None = None,                      # Reproducibility
    stop: list[str] | None = None,                # Stop sequences
    num_predict: int | None = None,               # Max output tokens
    format: str | None = None,                    # e.g. "json"
    keep_alive: str | int | None = None,          # Model keep-alive time
    # ... full options dict covers the rest
)
```

*(Note: The initial submission cut off here. Further sections such as non-functional requirements, success criteria, result shape definition, error semantics, CLI surface, etc. can be added in follow-up revisions.)*

---

**Source**: Direct paste from user query on 2026-06-12. Verbatim capture.
