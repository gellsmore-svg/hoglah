# External provider failure catalogue

Part of the 2026-10-01 robustness analysis ([index](README.md)). It covers
failures **outside** Hoglah: Ollama, OpenAI-compatible servers and the five agent
CLIs. Common subprocess failures come first because they apply to every CLI;
provider-specific behaviour follows.

**Evidence labels.**

| Label | Meaning |
|---|---|
| **OBS** | Reproduced here on 2026-10-01 |
| **DOC** | Stated in official documentation |
| **ISSUE** | Official issue tracker |
| **INF** | Strongly supported inference |
| **THEO** | Theoretical |

**Environment.**
- WSL2: Linux 6.18.40.1, Python 3.12.3, ollama-py 0.6.2.
- Ollama server 0.23.4, reached at `localhost:11434` (Windows host).
- Installed CLIs:
  - Claude Code **2.1.287** (`~/.local/bin/claude`)
  - Codex CLI **0.157.1** (`/usr/local/bin/codex`)
  - Grok CLI **1.0.44** (`~/.local/bin/grok`)
- **Gemini CLI and Kiro CLI are not installed.** Their rows are DOC/ISSUE only, plus simulation at the subprocess boundary.

The "Hoglah today" column describes v0.13.0 behaviour. Requirement IDs refer to
[ROBUSTNESS-REQUIREMENTS.md](ROBUSTNESS-REQUIREMENTS.md).

---

## 1. Common subprocess failures (all CLI backends)

These come from `CliAdapter.run` (`src/hoglah/cli_adapters.py:252-331`), not from any one provider.

| ID | Failure | Basis | Hoglah today | Category | Req |
|---|---|---|---|---|---|
| FM-CLI-001 | stdin inherited from the worker; the CLI reads a piped stdin to EOF | OBS (codex) / DOC | Job hangs in PROCESSING. The heartbeat keeps the lease alive, so the job is never reclaimed and never times out unless `timeout_seconds` is set | timeout (hang) | ROB-CLI-001 |
| FM-CLI-002 | No default execution timeout | code | `timeout_seconds` is optional and defaults to `None`. An interactive prompt or hung network leaves the job PROCESSING forever | timeout | ROB-CLI-001 |
| FM-CLI-003 | Prompt starts with `-` and is parsed as an option | OBS (claude, codex) | Prompt `--version` gives exit 0 → COMPLETED with the version string | caller_invalid seen as success | ROB-CLI-001 |
| FM-CLI-004 | Prompt larger than `MAX_ARG_STRLEN` (128 KiB) | OBS | FAILED `[Errno 7] Argument list too long`, unclassified | caller_invalid | ROB-CLI-001 |
| FM-CLI-005 | Worker dies (SIGKILL/SIGTERM/WSL stop); the CLI runs in its own session (`start_new_session=True`) | OBS | Child keeps running and editing the workdir. The job is reclaimed and run again, so side effects happen twice | host_environment / ambiguous | ROB-AMBIG-001, ROB-CLI-001 |
| FM-CLI-006 | Cancel or timeout sends SIGTERM to the group; grandchildren ignore it | code + THEO | After 2 s, SIGKILL goes to the **leader only**, so grandchildren (MCP servers, shells) survive | process_failure | ROB-CLI-001 |
| FM-CLI-007 | Exit 0 with empty stdout | OBS | COMPLETED with `''` | malformed_response | ROB-BACKEND-001 |
| FM-CLI-008 | Answer written only to stderr, exit 0 | OBS | COMPLETED with `''`; the answer is buried in `metadata.stderr` | malformed_response | ROB-BACKEND-001 |
| FM-CLI-009 | Non-zero exit; the real error is on stdout while stderr holds a warning | OBS (claude) | The error is built from `stderr or stdout`, so the warning is recorded and the cause is lost | protocol_error | ROB-BACKEND-001 |
| FM-CLI-010 | Killed by a signal (SIGSEGV, OOM-kill) | OBS | `exited -11: <partial stdout>`. The signal is unnamed, the partial answer appears as the error, and nothing is classified | process_failure | ROB-BACKEND-001 |
| FM-CLI-011 | Streaming on: multi-byte UTF-8 split across 256-byte reads | OBS | **Stored output corrupted** (601 U+FFFD in a Hebrew answer), still COMPLETED | malformed_response (silent) | ROB-BACKEND-001 |
| FM-CLI-012 | Huge stdout or stderr | code + THEO | `communicate()` buffers everything in worker memory; there is no cap | resource_exhaustion | ROB-BACKEND-001 |
| FM-CLI-013 | Binary moved or replaced mid-update; `cli_binaries` path missing | code | `FileNotFoundError` → FAILED, unclassified | configuration_error | ROB-CLASS-001 |
| FM-CLI-014 | Every non-zero CLI exit | OBS | `classify_error` returns `[]` for auth, bad model and hang alike. Never retried, with no cause recorded | unknown | ROB-CLASS-001 |
| FM-CLI-015 | CLI retries internally underneath Hoglah's retries | OBS (codex 5×), ISSUE (gemini ≥3 min) | Attempt duration and cost multiply; the job timeout must cover the CLI's own retries | timeout | ROB-RETRY-001 |
| FM-CLI-016 | Version drift: renamed flags or changed output conventions | DOC (each CLI changes often) | Flags are hard-coded. Today all exist, verified against `--help`. Drift would show up as an unclassified failure, or as wrong success if the output shape changes | protocol_error | ROB-BACKEND-001 |
| FM-CLI-017 | A broker message selects a CLI backend (`backend` is accepted from brokers) | code | Remote message → local agent run in `cli_workdir`, with tools auto-approved if `cli_yolo` is on | authorisation | ROB-SEC-001 |

## 2. Claude Code (installed 2.1.287)

Sources: the [headless documentation](https://code.claude.com/docs/en/headless) plus probes.

| ID | Failure | Basis | Observed or documented signal | Hoglah today | Req |
|---|---|---|---|---|---|
| FM-CLAUDE-001 | Not logged in | OBS | rc 1, `Not logged in · Please run /login` on **stdout**, 0.75 s | FAILED, unclassified, not retried (correct by accident) | ROB-BACKEND-001 |
| FM-CLAUDE-002 | Unknown model | OBS | rc 1; error on stdout ("There's an issue with the selected model …"); catalog warning on stderr | Error text becomes the stderr warning (FM-CLI-009) | ROB-BACKEND-001 |
| FM-CLAUDE-003 | Failures inside a run | DOC: "Claude Code prints the failure as the result on stdout" | stdout, non-zero exit | Same mis-selection as FM-CLI-009 | ROB-BACKEND-001 |
| FM-CLAUDE-004 | Rate limit, overload, billing, auth, model_not_found, server error | DOC: `stream-json` emits `system/api_retry` with `error` ∈ {`authentication_failed`, `billing_error`, `rate_limit`, `overloaded`, `invalid_request`, `model_not_found`, `server_error`, …} | structured | Hoglah uses `--output-format text` and discards this | ROB-BACKEND-001 |
| FM-CLAUDE-005 | SIGTERM | DOC | exit 143. The in-progress turn is left unfinished and no result is recorded; running Bash process trees are killed | Treated as cancel; outcome of tool side effects is unknown | ROB-AMBIG-001 |
| FM-CLAUDE-006 | Untrusted workdir content runs | DOC: without `--bare`, `-p` runs project hooks and `.mcp.json` servers "even in a folder you've never trusted" | — | Hoglah does not pass `--bare` | ROB-SEC-001 |
| FM-CLAUDE-007 | Piped stdin | OBS + DOC | Waits 3 s for data, then proceeds; 10 MB cap | Inherited stdin causes 3 s latency per job | ROB-CLI-001 |
| FM-CLAUDE-008 | Background subagent keeps `-p` open | DOC | Up to 10 min idle ceiling | No job timeout, so the slot is held | ROB-CLI-001 |
| FM-CLAUDE-009 | Unattended permission prompt | DOC | `dontAsk` denies; `--permission-prompts none` (≥2.1.259) tells the model not to retry denied calls | Uses `dontAsk`; a denied tool leaves silently partial work that still exits 0 | ROB-BACKEND-001 |

## 3. Codex CLI (installed 0.157.1)

Sources: [non-interactive mode docs](https://learn.chatgpt.com/docs/non-interactive-mode) and `codex exec --help`.

| ID | Failure | Basis | Signal | Hoglah today | Req |
|---|---|---|---|---|---|
| FM-CODEX-001 | Inherited open stdin pipe | OBS + DOC ("If stdin is piped and a prompt is also provided, stdin is appended") | Prints `Reading additional input from stdin...` and **blocks indefinitely** | Job hangs forever with no timeout | ROB-CLI-001 |
| FM-CODEX-002 | No credentials (empty `CODEX_HOME`) | OBS | WebSocket 401, falls back to HTTPS, `Reconnecting... 1/5 … 5/5`, rc 1 | FAILED, unclassified, after a long internal retry | ROB-BACKEND-001 |
| FM-CODEX-003 | `CODEX_HOME` path does not exist | OBS | rc 1 immediately, config error | FAILED, unclassified | ROB-CLASS-001 |
| FM-CODEX-004 | Prompt parsed as a flag | OBS | `--version` gives rc 0 | COMPLETED with the version string | ROB-CLI-001 |
| FM-CODEX-005 | Exit codes undocumented | DOC (absence) | Structured failure only via `--json` `turn.failed` / `error` events | Text mode, so no structured signal | ROB-BACKEND-001 |
| FM-CODEX-006 | Required MCP server fails to start | DOC | `exec` exits with an error | Unclassified | ROB-BACKEND-001 |

## 4. Grok CLI (installed 1.0.44)

| ID | Failure | Basis | Signal | Hoglah today | Req |
|---|---|---|---|---|---|
| FM-GROK-001 | Not signed in | OBS | rc 1; stderr `Not signed in … grok login --device-code`, or set `XAI_API_KEY`. Never prompts in `-p` mode | FAILED, unclassified | ROB-BACKEND-001 |
| FM-GROK-002 | Unknown model | OBS | rc 1; stderr `Invalid params: "unknown model id"` | FAILED, unclassified | ROB-BACKEND-001 |
| FM-GROK-003 | Prompt-as-flag | OBS | The prompt is the value of `-p`; `--version` gives rc 2 (`a value is required for '--single'`) | FAILED, which is the safe direction | ROB-CLI-001 |
| FM-GROK-004 | Large prompt | help text | `--prompt-file` exists and avoids argv limits and injection | Unused | ROB-CLI-001 |

## 5. Gemini CLI (not installed: DOC/ISSUE only)

Sources: [headless.md](https://github.com/google-gemini/gemini-cli/blob/main/docs/cli/headless.md) and [troubleshooting.md](https://github.com/google-gemini/gemini-cli/blob/main/docs/resources/troubleshooting.md).

| ID | Failure | Basis | Signal | Hoglah today | Req |
|---|---|---|---|---|---|
| FM-GEMINI-001 | Exit-code contract | DOC | 0 ok · 1 general/API · **41** auth · **42** input · **44** sandbox · **52** config · **53** turn limit | All collapse to "exited N", unclassified | ROB-BACKEND-001 |
| FM-GEMINI-002 | Quota exhausted causes a long hang before failing | ISSUE [#10925](https://github.com/google-gemini/gemini-cli/issues/10925) | 3+ min, then 429 | No default timeout, so the slot is held | ROB-CLI-001 |
| FM-GEMINI-003 | 429 "quota exhausted" text printed during a run that **succeeds** | ISSUE [#17906](https://github.com/google-gemini/gemini-cli/issues/17906) | stderr noise with exit 0 | Any text-scraping classifier misfires, so key on exit code and JSON | ROB-CLASS-001 |
| FM-GEMINI-004 | Repo-wide memory scan burns quota at once (429) | ISSUE [#22635](https://github.com/google-gemini/gemini-cli/issues/22635) | 429 | Retried as rate_limit, which worsens the ban | ROB-RETRY-001 |
| FM-GEMINI-005 | JSON output error object | DOC | `--output-format json` carries `response`, `stats` and `error` | Text mode | ROB-BACKEND-001 |

## 6. Kiro CLI (not installed: DOC/ISSUE only)

Sources: [exit codes](https://kiro.dev/docs/cli/reference/exit-codes/) and the issue tracker.

| ID | Failure | Basis | Signal | Hoglah today | Req |
|---|---|---|---|---|---|
| FM-KIRO-001 | Exit codes | DOC | 0 ok · 1 failure · 3 MCP startup failure (with `--require-mcp-startup`) | Unclassified | ROB-BACKEND-001 |
| FM-KIRO-002 | Tool approval in `--no-interactive` **fails rather than prompting** | ISSUE [kirodotdev/Kiro#11594](https://github.com/kirodotdev/Kiro/issues/11594) | Tool calls fail and the run continues | Exit 0 with work silently not done (`cli_yolo=False` omits `--trust-all-tools`) | ROB-BACKEND-001 |
| FM-KIRO-003 | stdout carries the UI footer instead of the reply (2.25.0) | ISSUE [awslabs/cli-agent-orchestrator#832](https://github.com/awslabs/cli-agent-orchestrator/issues/832) | `Trust All Tools active` footer | COMPLETED with the wrong payload | ROB-BACKEND-001 |
| FM-KIRO-004 | No PreToolUse hooks in `--no-interactive` | ISSUE [#11598](https://github.com/kirodotdev/Kiro/issues/11598) | — | No guardrail path | ROB-SEC-001 |

## 7. Ollama / local inference (server 0.23.4, ollama-py 0.6.2)

Sources: probes and the [Ollama FAQ](https://docs.ollama.com/faq).

| ID | Failure | Basis | Signal | Hoglah today | Category | Req |
|---|---|---|---|---|---|---|
| FM-OLLAMA-001 | Daemon absent or wrong port | OBS | Connection refused; ollama-py: "Failed to connect to Ollama…" | Retried 3× within **3.1 s total**, then FAILED. Any restart longer than ~3 s fails every queued job | network_transient | ROB-RETRY-001 |
| FM-OLLAMA-002 | Model not installed | OBS | `/api/generate` → 404 `model '…' not found` | Hoglah **auto-pulls inside the job**: registry → 500 "pull model manifest: file does not exist", FAILED, not retried. Offline, the pull fails as a connection error, which **is** retried. A typo'd but real tag triggers a multi-GB download inside the attempt | model_unavailable | ROB-BACKEND-001 |
| FM-OLLAMA-003 | Invalid model name | OBS | 400 `invalid model name` | Not retried (4xx) | caller_invalid | — |
| FM-OLLAMA-004 | Invalid option type | OBS | **HTTP 500** `option "temperature" must be of type float32` | Retried 3× as `server` | caller_invalid seen as 5xx | ROB-BACKEND-001, ROB-CLASS-001 |
| FM-OLLAMA-005 | Empty prompt | OBS | 200, `response:""`, `done_reason:"load"` | COMPLETED with `''` | malformed_response | ROB-BACKEND-001 |
| FM-OLLAMA-006 | Capability mismatch (embed-only model asked to generate) | OBS | 400 `does not support generate` | Not retried | caller_invalid | — |
| FM-OLLAMA-007 | **Silent input truncation** | OBS | 200; `prompt_eval_count == num_ctx` (128 of 1,774 tokens) | `truncated` is set only from `done_reason=="length"`, which also fires for the `num_predict` cap. Input truncation with a natural stop is invisible | context_limit (silent) | ROB-BACKEND-001 |
| FM-OLLAMA-008 | Server overloaded | DOC | 503 when the queue (`OLLAMA_MAX_QUEUE`, default 512) is full | Retried within ~3 s | provider_transient | ROB-RETRY-001 |
| FM-OLLAMA-009 | Insufficient memory to load a model | DOC: "all new requests will be queued until the new model can be loaded" | Indefinite wait | `ollama.AsyncClient` timeout is **None**, so the job hangs unless `timeout_seconds` is set | resource_exhaustion / timeout | ROB-CLI-001 |
| FM-OLLAMA-010 | Runner crash or OOM mid-generation | INF | 500 from the runner | Retried as `server`; repeated OOM is retried 3× | resource_exhaustion | ROB-CLASS-001 |
| FM-OLLAMA-011 | Stream ends without a `done:true` chunk | code + THEO | — | Partial text returned as success (`_run_stream` never checks `done`) | partial_response | ROB-BACKEND-001 |
| FM-OLLAMA-012 | Non-finite embedding | code | — | Raised as RuntimeError (good), unclassified | malformed_response | ROB-CLASS-001 |
| FM-OLLAMA-013 | WSL→Windows host address or gateway change | INF | Connection refused or timeout | network_transient retry, ~3 s budget | network_transient | ROB-RETRY-001 |

## 8. OpenAI-compatible HTTP (vLLM, llama.cpp server, LM Studio)

| ID | Failure | Basis | Hoglah today | Req |
|---|---|---|---|---|
| FM-OPENAI-001 | Slow or hung server | OBS | **Blocking urllib on the event loop**: the worker freezes for the whole call (12.1 s observed). No heartbeats, polls or cancels run, and `asyncio.wait_for` cannot fire | ROB-LOOP-001 |
| FM-OPENAI-002 | SSE stream without `[DONE]` or `finish_reason` | code | Partial output accepted as COMPLETED | ROB-BACKEND-001 |
| FM-OPENAI-003 | Malformed SSE chunk | code | Silently skipped (`except JSONDecodeError: continue`) | ROB-BACKEND-001 |
| FM-OPENAI-004 | `metadata.api_key` per job | OBS (E24) | Key copied into result, output file (twice), callback and broker payloads | ROB-SEC-001 |
| FM-OPENAI-005 | 4xx/5xx with body | code | `status_code` attribute set: structured, the one adapter that does this | — |
