# Robustness experiments (2026-10-01)

Fault-injection and probe scripts behind [`../FAILURE-CATALOG.md`](../FAILURE-CATALOG.md).
They are **evidence artefacts, not tests**: each reproduces one finding against
Hoglah v0.13.0 (`f0d322a`) and prints the observed outcome. None modifies
production code; they patch store methods on a live instance or drive a fake
CLI. The deterministic versions belong in the test suite (ROB-TEST-001).

Run from this directory with the repo venv (`../../../.venv/bin/python <script>`).
`fakecli.py` stands in for an agent CLI (behaviour selected by the prompt) and is
wired in through `cli_binaries`, so no provider tokens are spent.

| Exp | Script | Injection | Observed (recorded) |
|---|---|---|---|
| E01 | `e04_hol.py` (`N=60`) | 60 priority-10 children `depends_on` a priority-0 parent submitted last | all 61 jobs `queued` forever, no log. `N=10` completes (poll window 50 with default `affinity_reorder`) |
| E02 | `e05_cancel_race.py` | `cancel()` stalled between its status read and write while the worker completes | callback got `completed`; row ends `cancelled`, `result.output=None`; output file `cancelled` |
| E03 | `e06_hb.py` | first `heartbeat()` raises `database is locked` (lease 5 s, job 9 s) | `final: completed`, **2 executions**, nothing at WARNING |
| E04 | `e07_postcommit.py` | `store.get()` raises once right after a successful `set_result` | store `completed`, output file `failed`, named callback never fired |
| E05 | `e12_galeed_down.py` | `galeed_enabled=True`, Galeed Mongo on a closed port | `submit()` blocked 2.08 s; direct callback lost; 3×30 ms stub jobs took 12.6 s |
| E06 | `e11_openai_block.py` (and the variant without `timeout_seconds`) | OpenAI-compatible server answers after 12 s | event loop frozen 12.1 s (3.1 s with `timeout_seconds=3`, ended by the socket timeout) |
| E07 | `e13_fake.py stream` / `nostream` | fake CLI: Hebrew split mid-character, empty stdout, stderr-only answer, partial+SIGSEGV | streaming: `completed` with 601 U+FFFD; empty and stderr-only → `completed ''`; SIGSEGV → `failed`, error = partial answer text |
| E08 | inline in the report (`build_argv` + real CLIs) | prompt `"--version"` | claude 2.1.287 and codex 0.157.1 exit 0 with their version string; grok 1.0.44 rc 2 |
| E09 | shell probe (see FAILURE-CATALOG FM-CODEX-001) | real CLIs with stdin = open pipe | codex blocks (`Reading additional input from stdin...`) until killed; claude waits 3 s; grok unaffected |
| E10 | shell probe + `classify_real.py` | real CLIs with an empty `HOME`/`CODEX_HOME`; bad model | claude "Not logged in" on **stdout**; grok device-code hint on stderr; codex 5 internal reconnects then 401; `classify_error` → `[]` for every case |
| E11 | `classify_real.py` | synthetic messages | `"num_ctx=512 exceeded"`, `"a connection between ideas"`, `"returned 503 tokens"` → retried as transient |
| E12 | `e10_kill.sh` | `kill -9` worker mid CLI job (`FAKE_SLEEP=8`) | CLI child still alive; reclaimed after ~16 s; re-run despite `max_retries=0` → 2 side effects |
| E13 | `e10b_poison.sh` | job whose CLI SIGKILLs the worker | 4 worker runs → 4 executions; row still `processing` |
| E14 | `e18_sigterm.sh` | SIGTERM to `hoglah run` | exit 143 at once, no drain, CLI child orphaned, row `processing` |
| E15 | `e09_redeliver.py` | 205 jobs with a named callback, then two restarts | each restart (even `start_worker=False`) re-fires 200; newest 5 never included |
| E16 | `e14_stream_fail.py` | `ChunkWriter.write_chunk` → ENOSPC; read-only `stream_dir` | job `failed` `[Errno 28]`; job `failed` "Unexpected worker error" |
| E17 | `e20_skew.py` | peer process whose clock is +60 s runs `reclaim_stale_leases` | live job reclaimed, **2 executions** |
| E18 | `e19_kafka_skip.py` | fake transport with Kafka commit semantics; store fails once on `c1` | `c1` absent from store after restart; offset committed past it |
| E19 | curl probes (FAILURE-CATALOG FM-OLLAMA-*) | Ollama 0.23.4 raw API | 404 missing model; 400 invalid name; `temperature:"hot"` → **500**; empty prompt → 200 `done_reason:"load"`; `num_ctx=128` with 1,774-token prompt → 200, `prompt_eval_count=128` |
| E20 | `e_ollama_e2e.py` | the same through Hoglah `use_real=True` | bad option retried 3×; host down → 3 attempts in 3.1 s then `failed`; empty prompt → `completed ''` |
| E21 | inline (fake CLI, 200 KiB prompt) | argv size | `failed` `[Errno 7] Argument list too long` (100 KiB ok) |
| E22 | `env HOGLAH_CLI_BINARIES=claude=/x …` | documented env formats | `SettingsError` for CLI_BINARIES, MODEL_SLOTS, TAG_SLOTS, OLLAMA_HOSTS; JSON form works |
| E23 | `e07b_precommit.py` | `store.get()` raises once in the pre-commit race guard after provider success | provider called once; job persisted `failed` "Unexpected worker error" |
| E24 | inline | `submit(metadata={"api_key": …})` | secret in `JobResult.metadata`, stored `result_json`, and twice in the output file |
| E25 | shell (`echo n \| hoglah clear --db <tmp>`) | 3 QUEUED jobs, answer **n** at the prompt | prints "Clear cancelled." but all 3 rows are deleted (delete runs before `typer.confirm`) |
| E26 | `e26_poison_row.py` | one SQLite row with corrupt `request_json`, 3 healthy jobs | healthy jobs never claimed; "Error in worker loop" every second; `stats()` still reports 4 queued |

`worker.py` is the long-running worker used by the kill/poison shell scripts.
Shell scripts that need a CLI binary map pass it as JSON
(`HOGLAH_CLI_BINARIES='{"claude":"/abs/fakecli.py"}'`) because of E22.
