# Hoglah robustness: what to fix, in what order

**Reader:** decision-maker (maintainer scheduling the work). Knows what Hoglah
does and who depends on it, not its internals. **Next step:** decide whether
to start Step 1 now and accept one behaviour change. **Budget:** ~3 min read,
cognitive load M.

---

## Why this matters

Hoglah can do the wrong thing today **without reporting an error**. It can run
a coding agent twice, record corrupted output as a success, drop a Kafka
message, and delete queued jobs after you answer "no". Each of these was
reproduced on v0.13.0, not just inferred.

## What "fixed" means

One question: **does every failure leave Hoglah in a known state you can
recover from?** Today the answer is no, and nothing would tell you, because
none of the 232 existing tests kills a process or injects a store fault. The
analysis defines 65 fault-injection tests. A fix counts only when its test
passes.

## The three problems, and what each costs to fix

**1. Silent wrong results: cheap to fix.** Cancel can overwrite a finished job.
A store hiccup can report "failed" for work that succeeded. Non-ASCII output
gets corrupted, and an empty answer counts as success. `hoglah clear` deletes
before it asks. **Step 1** fixes all of these with small diffs and no
database change.

**2. Unknowable outcomes: needs a schema migration, and changes behaviour.**
When a worker dies mid-job, Hoglah cannot tell whether the provider finished,
so it reruns the job. That is harmless for inference but wrong for agents that
edit files, and a job that crashes its worker reruns forever. **Step 3** adds
an attempt log to both stores.

*The behaviour change you are approving:* after Step 3, an interrupted CLI
agent job is **not** rerun automatically. It becomes `FAILED outcome_unknown`
and waits for someone to requeue it. Inference jobs keep at-least-once
reruns.

*Constraint:* the attempt log and the new retry logic must ship together.
Shipped separately, the retries multiply and a job can run (max_retries+1)²
times.

**3. Lost work, a leak, and a noisy dependency: mostly cheap.** The Kafka bridge skips
a message while logging "will retry". If Galeed is down, every submit stalls
2 s and the failures are logged only at DEBUG. A per-job `api_key` is copied
into results, output files and callback payloads. Step 1 fixes the Kafka loss;
**Step 2** fixes the leak and makes Galeed non-blocking.

## What you must take on trust

- **There are no effort estimates.** The order is by risk and dependency. It
  says nothing about duration.
- Some failures were read from the code but not reproduced: RabbitMQ and
  Redis behaviour against live brokers, and Mongo write ambiguity. Their
  severity is unconfirmed either way.
- Gemini CLI and Kiro CLI were not installed, so their behaviour comes from
  documentation only.
- The safer lease check in Step 3 slows recovery after a crash by an amount
  nobody has measured yet.

## Decision

1. **Start Step 1 now.** It is low-risk, needs no schema change, and removes
   every reproduced data-loss and wrong-result bug that can be fixed locally.
2. **Approve the agent-rerun change** before Step 3 is designed, or say which
   agents should still auto-rerun.

Until the fixes land, warn people not to run `hoglah clear`, not to rely on
Kafka for messages that must not be lost, and not to pass API keys in job
metadata.

---

**Coverage:** included the P0 findings with operator-visible consequences,
Steps 1–3, the at-most-once change, and the reproduced vs code-read split.
Excluded internals (lease mechanics, the taxonomy, the Galeed event spec),
and Steps 4–5. The secret leak is kept as a security risk the reader owns
(E24, fixed by ROB-SEC-001 in Step 2). Full reference:
[README.md](README.md) §A, and [ROBUSTNESS-REQUIREMENTS.md](ROBUSTNESS-REQUIREMENTS.md)
§0 and §J.
