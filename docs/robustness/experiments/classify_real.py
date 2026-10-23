import sys, glob, os
from hoglah.models import classify_error, RetryPolicy
pairs = {"claude-badmodel":("claude",1,"out.claude","err.claude"),"codex-stdinhang":("codex",-15,"out.codex","err.codex"),
 "grok-badmodel":("grok",1,"out.grok","err.grok"),"claude-noauth":("claude",1,"out.claude-noauth","err.claude-noauth"),
 "codex-noauth":("codex",1,"out.c","err.c"),"grok-noauth":("grok",1,"out.grok-noauth","err.grok-noauth")}
for k,(n,rc,o,e) in pairs.items():
    so=open(o,errors="replace").read(); se=open(e,errors="replace").read()
    snippet=(se or so).strip()[:800]   # exactly as CliAdapter.run builds it
    exc=RuntimeError(f"{n} CLI exited {rc}: {snippet or 'no output'}")
    print(f"{k:16} classes={sorted(classify_error(exc))!s:40} default_retry={RetryPolicy().should_retry(exc)}  msg_head={str(exc)[:90]!r}")
# synthetic false-positive probes for the regex fallback
for m in ["num_ctx=512 exceeded","prompt mentions a connection between ideas","model gemma3:1b returned 503 tokens","Invalid API key (rate limit docs at ...)","killed by signal 9","No space left on device","[Errno 7] Argument list too long"]:
    exc=RuntimeError(m); print(f"{m[:40]:42} -> {sorted(classify_error(exc))} retry={RetryPolicy().should_retry(exc)}")
