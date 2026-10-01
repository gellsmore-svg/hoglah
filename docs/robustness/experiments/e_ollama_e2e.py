import tempfile, time, logging
from hoglah import Hoglah
S="hf.co/bartowski/SmolLM2-135M-Instruct-GGUF:Q8_0"
attempts=[]
class H(logging.Handler):
    def emit(self, rec):
        m=rec.getMessage()
        if "attempt" in m and "failed" in m: attempts.append(m)
logging.getLogger("hoglah").addHandler(H())
def run(label, host, **kw):
    d=tempfile.mkdtemp(); attempts.clear()
    h=Hoglah(config={"db_path":f"{d}/q.db","ollama_host":host,"log_level":"WARNING"}, use_real=True)
    t=time.monotonic(); r=h.wait(h.submit(**kw), timeout=180); dt=time.monotonic()-t
    print(f"{label:22} -> {r.status.value:9} attempts={len(attempts)} {dt:5.1f}s err={(r.error or '')[:90]!r} out={(r.output or '')[:30]!r}")
    h.close()
run("missing model", "http://localhost:11434", prompt="hi", model="no-such-model:zz")
run("bad option type", "http://localhost:11434", prompt="hi", model=S, options={"temperature":"hot"})
run("host unreachable", "http://localhost:11435", prompt="hi", model=S)
run("empty prompt", "http://localhost:11434", prompt="", model=S)
