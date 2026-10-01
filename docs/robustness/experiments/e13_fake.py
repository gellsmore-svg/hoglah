import tempfile, time, sys, os
from hoglah import Hoglah
d=tempfile.mkdtemp(); fake=os.path.abspath("fakecli.py")
stream = sys.argv[1]=="stream"
cfg={"db_path":f"{d}/q.db","log_level":"CRITICAL","cli_binaries":{"claude":fake},"cli_workdir":d}
if stream: cfg["stream_dir"]=f"{d}/streams"
h=Hoglah(config=cfg, use_real=True)
expected=("שלום עולם — "*200)
for p in ["utf8","empty","stderr_only","partial_crash","--version"]:
    r=h.wait(h.submit(prompt=p, model="fake", backend="claude", max_retries=0), timeout=30)
    extra = f"intact={r.output==expected} replacement_chars={r.output.count(chr(0xFFFD))}" if p=="utf8" and r.output else ""
    print(f"stream={stream} {p:14} -> {r.status.value:9} output={(r.output or '')[:50]!r} {extra} err={(r.error or '')[:60]!r}")
h.close()
