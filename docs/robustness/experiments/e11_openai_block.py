import tempfile, time, json, threading, asyncio
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from hoglah import Hoglah
from hoglah.http_adapter import OpenAICompatAdapter
from hoglah.routing import AdapterRegistry
from hoglah.adapters import StubAdapter
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"])); time.sleep(12)
        b=json.dumps({"id":"x","choices":[{"message":{"content":"late answer"},"finish_reason":"stop"}],"usage":{}}).encode()
        self.send_response(200); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def log_message(self,*a): pass
srv=ThreadingHTTPServer(("127.0.0.1",0),H); threading.Thread(target=srv.serve_forever,daemon=True).start()
d=tempfile.mkdtemp()
oa=OpenAICompatAdapter(base_url=f"http://127.0.0.1:{srv.server_port}/v1")
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL","concurrency":4}, adapter=StubAdapter())
h._registry=AdapterRegistry("stub",{"stub":StubAdapter(),"openai":oa})   # route by backend
# probe event-loop responsiveness from inside the worker loop
gaps=[]
async def ticker():
    last=time.monotonic()
    while True:
        await asyncio.sleep(0.1); now=time.monotonic(); gaps.append(now-last); last=now
while h._worker_loop_ref is None: time.sleep(0.05)
asyncio.run_coroutine_threadsafe(ticker(), h._worker_loop_ref)
t=time.monotonic(); jid=h.submit(prompt="x", model="gpt", backend="openai", timeout_seconds=3)
stub=h.submit(prompt="y", model="m")       # unrelated job queued right behind it
r=h.wait(jid, timeout=60); t1=time.monotonic()-t
s=h.wait(stub, timeout=60)
print(f"openai job: {r.status.value} after {t1:.1f}s (timeout_seconds=3) output={r.output!r} err={r.error!r}")
print(f"max event-loop stall: {max(gaps):.1f}s ; unrelated stub job status: {s.status.value}")
h.close()
