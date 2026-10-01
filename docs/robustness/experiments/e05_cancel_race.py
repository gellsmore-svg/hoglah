import tempfile, time, threading, json, asyncio
from hoglah import Hoglah
from hoglah.adapters import StubAdapter
class Slow(StubAdapter):
    async def run(self, req, **kw):
        await asyncio.sleep(0.4); return await super().run(req, **kw)
d=tempfile.mkdtemp(); delivered=[]
h=Hoglah(config={"db_path":f"{d}/q.db","output_dir":f"{d}/out"}, adapter=Slow())
jid=h.submit(prompt="x", model="m", callback=lambda r: delivered.append(r.status.value))
while h.status(jid).value!="processing": time.sleep(0.01)
real_get=h._store.get
def get_then_stall(job_id):
    row=real_get(job_id)
    if threading.current_thread() is not threading.main_thread(): return row
    time.sleep(1.0)                      # ... and is preempted while the worker completes
    return row
h._store.get=get_then_stall
ok=h.cancel(jid)                          # runs on main thread
h._store.get=real_get
time.sleep(0.5)
row=h._store.get(jid)
events=[]
print("cancel() returned:",ok,"| final status:",row["status"],"| result.output:",(row.get("result") or {}).get("output"))
print("callbacks delivered:",delivered,"| output file status:",json.load(open(f"{d}/out/{jid}.json"))["status"])
h.close()
