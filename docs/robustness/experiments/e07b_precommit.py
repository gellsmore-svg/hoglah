import tempfile, time, sqlite3, threading, asyncio
from hoglah import Hoglah
from hoglah.adapters import StubAdapter
runs=[]
class Count(StubAdapter):
    async def run(self, req, **kw): runs.append(1); return await super().run(req, **kw)
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL"}, adapter=Count())
real=h._store.get; state={"armed":False}
orig_run=Count.run
async def run(self, req, **kw):
    out=await orig_run(self, req, **kw); state["armed"]=True; return out   # provider has SUCCEEDED
Count.run=run
def get(job_id):
    if state["armed"] and threading.current_thread() is not threading.main_thread():
        state["armed"]=False; raise sqlite3.OperationalError("database is locked")   # the race-guard read
    return real(job_id)
h._store.get=get
r=h.wait(h.submit(prompt="x",model="m"),timeout=20)
print("provider calls:",len(runs),"| final status:",r.status.value,"| error:",r.error)
h.close()
