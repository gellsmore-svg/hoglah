import tempfile, time, asyncio, sqlite3, logging
from hoglah import Hoglah
from hoglah.adapters import StubAdapter
runs=[]
class Long(StubAdapter):
    async def run(self, req, **kw):
        runs.append(time.monotonic()); await asyncio.sleep(9); return await super().run(req, **kw)
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db","lease_seconds":5,"heartbeat_interval_seconds":1,"log_level":"WARNING"}, adapter=Long())
real_hb=h._store.heartbeat; n={"c":0}
def flaky_hb(*a,**k):
    n["c"]+=1
    if n["c"]==1: raise sqlite3.OperationalError("database is locked")
    return real_hb(*a,**k)
h._store.heartbeat=flaky_hb
jid=h.submit(prompt="x", model="m", timeout_seconds=60)
r=h.wait(jid, timeout=40)
print("final:",r.status.value,"| executions:",len(runs),"| heartbeat calls:",n["c"], "| error field:",h._store.get(jid)["error"])
h.close()
