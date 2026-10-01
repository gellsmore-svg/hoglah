import tempfile, time, asyncio, subprocess, sys
from hoglah import Hoglah
from hoglah.adapters import StubAdapter
runs=[]
class Long(StubAdapter):
    async def run(self, req, **kw):
        runs.append(time.time()); await asyncio.sleep(5); return await super().run(req, **kw)
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL"}, adapter=Long())   # default 30s lease, 10s heartbeat
jid=h.submit(prompt="x",model="m")
while not runs: time.sleep(0.05)
peer = f"""
import datetime, hoglah.store as s
real=s.datetime
class Skewed(real):
    @classmethod
    def now(cls, tz=None): return real.now(tz) + datetime.timedelta(seconds=60)   # this host's clock is 60s fast
s.datetime=Skewed
st=s.SQLiteJobStore('{d}/q.db'); print('peer reclaimed:', st.reclaim_stale_leases()); st.close()
"""
print(subprocess.run([sys.executable,"-c",peer],capture_output=True,text=True).stdout.strip())
r=h.wait(jid,timeout=60); print("final:",r.status.value,"| executions:",len(runs)); h.close()
