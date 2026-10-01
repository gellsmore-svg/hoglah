import tempfile, time, sqlite3, logging
from hoglah import Hoglah
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL"}, start_worker=False)
good=[h.submit(prompt=str(i),model="m") for i in range(3)]
bad=h.submit(prompt="bad",model="m",priority=5)
h.close()
c=sqlite3.connect(f"{d}/q.db"); c.execute("update jobs set request_json='{truncated' where id=?",(bad,)); c.commit(); c.close()
errs=[]
class H(logging.Handler):
    def emit(self,r): errs.append(r.getMessage())
logging.getLogger("hoglah").addHandler(H())
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"ERROR"})
time.sleep(5)
print("healthy jobs:",[h.status(j).value for j in good],"| 'Error in worker loop' logged",sum("worker loop" in e for e in errs),"times in 5s")
try: print("metrics/stats ok:", h.stats()["queued"])
except Exception as e: print("stats failed:",type(e).__name__)
h.close()
