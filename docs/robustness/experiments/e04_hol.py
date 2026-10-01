import tempfile, time, os
from hoglah import Hoglah
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db"}, start_worker=False)
parent_id="parent-0000"
kids=[h.submit(prompt=f"c{i}",model="m",priority=10,depends_on=[parent_id]) for i in range(int(os.environ.get("N","60")))]
# parent submitted LAST with lower priority; dependency target id pre-chosen
h.submit(prompt="p",model="m",priority=0,job_id=parent_id)
h.close()
h=Hoglah(config={"db_path":f"{d}/q.db"})   # start the worker
time.sleep(8)
print("parent:",h.status(parent_id).value, "| children:",sorted({h.status(k).value for k in kids}), "| stats:",h.stats()["counts"])
h.close()
