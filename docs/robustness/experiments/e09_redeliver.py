import tempfile
from hoglah import Hoglah
d=tempfile.mkdtemp(); calls=[]
cb={"k":lambda r: calls.append(r.job_id)}
h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL"}, callbacks=cb)
old=[h.submit(prompt=str(i),model="m",callback="k") for i in range(205)]
for j in old: h.wait(j,timeout=60)
print("live deliveries:",len(calls)); h.close()
for n in (1,2):
    calls.clear(); h=Hoglah(config={"db_path":f"{d}/q.db","log_level":"CRITICAL"}, callbacks=cb, start_worker=False)
    print(f"restart {n} (start_worker=False): redelivered {len(calls)} callbacks; newest 5 jobs included? {sum(j in calls for j in old[-5:])}/5")
    h.close()
