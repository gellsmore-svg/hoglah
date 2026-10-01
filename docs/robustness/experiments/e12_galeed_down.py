import tempfile, time
from hoglah import Hoglah
d=tempfile.mkdtemp(); got=[]
h=Hoglah(config={"db_path":f"{d}/q.db","galeed_enabled":True,"galeed_mongo_uri":"mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=2000","log_level":"INFO"})
t=time.monotonic(); jid=h.submit(prompt="x", model="m", callback=lambda r: got.append(r.status.value)); dt=time.monotonic()-t
r=h.wait(jid, timeout=30); time.sleep(0.5)
print(f"submit() blocked {dt:.2f}s | job {r.status.value} | direct callback fired: {got}")
t=time.monotonic(); ids=[h.submit(prompt=str(i),model="m") for i in range(3)]; print(f"3 more submits took {time.monotonic()-t:.2f}s")
t=time.monotonic(); [h.wait(i,timeout=60) for i in ids]; print(f"3 stub jobs end-to-end {time.monotonic()-t:.2f}s (stub run = 0.03s each)")
h.close()
