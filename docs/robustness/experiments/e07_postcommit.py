import tempfile, time, json, sqlite3, threading
from hoglah import Hoglah
d=tempfile.mkdtemp(); cbs=[]
h=Hoglah(config={"db_path":f"{d}/q.db","output_dir":f"{d}/out","log_level":"CRITICAL"}, callbacks={"k":lambda r: cbs.append(r.status.value)})
real_set=h._store.set_result; real_get=h._store.get; armed={"v":False}
def set_result(*a,**k):
    ok=real_set(*a,**k)
    if ok and k.get("lease_token"): armed["v"]=True
    return ok
def get(job_id):
    if armed["v"] and threading.current_thread() is not threading.main_thread():
        armed["v"]=False; raise sqlite3.OperationalError("database is locked")
    return real_get(job_id)
h._store.set_result=set_result; h._store.get=get
jid=h.submit(prompt="x", model="m", callback="k")
time.sleep(2)
print("store status:",real_get(jid)["status"],"| output file:",json.load(open(f"{d}/out/{jid}.json"))["status"], "| named callback fired:",cbs)
h.close()
