import json, tempfile, time
from hoglah.kafka_bridge import MessageBridge, Message
from hoglah.store import SQLiteJobStore
from hoglah.config import HoglahSettings
class KafkaLike:
    """One partition. Kafka semantics: commit(msg) sets committed offset = msg.offset+1;
    poll() returns the next record after the *fetch position* (no redelivery of a
    skipped record within a session); a new session starts at the committed offset."""
    def __init__(self, log, committed=0): self.log=log; self.committed=committed; self.pos=committed
    def poll(self, timeout):
        if self.pos>=len(self.log): time.sleep(0.05); return None
        i=self.pos; self.pos+=1; cid,body=self.log[i]
        return Message(source="in",partition=0,offset=i,key=cid,value=body,raw=i)
    def ack(self, m): self.committed=m.raw+1
    def nack(self, m, reason): self.committed=m.raw+1
    def produce_and_flush(self,*a,**k): pass
    def close(self): pass
log=[(f"c{i}", json.dumps({"prompt":f"p{i}","model":"m","correlation_id":f"c{i}"}).encode()) for i in range(3)]
d=tempfile.mkdtemp(); store=SQLiteJobStore(f"{d}/q.db"); real=store.enqueue; n={"c":0}
def flaky(req, **kw):
    n["c"]+=1
    if n["c"]==2: raise RuntimeError("database is locked")     # message #1 hits a transient store error
    return real(req, **kw)
store.enqueue=flaky
cfg=HoglahSettings(db_path=f"{d}/q.db")
t=KafkaLike(log); b=MessageBridge(store=store, config=cfg, transport=t); b.start(skip_republish=True)
time.sleep(2.0); b._running=False; time.sleep(0.2)
print("session 1: committed offset =", t.committed, "| jobs in store:", sorted(r["correlation_id"] for r in store.list(limit=None)))
t2=KafkaLike(log, committed=t.committed); b2=MessageBridge(store=store, config=cfg, transport=t2); b2.start(skip_republish=True)
time.sleep(1.0); b2._running=False
print("after restart from committed offset: jobs in store:", sorted(r["correlation_id"] for r in store.list(limit=None)), "-> c1 lost:", "c1" not in [r["correlation_id"] for r in store.list(limit=None)])
