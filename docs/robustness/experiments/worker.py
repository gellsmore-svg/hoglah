import sys, time, os
from hoglah import Hoglah
d=sys.argv[1]; os.environ["HOGLAH_WORKER_PID"]=str(os.getpid())
h=Hoglah(config={"db_path":f"{d}/q.db","lease_seconds":10,"heartbeat_interval_seconds":2,"log_level":"INFO",
                 "cli_binaries":{"claude":os.path.abspath("fakecli.py")},"cli_workdir":d}, use_real=True)
while True: time.sleep(1)
