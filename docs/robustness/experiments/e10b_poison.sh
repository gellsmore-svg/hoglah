PY=~/domains/Hoglah/.venv/bin/python; D=$(mktemp -d)
JID=$($PY -c "
from hoglah import Hoglah; h=Hoglah(config={'db_path':'$D/q.db'}, start_worker=False)
print(h.submit(prompt='poison:$D/attempts.txt', model='fake', backend='claude', max_retries=0)); h.close()")
for k in 1 2 3 4; do timeout 25 $PY worker.py $D >>$D/w.log 2>&1; echo "worker run $k exit=$? attempts_so_far=$(wc -l < $D/attempts.txt 2>/dev/null || echo 0)"; done
$PY -c "import sqlite3;print('row:',sqlite3.connect('$D/q.db').execute(\"select status,error from jobs where id='$JID'\").fetchone())"
