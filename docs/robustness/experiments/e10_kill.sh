set -u; PY=~/domains/Hoglah/.venv/bin/python; D=$(mktemp -d); export FAKE_SLEEP=8
JID=$($PY -c "
from hoglah import Hoglah; h=Hoglah(config={'db_path':'$D/q.db'}, start_worker=False)
print(h.submit(prompt='touch:$D/side_effects.txt', model='fake', backend='claude', max_retries=0)); h.close()")
$PY worker.py $D >$D/w1.log 2>&1 & W1=$!
for i in $(seq 50); do [ -s $D/side_effects.txt ] && break; sleep 0.1; done
kill -9 $W1; T0=$(date +%s); sleep 0.5
echo "after SIGKILL of worker: fake CLI still alive? $(pgrep -f "fakecli.py.*touch:$D" >/dev/null && echo YES || echo no)"
$PY -c "
import sqlite3; r=sqlite3.connect('$D/q.db').execute(\"select status,lease_expires_at,error from jobs where id='$JID'\").fetchone(); print('row after kill:',r)"
$PY worker.py $D >$D/w2.log 2>&1 & W2=$!
for i in $(seq 300); do s=$($PY -c "import sqlite3;print(sqlite3.connect('$D/q.db').execute(\"select status from jobs where id='$JID'\").fetchone()[0])"); [ "$s" = completed ] && break; sleep 0.1; done
echo "recovered to '$s' after $(( $(date +%s)-T0 ))s; side effects recorded: $(wc -l < $D/side_effects.txt) (max_retries=0)"
grep -i "reclaim\|recover" $D/w2.log | head -3; kill $W2; sleep 0.3; pkill -f "fakecli.py.*touch:$D"
