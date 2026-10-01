PY=~/domains/Hoglah/.venv/bin/python; D=$(mktemp -d); export FAKE_SLEEP=6
export HOGLAH_CLI_BINARIES="{\"claude\":\"$PWD/fakecli.py\"}" HOGLAH_CLI_WORKDIR=$D
JID=$($PY -c "
from hoglah import Hoglah; h=Hoglah(config={'db_path':'$D/q.db'}, start_worker=False)
print(h.submit(prompt='touch:$D/se.txt', model='fake', backend='claude')); h.close()")
~/domains/Hoglah/.venv/bin/hoglah run --db $D/q.db --real >$D/run.log 2>&1 & P=$!
for i in $(seq 80); do [ -s $D/se.txt ] && break; sleep 0.1; done
kill -TERM $P; sleep 1
echo "hoglah run alive after SIGTERM? $(kill -0 $P 2>/dev/null && echo yes || echo no) ; exit status: $(wait $P; echo $?)"
echo "'Shutting down' printed? $(grep -c 'Shutting down' $D/run.log)"
echo "CLI child alive? $(pgrep -f "fakecli.py.*touch:$D" >/dev/null && echo YES || echo no)"
$PY -c "import sqlite3;print('row:',sqlite3.connect('$D/q.db').execute(\"select status,lease_expires_at from jobs where id='$JID'\").fetchone())"
pkill -f "fakecli.py.*touch:$D"
