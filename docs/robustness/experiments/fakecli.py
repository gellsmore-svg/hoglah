#!/usr/bin/env python3
import sys, os, time, signal, subprocess
prompt = sys.argv[-1]
out = sys.stdout.buffer
if prompt == "utf8":
    data = ("שלום עולם — " * 200).encode()          # Hebrew: 2-byte chars + 3-byte em dash
    for i in range(0, len(data), 7):                  # 7-byte writes force mid-character splits
        out.write(data[i:i+7]); out.flush(); time.sleep(0.0005)
elif prompt == "empty":
    pass
elif prompt == "stderr_only":
    sys.stderr.write("the real answer is 42\n")
elif prompt == "partial_crash":
    out.write(b"partial answer that will never fin"); out.flush(); os.kill(os.getpid(), signal.SIGSEGV)
elif prompt == "orphan":
    # child that ignores SIGTERM and outlives us, holding the workdir
    subprocess.Popen([sys.executable, "-c", "import signal,time,os;signal.signal(signal.SIGTERM,signal.SIG_IGN);open('orphan.pid','w').write(str(os.getpid()));time.sleep(120)"])
    time.sleep(120)
elif prompt.startswith("poison:"):
    open(prompt.split(":",1)[1],"a").write("attempt\n"); os.kill(int(os.environ["HOGLAH_WORKER_PID"]), signal.SIGKILL); time.sleep(5)
elif prompt.startswith("touch:"):
    open(prompt.split(":",1)[1],"a").write("side effect at %f\n" % time.time()); time.sleep(float(os.environ.get("FAKE_SLEEP","0"))); out.write(b"done")
else:
    out.write(("echo:" + " ".join(sys.argv[1:])).encode())
