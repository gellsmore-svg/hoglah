import tempfile, os, errno
from hoglah import Hoglah
from hoglah import streaming
d=tempfile.mkdtemp()
h=Hoglah(config={"db_path":f"{d}/q.db","stream_dir":f"{d}/s","log_level":"CRITICAL"})
# (a) disk full while writing chunks
orig=streaming.ChunkWriter.write_chunk
def enospc(self,text): raise OSError(errno.ENOSPC,"No space left on device")
streaming.ChunkWriter.write_chunk=enospc
r=h.wait(h.submit(prompt="hello",model="m"),timeout=30); print("(a) ENOSPC on stream chunk ->",r.status.value,"|",r.error)
streaming.ChunkWriter.write_chunk=orig
# (b) stream dir not writable at job start
os.chmod(f"{d}/s",0o500)
r=h.wait(h.submit(prompt="hello",model="m"),timeout=30); print("(b) stream dir read-only     ->",r.status.value,"|",r.error)
os.chmod(f"{d}/s",0o700); h.close()
