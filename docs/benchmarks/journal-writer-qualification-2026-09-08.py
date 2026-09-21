from pathlib import Path
from contextlib import contextmanager,closing
from concurrent.futures import ThreadPoolExecutor
import tempfile,time,json,statistics
from ucloud_sandboxes.storage_native_daemon import StorageNativeJournal
@contextmanager
def old_connection(journal):
 with closing(journal._connect()) as connection:yield connection
out=[]
with tempfile.TemporaryDirectory(prefix='density-writer-',dir='/var/lib/gvisor-acl-build') as tmp:
 for repeat in range(3):
  for mode in ['sqlite_busy_handler','local_writer_guard']:
   j=StorageNativeJournal(Path(tmp)/(mode+str(repeat)+'.sqlite'))
   connect=j._write_connection if mode=='local_writer_guard' else lambda:old_connection(j)
   def write(_):
    start=time.perf_counter()
    with connect() as c:
     c.execute('BEGIN IMMEDIATE');c.execute('UPDATE counters SET next_value = next_value + 1');c.commit()
    return (time.perf_counter()-start)*1000
   start=time.perf_counter()
   with ThreadPoolExecutor(max_workers=16) as pool:times=list(pool.map(write,range(512)))
   duration=(time.perf_counter()-start)*1000
   with closing(j._connect()) as c:
    assert c.execute('SELECT next_value FROM counters').fetchone()[0]==200512
    assert c.execute('PRAGMA synchronous').fetchone()[0]==2
   out.append({'repeat':repeat,'mode':mode,'transactions':512,'workers':16,'wave_ms':duration,'p50_ms':statistics.median(times),'p95_ms':sorted(times)[int(.95*(len(times)-1))],'max_ms':max(times)})
print(json.dumps({'status':'passed','scope':'Temporary ext4 journal microbenchmark; full synchronous durability and all committed increments verified; not sandbox latency acceptance','results':out}))
