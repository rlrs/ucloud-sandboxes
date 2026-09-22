import importlib.util,sys,tempfile,sqlite3,time,threading,json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
for label,source in [('before','/tmp/durable_batch_before.py'),('after','/src/ucloud_sandboxes/durable_batch.py')]:
 spec=importlib.util.spec_from_file_location('batch_'+label,source);m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;spec.loader.exec_module(m)
 with tempfile.TemporaryDirectory() as root:
  path=Path(root)/'test.sqlite'
  def connect():
   c=sqlite3.connect(path,isolation_level=None,check_same_thread=False);c.execute('PRAGMA journal_mode=WAL');c.execute('PRAGMA synchronous=FULL');return c
  c=connect();c.execute('CREATE TABLE t (id integer primary key)');c.close()
  batch=m.DurableSqliteBatch(connect,lambda:None)
  gate=threading.Barrier(128)
  def write(i):
   gate.wait()
   for j in range(16):
    with batch.transaction() as c:c.execute('INSERT INTO t VALUES (?)',(i*16+j,))
  wall=time.monotonic();cpu=time.process_time()
  with ThreadPoolExecutor(128) as pool:list(pool.map(write,range(128)))
  elapsed=time.monotonic()-wall;cpu=time.process_time()-cpu
  c=connect();assert c.execute('select count(*) from t').fetchone()[0]==2048;c.close()
  print(json.dumps({'version':label,'operations':2048,'wall_seconds':elapsed,'cpu_seconds':cpu,'metrics':batch.metrics()}),flush=True)
  time.sleep(1.2)
