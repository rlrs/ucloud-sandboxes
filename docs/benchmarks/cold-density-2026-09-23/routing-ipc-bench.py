from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
import json,time,threading,sys
from ucloud_sandboxes.routing import RoutingStore,ExecRoute
from ucloud_sandboxes.routing_writer import RoutingWriteProcess
from old_routing_writer import RoutingWriteProcess as OldWriter

def bench(cls):
 with TemporaryDirectory() as tmp:
  store=RoutingStore(Path(tmp)/'routes.sqlite');writer=cls(store);stop=threading.Event()
  payload=json.dumps([{'id':str(i),'state':'running','spec':{'env':{'X':'x'*256}}}for i in range(128)])
  def load():
   while not stop.is_set():json.loads(payload);time.sleep(.001)
  noise=[threading.Thread(target=load)for _ in range(4)]
  for t in noise:t.start()
  def one(i):
   t=time.monotonic();writer.upsert_exec(ExecRoute(session_id=f'e{i}',sandbox_id='s',node_id='n',job_id='j',node_url='http://node'));return time.monotonic()-t
  started=time.monotonic()
  try:
   with ThreadPoolExecutor(max_workers=64)as pool:values=sorted(pool.map(one,range(1024)))
  finally:
   stop.set()
   for t in noise:t.join()
   writer.close()
  print(json.dumps({'writer':cls.__module__,'seconds':time.monotonic()-started,'p50':values[512],'p95':values[973],'max':values[-1]}),flush=True)
if __name__=='__main__':
 for cls in [OldWriter,RoutingWriteProcess,RoutingWriteProcess,OldWriter]:bench(cls)
