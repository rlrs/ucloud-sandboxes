from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json,time,threading,os
from ucloud_sandboxes.routing import RoutingStore,ExecRoute,SandboxRoute
from ucloud_sandboxes.models import ResourceQuantity,SandboxInventoryEntry,utc_now
from ucloud_sandboxes.routing_writer import RoutingWriteProcess
from old_routing_writer import RoutingWriteProcess as OldWriter

def bench(cls):
 with TemporaryDirectory() as tmp:
  store=RoutingStore(Path(tmp)/'routes.sqlite');routes=[];observations=[[]for _ in range(6)]
  for i in range(512):
   node=i%6;r=SandboxRoute(sandbox_id=f's{i}',node_id=f'n{node}',job_id=f'j{node}',node_url=f'http://node{node}',resources=ResourceQuantity(memory_mb=1024),spec={'id':f's{i}','env':{'CONFIG':'x'*8192}},state='running',generation=1,create_operation_id='create',spec_hash='a'*64,node_epoch='boot',activity_epoch=1)
   store.upsert_sandbox(r);routes.append(r);observations[node].append(SandboxInventoryEntry(sandbox_id=r.sandbox_id,generation=1,operation_id='create',spec_hash='a'*64,state='running',resources=r.resources))
  writer=cls(store);stop=threading.Event();failures=[];hbs=[]
  payload=json.dumps([{'id':str(i),'state':'running','spec':{'env':{'X':'x'*256}}}for i in range(128)])
  def noise():
   while not stop.is_set():json.loads(payload);time.sleep(.001)
  def heartbeat(n):
   try:
    while not stop.is_set():
     started=time.monotonic();writer.reconcile_sandboxes_for_node(f'http://node{n}',observations[n],node_id=f'n{n}',job_id=f'j{n}',reported_sandbox_ids=[r.sandbox_id for r in observations[n]],observed_at=utc_now().isoformat(),node_epoch='boot',activity_epoch=1)
     hbs.append(time.monotonic()-started);stop.wait(1)
   except BaseException as exc:failures.append(repr(exc))
  threads=[threading.Thread(target=noise)for _ in range(4)]+[threading.Thread(target=heartbeat,args=(n,))for n in range(6)]
  def one(i):
   r=routes[i%512];t=time.monotonic();writer.upsert_exec(ExecRoute(session_id=f'e{i}',sandbox_id=r.sandbox_id,node_id=r.node_id,job_id=r.job_id,node_url=r.node_url));return time.monotonic()-t
  started=time.monotonic()
  for t in threads:t.start()
  try:
   with ThreadPoolExecutor(max_workers=64)as pool:values=sorted(pool.map(one,range(2048)))
  finally:
   elapsed=time.monotonic()-started;stop.set()
   for t in threads:t.join()
   writer.close()
  assert not failures,failures
  print(json.dumps({'writer':cls.__module__,'seconds':elapsed,'p50':values[1024],'p95':values[1945],'max':values[-1],'heartbeats':len(hbs),'heartbeat_max':max(hbs),'local_metrics':store._write_batches.metrics()}),flush=True)
if __name__=='__main__':
 os.sched_setaffinity(0,sorted(os.sched_getaffinity(0))[:4])
 for cls in [OldWriter,RoutingWriteProcess,RoutingWriteProcess,OldWriter]:bench(cls)
