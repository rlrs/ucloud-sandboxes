from ucloud_sandboxes.routing import RoutingStore,SandboxRoute
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.sandbox import SandboxSpec,sandbox_spec_fingerprint
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
import time,json,threading
def run(label,cls):
 import ucloud_sandboxes.routing as routing
 routing.DurableSqliteBatch=cls
 with TemporaryDirectory(prefix='routing-bench-') as tmp:
  store=RoutingStore(Path(tmp)/'routes.sqlite');store._write_batches.delay=0.005;routes=[]
  for i in range(128):
   spec=SandboxSpec(id='bench-'+str(i),image='python')
   routes.append(store.upsert_sandbox(SandboxRoute(sandbox_id=spec.id,node_id='node',job_id='job',node_url='http://node',resources=ResourceQuantity(vcpu=1,memory_mb=1024,disk_mb=4096),spec=spec.to_dict(),generation=1,create_operation_id='00000000-0000-4000-8000-000000000001',spec_hash=sandbox_spec_fingerprint(spec),state='running')))
  gate=threading.Barrier(128);latencies=[]
  def run(i):
   gate.wait()
   for cycle in range(4):
    t=time.monotonic()
    for state in ['model_wait','ready_to_wake','waking','acting']:
     store.upsert_program_request_transition_with_change(routes[i],request_id=f'{i}-{cycle}',rollout_id=str(i),state=state)
    latencies.append(time.monotonic()-t)
  start=time.monotonic();cpu=time.process_time()
  with ThreadPoolExecutor(128) as pool:list(pool.map(run,range(128)))
  print(json.dumps({'version':label,'seconds':time.monotonic()-start,'cpu':time.process_time()-cpu,'p95':sorted(latencies)[int(.95*len(latencies))],'metrics':store._write_batches.metrics()}))
  time.sleep(1.2)
import importlib.util,sys
from ucloud_sandboxes.durable_batch import DurableSqliteBatch
spec=importlib.util.spec_from_file_location('durable_before',sys.argv[1]);old=importlib.util.module_from_spec(spec);sys.modules[spec.name]=old;spec.loader.exec_module(old)
for label,cls in [('before',old.DurableSqliteBatch),('after',DurableSqliteBatch),('after',DurableSqliteBatch),('before',old.DurableSqliteBatch)]:run(label,cls)
