set -e
sudo /work/ucloud-sandboxes/qualification/20260922/venv/bin/python - <<'PY'
from ucloud_sandboxes.routing import RoutingStore,SandboxRoute
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.sandbox import SandboxSpec,sandbox_spec_fingerprint
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tempfile import TemporaryDirectory
import time,json,threading
with TemporaryDirectory(prefix='routing-bench-',dir='/var/lib/ucloud-sandboxes') as tmp:
 store=RoutingStore(Path(tmp)/'routes.sqlite');routes=[]
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
 print(json.dumps({'seconds':time.monotonic()-start,'cpu':time.process_time()-cpu,'p95':sorted(latencies)[int(.95*len(latencies))],'metrics':store._write_batches.metrics()}))
 time.sleep(1.2)
PY
