import json,tempfile,time,threading,statistics,sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from ucloud_sandboxes.routing import RoutingStore,SandboxRoute
from tests.test_control_plane import _gateway_server
from ucloud_sandboxes.models import ResourceQuantity,SandboxInventoryEntry,utc_now

with tempfile.TemporaryDirectory() as tmp:
 server=_gateway_server(Path(tmp), routing_file=Path(tmp)/'routing.sqlite'); cls=server.RequestHandlerClass; s=cls.routing_store; routes=[]
 for i in range(128):
  r=SandboxRoute(sandbox_id=f's{i}',node_id=f'n{i//64}',job_id=f'j{i//64}',node_url=f'http://n{i//64}',resources=ResourceQuantity(vcpu=1,memory_mb=1024),spec={'id':f's{i}','image':'python','metadata':'x'*16384},state='running',generation=1,create_operation_id='create-'+str(i),spec_hash='a'*64,node_epoch='boot',created_at=utc_now().isoformat())
  s.upsert_sandbox(r);routes.append(r)
 if '--serialized' in sys.argv:
  original=s.sandbox_routes_readonly; guard=threading.Lock()
  def listing():
   with guard:return original()
  s.sandbox_routes_readonly=listing
 lat={k:[] for k in ['list','write','heartbeat']};barrier=threading.Barrier(58)
 def run(n):
  barrier.wait()
  kind='list' if n<24 else 'write' if n<56 else 'heartbeat'
  for j in range(12 if kind!='heartbeat' else 6):
   t=time.monotonic()
   if kind=='list':
    handler=object.__new__(cls);handler._write_bytes=lambda *a,**kw: None;handler._list_sandboxes_from_cache()
   elif kind=='write':s.upsert_program_request_transition_with_change(routes[n],request_id=f'r{n}-{j}',rollout_id='r',state='ready_to_wake')
   else:
    rs=routes[(n-56)*64:(n-55)*64]
    obs=[SandboxInventoryEntry(r.sandbox_id,r.generation,r.create_operation_id,r.spec_hash,'running',r.resources) for r in rs]
    s.reconcile_sandboxes_for_node(rs[0].node_url,obs,node_id=rs[0].node_id,job_id=rs[0].job_id,observed_at=utc_now().isoformat(),node_epoch='boot',activity_epoch=1,reported_sandbox_ids=[r.sandbox_id for r in rs])
   lat[kind].append(time.monotonic()-t)
 t=time.monotonic()
 with ThreadPoolExecutor(max_workers=58) as pool:list(pool.map(run,range(58)))
 print(json.dumps({'source':__import__('ucloud_sandboxes.control_plane',fromlist=['x']).__file__,'python':sys.version,'elapsed':time.monotonic()-t,'latency':{k:{'count':len(a),'p50':statistics.median(a),'p95':sorted(a)[int(len(a)*.95)],'max':max(a)} for k,a in lat.items()},'batch':s._write_batches.metrics()}),flush=True)

 server.server_close()
