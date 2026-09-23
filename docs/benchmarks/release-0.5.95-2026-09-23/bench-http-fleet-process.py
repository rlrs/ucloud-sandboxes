import json,subprocess,sys,time,threading
from ucloud_sandboxes.http_server import HighBacklogThreadingHTTPServer,JsonHttpHandler
import tempfile
from pathlib import Path
from ucloud_sandboxes.routing import RoutingStore,SandboxRoute
from ucloud_sandboxes.models import ResourceQuantity
from ucloud_sandboxes.fleet_reader import FleetSnapshotReader
from ucloud_sandboxes.control_state import ControlStateStore
from ucloud_sandboxes.control_plane import _sandbox_list_bytes
from uuid import uuid4
def main():
 tmp=tempfile.TemporaryDirectory();store=RoutingStore(Path(tmp.name)/'routes.sqlite');control=ControlStateStore(Path(tmp.name)/'control.sqlite');reader=FleetSnapshotReader(control.path,store.path,120) if '--isolated' in sys.argv else None
 for i in range(256):store.upsert_sandbox(SandboxRoute(sandbox_id=str(i),node_id='n',job_id='j',node_url='http://n',resources=ResourceQuantity(),spec={'id':str(i),'content':'x'*1024},state='running',generation=1,create_operation_id='c'+str(i),spec_hash='a'*64))
 payload={'items':[{'id':str(i),'spec':'x'*1024} for i in range(256)]}
 class Handler(JsonHttpHandler):
  allow_http_keep_alive=False
  def do_GET(self):
   if self.path=='/fleet':self._write_bytes(reader.read() if reader else _sandbox_list_bytes(control,store,120), 'application/json')
   else:
    route=store.get_sandbox_readonly('0');store.upsert_program_request_transition_with_change(route, request_id=uuid4().hex,rollout_id='r',state='acting');self._write_json({'id':route.sandbox_id})
 s=HighBacklogThreadingHTTPServer(('127.0.0.1',0),Handler)
 t=threading.Thread(target=s.serve_forever,kwargs={'poll_interval':.01},daemon=True);t.start()
 client='''import json,sys,time,statistics
from http.client import HTTPConnection
from concurrent.futures import ThreadPoolExecutor
port=int(sys.argv[1])
def run(i):
 a=[]
 for _ in range(40):
  c=HTTPConnection('127.0.0.1',port,timeout=30);t=time.monotonic()
  try:
   c.request('GET','/fleet' if i<4 else '/healthz');r=c.getresponse();assert r.status==200;r.read();a.append(time.monotonic()-t)
  finally:c.close()
 return a
start=time.monotonic()
with ThreadPoolExecutor(max_workers=32) as ex:a=sorted(x for chunk in ex.map(run,range(32)) for x in chunk)
elapsed=time.monotonic()-start
print(json.dumps({'requests':len(a),'elapsed':elapsed,'rps':len(a)/elapsed,'p50':statistics.median(a),'p95':a[int(.95*len(a))],'max':max(a)}))
 '''
 try:
  print('ISOLATED',bool(reader),flush=True)
  print('SERVER',__import__('ucloud_sandboxes.http_server',fromlist=['x']).__file__,flush=True)
  subprocess.run([sys.executable,'-c',client,str(s.server_port)],check=True)
 finally:
  s.shutdown();s.server_close();t.join(2)
  if reader:reader.close()
if __name__=='__main__':main()
