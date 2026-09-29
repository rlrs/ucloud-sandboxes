import asyncio,hashlib,json,os,pathlib,statistics,subprocess,sys,tempfile,time,zipfile

async def arm(context):
 from ucloud_sandboxes_sdk import AsyncSandboxClient,Image,__version__
 from ucloud_sandboxes_sdk.client import SandboxApiError
 import ucloud_sandboxes_sdk.client as module
 class Client(AsyncSandboxClient):
  def __init__(self):super().__init__('http://unused.invalid');self.requests=0
  async def _request_json(self,method,path,**kwargs):
   self.requests+=1
   await asyncio.sleep(0)
   if method=='GET':raise SandboxApiError('fixture absent',status_code=404)
   if method=='PUT':return {'stored':True}
   return {'build':{'build_id':'owned-no-network','status':'running'}}
 gaps=[];done=False
 async def ticker():
  previous=time.perf_counter()
  while not done:
   await asyncio.sleep(.005);now=time.perf_counter();gaps.append(now-previous);previous=now
 tick=asyncio.create_task(ticker());await asyncio.sleep(.01)
 client=Client();start=time.perf_counter();cpu=time.process_time()
 await asyncio.gather(*(client.submit_image_build(Image.from_dockerfile(name=f'proof-{i}',context_path=context)) for i in range(4)))
 seconds=time.perf_counter()-start;cpu_seconds=time.process_time()-cpu;done=True;await tick
 return {'seconds':seconds,'cpu_seconds':cpu_seconds,'max_heartbeat_gap_seconds':max(gaps),'requests':client.requests,'source_sha256':hashlib.sha256(pathlib.Path(module.__file__).read_bytes()).hexdigest(),'version':__version__}

if len(sys.argv)>1:
 sys.path.insert(0,sys.argv[1]);print(json.dumps(asyncio.run(arm(pathlib.Path(sys.argv[2])))));raise SystemExit

baseline=pathlib.Path('/tmp/ucloud-sdk-archive-baseline-0433')
with zipfile.ZipFile('/tmp/ucloud-sdk-status-published-20260928/ucloud_sandboxes_sdk-0.4.33-py3-none-any.whl') as wheel:
 for name in wheel.namelist():
  if name.startswith('ucloud_sandboxes_sdk/') and not name.endswith('/'):
   path=baseline/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(wheel.read(name))
with tempfile.TemporaryDirectory(prefix='sdk-archive-abba-') as scratch:
 context=pathlib.Path(scratch);(context/'Dockerfile').write_text('FROM scratch\nCOPY payload.bin /payload.bin\n')
 data=os.urandom(16*1024*1024);(context/'payload.bin').write_bytes(data)
 rows=[]
 for label,path in [('A',baseline),('B',pathlib.Path('/tmp/ucloud-sdk-status-release-20260928/src')),('B',pathlib.Path('/tmp/ucloud-sdk-status-release-20260928/src')),('A',baseline)]:
  row=json.loads(subprocess.check_output([sys.executable,__file__,str(path),str(context)],text=True));row['arm']=label;rows.append(row)
 result={'order':'ABBA','fixture_bytes':len(data),'fixture_sha256':hashlib.sha256(data).hexdigest(),'submissions_per_arm':4,'heartbeat_interval_seconds':.005,'network_calls':0,'production_calls':0,'arms':rows,'limits':['No-network local packaging microbenchmark, not full image build latency.','Same immutable16MiB incompressible fixture used for every arm.']}
 path=pathlib.Path('/tmp/ucloud-sdk-archive-abba-20260929.json');path.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
