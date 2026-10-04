import concurrent.futures,hashlib,json
from pathlib import Path
from urllib.request import urlopen
root=Path('/tmp/ucloud-cache-study-20260930');registry=json.loads((root/'harbor-registry.json').read_text());out=root/'eval-image-inputs';out.mkdir(exist_ok=True)
selected={'swebench-verified':'1.0','swebench_multilingual':'1.0','terminal-bench':'2.0','openthoughts-tblite':'2.0'}
jobs=[]
for dataset in registry:
 if selected.get(dataset['name'])!=dataset['version']:continue
 for task in dataset['tasks']:
  for file in (['environment/Dockerfile','task.toml'] if dataset['name'] in ['terminal-bench','openthoughts-tblite'] else ['environment/Dockerfile']):
   jobs.append((dataset['name'],task,file))
def get(job):
 family,task,file=job;repo=task['git_url'].removesuffix('.git').removeprefix('https://github.com/');url=f"https://raw.githubusercontent.com/{repo}/{task['git_commit_id']}/{task['path']}/{file}"
 destination=out/family/task['name']/file;destination.parent.mkdir(parents=True,exist_ok=True)
 try:
  if not destination.exists():
   with urlopen(url,timeout=60) as response:content=response.read()
   destination.write_bytes(content)
  return {'family':family,'task':task['name'],'file':file,'revision':task['git_commit_id'],'url':url,'sha256':hashlib.sha256(destination.read_bytes()).hexdigest()}
 except Exception as e:return {'family':family,'task':task['name'],'file':file,'error':str(e)}
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as workers:results=list(workers.map(get,jobs))
(root/'eval-image-source-receipt.json').write_text(json.dumps({'registry_sha256':hashlib.sha256((root/'harbor-registry.json').read_bytes()).hexdigest(),'files':results},indent=2))
print(json.dumps({'fetched':len(results),'errors':[x for x in results if 'error' in x][:10],'error_count':sum('error' in x for x in results)}))
