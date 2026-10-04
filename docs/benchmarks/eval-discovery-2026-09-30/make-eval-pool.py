import collections,json,re
from pathlib import Path
root=Path('/tmp/ucloud-cache-study-20260930');inputs=root/'eval-image-inputs';receipt=json.loads((root/'eval-image-source-receipt.json').read_text());revisions={x['family']:x['revision'] for x in receipt['files'] if 'revision' in x};images={};coverage={}
def add(source,family,level,revision):
 item=images.setdefault(source,{'source':source,'families':[],'task_rows':0,'uses':[]})
 if family not in item['families']:item['families'].append(family)
 item['task_rows']+=1
 use=next((u for u in item['uses'] if (u['family'],u['level'],u['revision'])==(family,level,revision)),None)
 if use is None:use={'family':family,'level':level,'revision':revision,'task_rows':0};item['uses'].append(use)
 use['task_rows']+=1
for directory,family in [('swebench-verified','SWE-bench Verified'),('swebench_multilingual','SWE-bench Multilingual'),('terminal-bench','Terminal-Bench 2'),('openthoughts-tblite','OpenThoughts TBLite')]:
 tasks=sorted((inputs/directory).iterdir());coverage[family]={'tasks':len(tasks),'revision':revisions[directory]}
 for task in tasks:
  docker=(task/'environment/Dockerfile').read_text();base=re.search(r'^\s*FROM\s+(\S+)',docker,re.M|re.I)[1]
  if directory=='terminal-bench':
   source=re.search(r'^docker_image\s*=\s*[\"\']([^\"\']+)',(task/'task.toml').read_text(),re.M)[1];level='upstream_task_image'
  elif directory=='openthoughts-tblite':source=base;level='base_only'
  else:source=base;level='upstream_task_image'
  add(source,family,level,revisions[directory])
for directory,family,revision in [('/tmp/ucloud-cache-study-terminal-20260930','Terminal-Lego','92e6b5f577610cec9b040250a94ce66cfce24839'),('/tmp/ucloud-cache-study-senior-20260930/tasks','Senior SWE-Bench','e30b0e19fdbc4b099e752c6d5324f5b250aee3dc')]:
 paths=sorted(Path(directory).glob('*/environment/Dockerfile'));coverage[family]={'tasks':len(paths),'revision':revision}
 for path in paths:
  m=re.search(r'^\s*FROM\s+(\S+)',path.read_text(),re.M|re.I)
  if m:add(m[1],family,'base_only',revision)
plan={'schema':1,'scope':'Pinned upstream pools; task subset and heldout membership unknown. Base-only uses are not task-ready.','coverage':coverage,'images':sorted(images.values(),key=lambda x:(-x['task_rows'],x['source']))}
dest=Path('build/eval-image-pool-20260930');dest.mkdir(exist_ok=True);(dest/'inventory.json').write_text(json.dumps(plan,indent=2)+'\n')
# Prioritize common compatible bases, then round-robin actual evaluation images.
selected=[];seen=set()
for x in plan['images']:
 if x['task_rows']>=10 and not x['source'].startswith(('ghcr.io/','--')) and '$' not in x['source']:
  selected.append(x);seen.add(x['source'])
for n in range(10):
 for family in ['SWE-bench Verified','SWE-bench Multilingual','Terminal-Bench 2']:
  candidates=[x for x in plan['images'] if family in x['families'] and x['source'] not in seen]
  if candidates:selected.append(candidates[0]);seen.add(candidates[0]['source'])
(dest/'plan.json').write_text(json.dumps({**plan,'images':selected},indent=2)+'\n')
print(json.dumps({'coverage':coverage,'unique_sources':len(images),'batch':len(selected),'batch_common_bases':[(x['source'],x['task_rows']) for x in selected if x['task_rows']>=10]}))
