"""Export exact cached selectors; never broaden a prepared image to its project."""
import collections,csv,hashlib,importlib.util,json,re,sqlite3,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
REPO=Path('/home/alex-admin/ucloud-sandboxes')
sys.path[:0]=[str(REPO),str(REPO/'scripts')]
from ucloud_sandboxes.prepared_images import PreparedImageCatalog,SMITH_TAIL
from plan_image_pool import remaining_live_work
catalog=PreparedImageCatalog(ROOT/'prepared-images.sqlite3')
study=Path('/tmp/ucloud-cache-study-20260930')
research=Path('/tmp/ucloud-cache-study-research-https-20260930')
rows=[];excluded=collections.Counter();seen=set();provenance={}
availability=json.loads((ROOT/'availability.json').read_text())
def add(family,selector,recipe,*,task=None,dataset=None,revision=None,dataset_image=None,task_rows=1,scope='training',source_task_image=False,files=None):
 match=catalog.choose(recipe['dockerfile'],files or {})
 if not match:excluded[family+': no supported prepared match']+=1;return
 if not availability['references'].get(match['reference'],{}).get('ready'):excluded[family+': artifact unavailable']+=1;return
 key=(family,selector,task)
 if key in seen:return
 seen.add(key)
 row={'family':family,'scope':scope,'image':selector,'task_name':task,'dataset_image':dataset_image or selector,'dataset':dataset,'revision':revision,'upstream_rows':task_rows,
      'cached_kind':match['kind'],'prepared_reference':match['reference'],'foundation_key':match.get('key'),
      'remaining_build_work':remaining_live_work(match['dockerfile']),'task_source_cached':source_task_image,
      'recipe_sha256':hashlib.sha256(json.dumps(recipe,sort_keys=True).encode()).hexdigest(),
      'erofs_bytes':availability['references'][match['reference']]['erofs_bytes']}
 rows.append(row)
# Full task-source images: exact dataset references, with the explicit rebench mirror mapping.
inventory=json.loads((study/'swe-image-inventory.json').read_text())
family_names={'Multi-SWE':'MultiSWE','R2E-Gym':'R2E-Gym','SWE-Lego':'SWE-Lego','SWE-rebench':'SWE-rebench v2','Scale-SWE':'ScaleSWE','SWE-smith':'SWE-smith'}
for dataset,group in inventory.items():
 family=next(v for k,v in family_names.items() if k in dataset)
 provenance[dataset]=group['revision']
 for item in group['images']:
  selector=item['ref'];source=selector.replace('prime/primeintellect/','docker.io/swerebenchv2/',1) if family=='SWE-rebench v2' else selector
  recipe={'dockerfile':'FROM '+source+'\n'+(SMITH_TAIL if family=='SWE-smith' else '')}
  add(family,selector,recipe,dataset=dataset,revision=group['revision'],task_rows=item['tasks'],source_task_image=True)
# OpenSWE uses a platform image alias, distinct from its mixed-case dataset image_name.
with sqlite3.connect(REPO/'build/openswe-foundations-20260930/coverage/source.sqlite') as db:
 for source,encoded in db.execute('select source,recipe from images'):
  recipe=json.loads(encoded)
  if recipe.get('source_repository') or recipe.get('context_dir'):
   excluded['OpenSWE: context must be materialized to establish matcher bounds']+=1;continue
  lowered=source.lower();family,sep,alias=lowered.partition('--')
  selector=lowered if '/' in lowered else family+'/'+alias.replace('__','.')+':latest'
  add('OpenSWE',selector,recipe,dataset='GAIR/OpenSWE',revision='a8db93af5335df2c8baac0cd1ff367e4d475d3d7',dataset_image=source)
def context_files(context):
 files={};total=0;count=0
 for path in context.rglob('*'):
  count+=1
  if count>1000 or path.is_symlink():return None
  if not path.is_file():continue
  total+=path.stat().st_size
  if total>8*1024**2:return None
  name=path.relative_to(context).as_posix()
  if name in {'.dockerignore','Dockerfile.dockerignore'} and path.stat().st_size:return None
  with path.open('rb') as stream:
   if stream.read(100).startswith(b'version https://git-lfs.github.com/spec/v1'):return None
  if name in {'base_install.sh','post_install.sh'}:files[name]=path.read_bytes()
 return files
def image_from_task(path):
 return re.search(r'^docker_image\s*=\s*["\']([^"\']+)',path.read_text(),re.M)[1]
# TMax's task names can be used directly with task_ids_file.
tmax=Path('/tmp/ucloud-cache-study-prime-tasks-20260930/datasets/tmax')
for path in sorted(tmax.glob('*/environment/Dockerfile')):
 files=context_files(path.parent)
 if files is None:excluded['TMax: context too large, ignored, linked or unresolved LFS']+=1;continue
 selector=image_from_task(path.parent.parent/'task.toml')
 if selector=='prime/primeintellect/tmax:task_000000_c19dda5b':selector='prime/prime/tmax:task_000000_c19dda5b'
 add('TMax',selector,{'dockerfile':path.read_text()},task=path.parent.parent.name,files=files,dataset='tmax@2026-07-01')
# Terminal-Lego's actual pinned recipe adds a cross-image UV copy. The current
# resolver intentionally rejects that syntax. Do not infer eligibility from
# the simpler upstream Dockerfile and accidentally promise automatic reuse.
excluded['Terminal-Lego: actual indexed verifier recipe unsupported by automatic resolver']=13825
# Evaluation selectors are separate; never add them to the training starter.
evalroot=study/'eval-image-inputs';evalreceipt=json.loads((study/'eval-image-source-receipt.json').read_text())
evalrev={r['family']:r['revision'] for r in evalreceipt['files'] if r.get('revision')}
for folder,family in [('swebench-verified','SWE-bench Verified'),('swebench_multilingual','SWE-bench Multilingual'),('terminal-bench','Terminal-Bench 2'),('openthoughts-tblite','OpenThoughts TBLite')]:
 for path in sorted((evalroot/folder).glob('*/environment/Dockerfile')):
  text=path.read_text();m=re.search(r'^\s*FROM\s+(\S+)',text,re.M)
  if not m:continue
  selector=image_from_task(path.parent.parent/'task.toml') if folder=='terminal-bench' else m[1]
  recipe={'dockerfile':'FROM '+selector+'\n'} if folder=='terminal-bench' else {'dockerfile':text}
  add(family,selector,recipe,task=path.parent.parent.name,revision=evalrev.get(folder),scope='evaluation',source_task_image=folder!='openthoughts-tblite')
for path in sorted(Path('/tmp/ucloud-cache-study-senior-20260930/tasks').glob('*/environment/Dockerfile')):
 text=path.read_text();m=re.search(r'^\s*FROM\s+(\S+)',text,re.M)
 if m:add('Senior SWE-Bench',m[1],{'dockerfile':text},task=path.parent.parent.name,revision='e30b0e19fdbc4b099e752c6d5324f5b250aee3dc',scope='evaluation')
rows.sort(key=lambda r:(r['family'],r['image']))
out=ROOT/'list';out.mkdir(exist_ok=False);by=out/'by-environment';by.mkdir()
summary={}
for family in sorted({r['family'] for r in rows}):
 selected=[r for r in rows if r['family']==family];slug=re.sub(r'[^a-z0-9]+','-',family.lower()).strip('-')
 (by/(slug+'.images.txt')).write_text(''.join(image+'\n' for image in sorted({r['image'] for r in selected})))
 (by/(slug+'.dataset-images.json')).write_text(json.dumps(sorted({r['dataset_image'] for r in selected}),indent=2)+'\n')
 tasks=sorted({r['task_name'] for r in selected if r['task_name']})
 if tasks:(by/(slug+'.task-ids.json')).write_text(json.dumps(tasks,indent=2)+'\n')
 summary[family]={'records':len(selected),'unique_images':len({r['image'] for r in selected}),'cached_kind':dict(collections.Counter(r['cached_kind'] for r in selected)), 'remaining_run_steps':sum('run_requires_qualification' in r['remaining_build_work'] for r in selected)}
# Deterministic, balanced source-image choices for a 512+ rollout launch. These
# are image selectors, not a fabricated training dataset or changed split.
queues={family:sorted([r for r in rows if r['family']==family and r['scope']=='training' and r['task_source_cached']],key=lambda r:(r['erofs_bytes'],r['image'])) for family in summary}
starter=[]
while len(starter)<512:
 progressed=False
 for family in sorted(queues):
  if queues[family] and len(starter)<512:starter.append(queues[family].pop(0));progressed=True
 if not progressed:break
assert len(starter)==512 and len({(r['family'],r['image']) for r in starter})==512
starter_components={p['digest']:p['bytes'] for r in starter for p in availability['references'][r['prepared_reference']]['components']}
payload={'schema':1,'observed_at_utc':availability['observed_at_utc'],'scope':'Exact image selectors; preserve your existing train/heldout split. Base coverage permits live task setup and is not a latency certificate.', 'environment_revision':'c7ea0d7fe3379f0e6ffb0a68c82c80c184601f92','verifiers_revision':'7856cc1','datasets':provenance,'summary':summary,'exclusions':dict(excluded),'unverified':['BIRD SQL','BIRD dev SQL','NeMo Calendar','NeMo Instruction','NeMo Pivot','NeMo Workplace','BrowseComp-Plus BM25 service'],'images':rows}
(out/'cached-images.json').write_text(json.dumps(payload,indent=2)+'\n')
(out/'cached-512.json').write_text(json.dumps({'schema':1,'observed_at_utc':availability['observed_at_utc'],'selection':'512 balanced exact cached task-source image choices, smallest EROFS images first within each family; apply only within the existing training split','counts':dict(collections.Counter(r['family'] for r in starter)),'unique_erofs_bytes':sum(starter_components.values()),'images':starter},indent=2)+'\n')
with (out/'cached-images.csv').open('w') as stream:
 fields=['family','scope','image','task_name','dataset_image','cached_kind','prepared_reference','foundation_key','erofs_bytes','remaining_build_work']
 writer=csv.DictWriter(stream,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows({**r,'remaining_build_work':';'.join(r['remaining_build_work'])} for r in rows)
(out/'summary.json').write_text(json.dumps({k:v for k,v in payload.items() if k!='images'},indent=2)+'\n')
print(json.dumps({'summary':summary,'starter':dict(collections.Counter(r['family'] for r in starter)),'exclusions':dict(excluded),'total':len(rows)},indent=2))
