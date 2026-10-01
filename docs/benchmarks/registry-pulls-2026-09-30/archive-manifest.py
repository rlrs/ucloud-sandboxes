from pathlib import Path
import hashlib,json
root=Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
paths={}
for phase in ('a','b','a2'):
 for name in ('summary.json','before.json','launch.json'):
  key='slotq-pulls-'+phase+'-cold/'+name;paths[key]=root/key
 paths['fixtures/cold-'+phase+'-cases.json']=root/('cold-'+phase+'-fixtures')/'cases.json'
for folder in ('baseline-receipts','upgrades','upgrades-a2','telemetry'):
 for path in (root/folder).iterdir():
  if path.is_file():paths[folder+'/'+path.name]=path
for name in ('baseline-pool-inspection.json','telemetry-collection.json'):paths[name]=root/name
print(json.dumps({'files':{name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in paths.items()}},indent=2))
