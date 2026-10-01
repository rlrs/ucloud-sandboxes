"""Read-only provider and gateway audit for the bounded qualification window."""
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import shlex
import sqlite3
from urllib.error import HTTPError
from urllib.request import Request, HTTPRedirectHandler, build_opener
from ucloud_sandboxes.config import DeploymentConfig

ROOT=Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
config=DeploymentConfig.from_dict(json.loads(Path('/etc/ucloud-sandboxes/deployment.json').read_text()))
start=datetime.fromisoformat(json.loads((ROOT/'hold'/'hold.jsonl').read_text().splitlines()[0])['at'])
finish=datetime.fromisoformat(json.loads((ROOT/'slotq-pulls-a2-cold'/'summary.json').read_text())['finished_at'])+timedelta(seconds=60)
with sqlite3.connect(config.metrics_path().resolve().as_uri()+'?mode=ro',uri=True) as db:
    rows=db.execute('SELECT timestamp,data_json FROM metric_events WHERE kind=? AND timestamp_epoch>=? AND timestamp_epoch<=? ORDER BY timestamp_epoch',('vm_submitted',start.timestamp(),finish.timestamp())).fetchall()
submissions=[]
for at,payload in rows:
    data=json.loads(payload); identity=str(data['job_id']);assert identity.isdigit()
    submissions.append({'id':identity,'submitted_at':at,'role':data['role']})
ids=sorted({v['id'] for v in submissions})
assert {'168016286','168016287','168016314','168016315'}<=set(ids) and 4<=len(ids)<=20
smoke_path=ROOT/'image-smokes'/'summary.json'
smokes=json.loads(smoke_path.read_text())
assert smokes['passed'] is True and len(smokes['results'])==3
assert all(case['verified'] and case['deleted'] and not case.get('cleanup_error') for case in smokes['results'])
candidate=json.loads((ROOT/'slotq-pulls-b-cold'/'summary.json').read_text())
owned={record['image_id']:record for record in candidate['records']}
assert all(case['image_id'] in owned and case['recipe']==owned[case['image_id']]['recipe'] for case in smokes['results'])
assert len({case['image_id'] for case in smokes['results']})==3
smoke_workers=sorted({case['worker']['job_id'] for case in smokes['results']})
assert smoke_workers and all(v.isdigit() for v in smoke_workers)
ids=sorted(set(ids)|set(smoke_workers))
assert 4<=len(ids)<=24
class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None
opener=build_opener(NoRedirect)
key=config.provider.settings.get('api_token_env','HETZNER_API_KEY')
assert config.provider.kind=='hetzner'
values={}
for line in Path('/etc/ucloud-sandboxes/hetzner.env').read_text().splitlines():
    parts=shlex.split(line,comments=True)
    if parts and parts[0]=='export':parts=parts[1:]
    if len(parts)==1 and '=' in parts[0]:
        name,value=parts[0].split('=',1);values[name]=value
assert values.get(key)
provider=[]
for identity in ids:
    req=Request('https://api.hetzner.cloud/v1/servers/'+identity,headers={'Authorization':'Bearer '+values[key]})
    try:
        with opener.open(req,timeout=10) as response: status=response.status
    except HTTPError as error:
        status=error.code;error.close()
    provider.append({'id':identity,'http_status':status,'absent':status==404})
api_token=config.sandbox_api_token_file().read_text().strip()
def get(path):
    req=Request('http://127.0.0.1:'+str(config.gateway_port)+path,headers={'Authorization':'Bearer '+api_token})
    with opener.open(req,timeout=10) as response:body=response.read(16*1024**2+1)
    assert len(body)<=16*1024**2
    return json.loads(body)
sandboxes=get('/v1/sandboxes?view=status')['sandboxes']
builds=get('/v1/images/builds')['builds']
prepared=get('/v1/builders/prepare')
with sqlite3.connect(config.control_state_file().resolve().as_uri()+'?mode=ro',uri=True) as db:
    fleet=[json.loads(v[0]) for v in db.execute('SELECT payload FROM control_records WHERE namespace=?',('heartbeat',))]
result={'provider':'hetzner','mutations':0,'all_absent':all(v['absent'] for v in provider),
        'verified_at':datetime.now(timezone.utc).isoformat(),'nodes':provider,'qualification_submissions':submissions,'smoke_worker_ids':smoke_workers,'smokes_finished_at':smokes['finished_at'],
        'window_start':start.isoformat(),'window_end':finish.isoformat(),
        'live':{'fleet_count':len(fleet),'fleet_ids':[v['job_id'] for v in fleet],
                'sandbox_count':len(sandboxes),'active_builds':sum(v['status'] not in {'succeeded','failed'} for v in builds),
                'prepared_builder_count':prepared['demand']['prepared_builder_count'],
                'prepared_sandbox_count':len(prepared['demand']['prepared']),
                'pending_count':prepared['demand']['pending_count'],'pending_image_builds':prepared['demand']['pending_image_builds']}}
print(json.dumps(result,indent=2))
