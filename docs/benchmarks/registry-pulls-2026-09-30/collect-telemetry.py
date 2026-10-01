"""Stop only owned samplers after the final measured tail; retain validated gzip copies."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import shlex
import subprocess

ROOT=Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
NODES={'168016286':'10.42.0.3','168016287':'10.42.0.4','168016314':'10.42.0.5','168016315':'10.42.0.6'}
UNIT='ucloud-registry-pulls-monitor'
PACK='''import gzip,hashlib,json,subprocess\nfrom pathlib import Path\np=Path(PATH)\nsubprocess.run(['systemctl','stop','ucloud-registry-pulls-monitor'],check=True,timeout=20)\nbody=p.read_bytes();rows=[json.loads(v) for v in body.splitlines() if v]\nassert rows\nout=p.with_suffix(p.suffix+'.gz');assert not out.exists();out.write_bytes(gzip.compress(body,compresslevel=6,mtime=0))\nassert gzip.decompress(out.read_bytes())==body\nprint(json.dumps({'samples':len(rows),'sha256':hashlib.sha256(out.read_bytes()).hexdigest()}))\n'''

def ssh_args(node):
    return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','StrictHostKeyChecking=yes',
            '-o','UserKnownHostsFile=/var/lib/ucloud-sandboxes/state/ssh-known-hosts/'+node,
            '-i','/var/lib/ucloud-sandboxes/state/ssh/gateway-init']

def collect(node):
    args=ssh_args(node)
    source='/work/registry-pulls-inputs/host-telemetry.jsonl'
    result=subprocess.run(args+['root@'+NODES[node],shlex.join(['python3','-'])],
                          input=PACK.replace('PATH',repr(source)),text=True,capture_output=True,timeout=60)
    assert result.returncode==0, 'Owned sampler archive failed'
    value=json.loads(result.stdout)
    target=ROOT/'telemetry'/('builder-'+node+'.jsonl.gz')
    command=['scp',*args[1:],'root@'+NODES[node]+':'+source+'.gz',str(target)]
    subprocess.run(command,check=True,timeout=30,capture_output=True)
    body=target.read_bytes();assert hashlib.sha256(body).hexdigest()==value['sha256']
    assert len([json.loads(line) for line in gzip.decompress(body).splitlines() if line])==value['samples']
    return {'job_id':node,'sampler_stopped':True,**value}

summary=json.loads((ROOT/'slotq-pulls-a2-cold'/'summary.json').read_text())
assert summary['passed'] and summary['active_builds_after']==0 and summary['live_admission_drained']
assert (datetime.now(timezone.utc)-datetime.fromisoformat(summary['finished_at'])).total_seconds()>=60
(ROOT/'telemetry').mkdir(mode=0o700)
start=datetime.now(timezone.utc).isoformat()
result=subprocess.run(['python3','-'],input=PACK.replace('PATH',repr(str(ROOT/'gateway-telemetry.jsonl'))),
                      capture_output=True,text=True,timeout=60)
assert result.returncode==0,'Gateway sampler archive failed'
gateway=json.loads(result.stdout)
(ROOT/'gateway-telemetry.jsonl.gz').replace(ROOT/'telemetry'/'gateway.jsonl.gz')
with ThreadPoolExecutor(4) as pool: rows=list(pool.map(collect,NODES))
receipt={'complete':True,'started_at':start,'finished_at':datetime.now(timezone.utc).isoformat(),
         'sampler_unit':UNIT,'gateway':{'sampler_stopped':True,**gateway},'builders':rows}
(ROOT/'telemetry-collection.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
