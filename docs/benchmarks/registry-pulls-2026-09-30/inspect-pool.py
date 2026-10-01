"""Stage read-only inspectors and bounded metrics only on four owned builders."""
import base64
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import time

ROOT = Path('/work/ucloud-sandboxes/registry-pull-qualification-20260930')
REMOTE = Path('/work/registry-pulls-inputs')
NODES = {'168016286':'10.42.0.3','168016287':'10.42.0.4','168016314':'10.42.0.5','168016315':'10.42.0.6'}
PINS = {'inspect_owned_builder.py':'46f98bbdf3ea82e7a64be1d36529e5a2a1dd8183a680d1ddf830f2fd742446fd',
'upgrade_owned_builder.py':'160559f723d08e83b88759f1e4f8b9d9b4675db319315a598cd22e22a898591c',
'baseline.whl':'405516727b99eed510a0bcfcee39203cb69f67a4b56ad7c68bc5d3eaa672e628',
'build_load_telemetry.py':'ed9a6b6fecdd01984f5b54a05ae0ba696c03f81d6b2ef481cc8f009ec00b9c5e'}

def ssh(node, args, source=None, timeout=45):
    cmd=['ssh','-o','BatchMode=yes','-o','ConnectTimeout=10','-o','StrictHostKeyChecking=yes',
         '-o','UserKnownHostsFile=/var/lib/ucloud-sandboxes/state/ssh-known-hosts/'+node,
         '-i','/var/lib/ucloud-sandboxes/state/ssh/gateway-init','root@'+NODES[node],shlex.join(args)]
    result=subprocess.run(cmd,input=source,text=True,capture_output=True,timeout=timeout)
    if result.returncode:
        raise RuntimeError('Owned node command failed')
    return result.stdout

def inspect(node):
    start=datetime.now(timezone.utc).isoformat()
    items={name:{'sha256':pin,'data':base64.b64encode((ROOT/name).read_bytes()).decode()} for name,pin in PINS.items()}
    script='import base64,hashlib,os\nfrom pathlib import Path\nos.umask(0o022)\nroot=Path('+repr(str(REMOTE))+')\nroot.mkdir(mode=0o755,exist_ok=True)\nitems='+repr(items)+'\nfor name,item in items.items():\n b=base64.b64decode(item["data"]);assert hashlib.sha256(b).hexdigest()==item["sha256"]\n p=root/name\n if p.exists():assert p.read_bytes()==b\n else:p.write_bytes(b);p.chmod(0o644)\n'
    for attempt in range(8):
        try:
            ssh(node,['python3','-'],script)
            break
        except Exception:
            if attempt==7:raise
            time.sleep(5)
    result=json.loads(ssh(node,['python3',str(REMOTE/'inspect_owned_builder.py'),'--expected-job-id',node,
                      '--wheel',str(REMOTE/'baseline.whl'),'--wheel-sha256',PINS['baseline.whl']]))
    assert result['complete'] and result['after']['job_id']==node and result['service_changed'] is False
    path=ROOT/'baseline-receipts'/(node+'.json')
    with path.open('x') as out:
        path.chmod(0o600);json.dump(result,out,indent=2);out.write('\n')
    ssh(node,['systemd-run','--unit=ucloud-registry-pulls-monitor','--collect','--quiet',
              '--property=RuntimeMaxSec=3650','--property=Nice=10','/usr/bin/python3',
              str(REMOTE/'build_load_telemetry.py'),'sample','--output',str(REMOTE/'host-telemetry.jsonl'),
              '--label','builder-'+node,'--duration','3600','--interval','2'])
    ssh(node,['systemctl','is-active','--quiet','ucloud-registry-pulls-monitor'])
    return dict(job_id=node,node_epoch=result['after']['node_epoch'],node_ip=NODES[node],
                installed_files_match_wheel=result['installed_files_match_wheel'],
                started_at=start,finished_at=datetime.now(timezone.utc).isoformat(),sampler_active=True,complete=True)

for name,pin in PINS.items():
    assert hashlib.sha256((ROOT/name).read_bytes()).hexdigest()==pin
(ROOT/'baseline-receipts').mkdir(mode=0o700)
subprocess.run(['systemd-run','--unit=ucloud-registry-pulls-monitor','--collect','--quiet',
    '--property=RuntimeMaxSec=3650','--property=Nice=10','/usr/bin/python3',str(ROOT/'build_load_telemetry.py'),
    'sample','--output',str(ROOT/'gateway-telemetry.jsonl'),'--label','gateway','--duration','3600','--interval','2',
    '--health-url','https://77.42.92.27/healthz'],check=True,timeout=15)
with ThreadPoolExecutor(4) as pool:
    rows=list(pool.map(inspect,NODES))
receipt={'complete':True,'nodes':rows,'baseline_wheel_sha256':PINS['baseline.whl']}
(ROOT/'baseline-pool-inspection.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt),flush=True)
