from pathlib import Path
import os,json,tempfile,time,statistics,subprocess,hashlib
from ucloud_sandboxes.direct_network import DirectNetworkManager
assert os.geteuid()==0 and os.readlink('/proc/self/ns/net')!=os.readlink('/proc/1/ns/net'), 'requires isolated network namespace'
with tempfile.TemporaryDirectory(prefix='density-network-',dir='/var/tmp') as tmp:
 m=DirectNetworkManager(Path(tmp)/'state.json',allowed_tcp_egress=('10.36.0.2:8092',))
 # Enumerate precisely the checks production reconciliation requires.
 checks=[];original=m._ensure_iptables
 m._ensure_iptables=lambda check,install,**kw:checks.append(tuple(check))
 m._ensure_host_rules();m._ensure_iptables=original
 m._ensure_host_rules()
 assert all(subprocess.run(c,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0 for c in checks)
 before=m._iptables_snapshot();matched=sum(m._iptables_rule_key(c) in before for c in checks)
 # Missing private-network denial must be restored on the next reconciliation.
 deny=next(c for c in checks if '10.0.0.0/8' in c)
 subprocess.run(tuple('-D' if x=='-C' else x for x in deny),check=True)
 assert subprocess.run(deny,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode!=0
 m._ensure_host_rules();assert subprocess.run(deny,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
 # An unrelated port cannot stand in for the configured egress permission.
 tcp=next(c for c in checks if '--dport' in c)
 subprocess.run(tuple('-D' if x=='-C' else x for x in tcp),check=True)
 wrong=tuple('-A' if x=='-C' else ('8093' if x=='8092' else x) for x in tcp)
 subprocess.run(wrong,check=True)
 assert m._iptables_rule_key(tcp) not in m._iptables_snapshot()
 m._ensure_host_rules();assert subprocess.run(tcp,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0
 samples={'original_checks':[],'snapshot_checks':[]};snapshot=m._iptables_snapshot
 for _ in range(25):
  for label in samples:
   m._iptables_snapshot=snapshot if label=='snapshot_checks' else lambda:None
   start=time.perf_counter();m._ensure_host_rules();samples[label].append((time.perf_counter()-start)*1000)
 print(json.dumps({'status':'passed','isolated_network_namespace':True,'required_checks':len(checks),'snapshot_exact_matches':matched,'missing_private_deny_repaired':True,'wrong_port_did_not_match':True,'timings_ms':{k:{'count':len(v),'p50':statistics.median(v),'p95':sorted(v)[int(.95*(len(v)-1))]} for k,v in samples.items()},'samples_ms':samples}))
