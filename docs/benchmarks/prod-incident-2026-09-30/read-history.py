import json, sqlite3
from pathlib import Path
from datetime import datetime,timezone
root=Path('/var/lib/ucloud-sandboxes/state')
out={'at':datetime.now(timezone.utc).isoformat(),'schemas':{}}
for name in ('build-history.sqlite','metrics.sqlite','control-state.sqlite','autoscaler-state.sqlite','images.sqlite'):
 with sqlite3.connect((root/name).as_uri()+'?mode=ro',uri=True,timeout=10) as db:
  db.execute('PRAGMA query_only=ON')
  out['schemas'][name]=db.execute("SELECT name,sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").fetchall()
  if name=='build-history.sqlite':
   cutoff=datetime(2026,9,29,23,11,tzinfo=timezone.utc).timestamp()
   out['build_counts']=db.execute('SELECT status,count(*),min(finished_epoch),max(finished_epoch) FROM terminal_builds WHERE finished_epoch>=? GROUP BY status',(cutoff,)).fetchall()
   out['builds']=[json.loads(r[0]) for r in db.execute('SELECT summary_json FROM terminal_builds WHERE finished_epoch>=? ORDER BY finished_epoch DESC LIMIT 1000',(cutoff,))]
print(json.dumps(out))
