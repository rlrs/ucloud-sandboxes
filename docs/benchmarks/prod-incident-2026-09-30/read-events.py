import json,sqlite3
from pathlib import Path
root=Path('/var/lib/ucloud-sandboxes/state');out={}
with sqlite3.connect((root/'metrics.sqlite').as_uri()+'?mode=ro',uri=True,timeout=10) as db:
 db.execute('PRAGMA query_only=ON')
 out['retained']=db.execute('SELECT count(*),min(timestamp),max(timestamp) FROM metric_events').fetchone()
 out['kinds']=db.execute('SELECT kind,count(*),min(timestamp),max(timestamp) FROM metric_events WHERE timestamp>=? GROUP BY kind ORDER BY count(*) DESC',('2026-09-29T23:11:00',)).fetchall()
 for kind,n,*_ in out['kinds']:
  if kind!='autoscaler_tick':
   out.setdefault('recent_by_kind',{})[kind]=[{'timestamp':t,'data':json.loads(d)} for t,d in db.execute('SELECT timestamp,data_json FROM metric_events WHERE timestamp>=? AND kind=? ORDER BY sequence DESC LIMIT 500',('2026-09-29T23:11:00',kind))]
with sqlite3.connect((root/'control-state.sqlite').as_uri()+'?mode=ro',uri=True) as db:
 out['fleet']=[json.loads(r[0]) for r in db.execute("SELECT payload FROM control_records WHERE namespace='heartbeat'")]
with sqlite3.connect((root/'images.sqlite').as_uri()+'?mode=ro',uri=True) as db:
 out['image_builds']=[json.loads(r[0]) for r in db.execute('SELECT record_json FROM image_state_v1_builds LIMIT 1000')]
print(json.dumps(out))
