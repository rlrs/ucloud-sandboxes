import sqlite3,json
from pathlib import Path
p=Path('/var/lib/ucloud-sandboxes/state/metrics.sqlite');out={}
with sqlite3.connect(p.as_uri()+'?mode=ro',uri=True) as db:
 db.execute('PRAGMA query_only=ON')
 out['program_states']=db.execute("SELECT json_extract(data_json,'$.state'),json_extract(data_json,'$.last_error'),count(*),min(timestamp),max(timestamp) FROM metric_events WHERE kind='program_state_transition' AND timestamp>='2026-09-30T04:00:00' GROUP BY 1,2").fetchall()
 out['node_heartbeats']=[{'timestamp':t,'data':json.loads(d)} for t,d in db.execute("SELECT timestamp,data_json FROM metric_events WHERE kind='node_heartbeat' AND timestamp>='2026-09-30T04:00:00' ORDER BY sequence")]
 out['cycles']=[{'timestamp':t,'data':json.loads(d)} for t,d in db.execute("SELECT timestamp,data_json FROM metric_events WHERE kind='autoscaler_cycle' AND timestamp>='2026-09-30T04:25:00' AND timestamp<'2026-09-30T05:18:00' ORDER BY sequence")]
print(json.dumps(out))
