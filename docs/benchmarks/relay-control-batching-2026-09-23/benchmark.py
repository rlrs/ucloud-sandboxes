import asyncio,json,os,time
from uuid import uuid4
from psycopg import AsyncConnection,sql
from ucloud_sandboxes.shared_control.database import PostgresDatabase
from ucloud_sandboxes.shared_control.relay import PostgresRelayState

async def main():
 schema='ucloud_shared_bench_'+uuid4().hex
 store=PostgresDatabase(os.environ['UCLOUD_TEST_POSTGRES_DSN'],'control-bench',schema=schema)
 await store.open();await store.migrate();state=PostgresRelayState(store)
 reg=await state.register_rollout('agent');token=reg['registration_token']
 async with store.transaction('seed') as conn:
  await conn.execute("""INSERT INTO relay_requests(deployment_id,request_id,rollout_id,registration_token,endpoint,method,created_at,expires_at,payload_bytes,reserved_bytes,state,request_digest,reattachable) SELECT %s,'renew-'||i,'agent',%s,'/test','POST',0,1e12,0,0,'completed','digest',false FROM generate_series(1,512)i""",(state.deployment,token))
  rows=await(await conn.execute("INSERT INTO relay_lifecycle(deployment_id,request_id,action,claim_token,claim_until) SELECT deployment_id,request_id,'wake',gen_random_uuid(),clock_timestamp()+interval '30 seconds' FROM relay_requests RETURNING *")).fetchall()
 state._active_claims={(r['request_id'],r['action']):r['claim_token']for r in rows}
 async with await AsyncConnection.connect(os.environ['UCLOUD_TEST_POSTGRES_DSN'],autocommit=True)as observer:
  async def lsn():return(await(await observer.execute('SELECT pg_current_wal_insert_lsn()')).fetchone())[0]
  async def old_auth():
   async with store.transaction('relay_authorize') as conn:await state._registration(conn,'agent',token)
  async def renew_one(row):
   async with store.transaction('relay_renew_lifecycle')as conn:
    await conn.execute("UPDATE relay_lifecycle SET claim_until=clock_timestamp()+%s*interval '1 second' WHERE deployment_id=%s AND request_id=%s AND action=%s AND claim_token=%s AND NOT done RETURNING request_id",(state.claim_seconds,state.deployment,row['request_id'],row['action'],row['claim_token']))
  try:
   for kind in ('authorize','renew'):
    for variant in ('baseline','candidate','candidate','baseline'):
     samples=[];store.observe=samples.append
     before=await lsn();wall=time.perf_counter();cpu=time.process_time()
     if kind=='authorize':
      for _ in range(512):
       if variant=='baseline':await old_auth()
       else:await state.require_current_registration('agent',token)
     elif variant=='baseline':await asyncio.gather(*(renew_one(row)for row in rows))
     else:await state._renew_lifecycle_claims()
     elapsed=time.perf_counter()-wall;cpusec=time.process_time()-cpu;after=await lsn()
     wal=(await(await observer.execute('SELECT pg_wal_lsn_diff(%s,%s)',(after,before))).fetchone())[0]
     print(json.dumps({'kind':kind,'variant':variant,'items':512,'seconds':elapsed,'python_cpu_seconds':cpusec,'transactions':len(samples),'wal_bytes':int(wal),'pool_wait_seconds':sum(s.pool_wait_seconds for s in samples),'commit_seconds':sum(s.commit_seconds for s in samples)}),flush=True)
  finally:
   await store.close()
   await observer.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
asyncio.run(main())
