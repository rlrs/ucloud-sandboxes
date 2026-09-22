"""Linux-only microbenchmark. Set UCLOUD_TEST_POSTGRES_DSN to a disposable DB.

Argument: baseline relay.py extracted from commit c714325. Uses isolated schemas.
Run with the candidate package on PYTHONPATH; PostgreSQL must already be running.
"""
import asyncio,importlib.util,sys,time,os,json,statistics
from pathlib import Path
from uuid import uuid4
from ucloud_sandboxes.shared_control.postgres import PostgresControlStore
from ucloud_sandboxes.shared_control.relay import PostgresRelayState
from ucloud_sandboxes import model_relay as api
from psycopg import AsyncConnection,sql
spec=importlib.util.spec_from_file_location('ucloud_sandboxes.shared_control.relay_before',sys.argv[1]);old=importlib.util.module_from_spec(spec);sys.modules[spec.name]=old;spec.loader.exec_module(old)
async def run(label,cls):
 dsn=os.environ['UCLOUD_TEST_POSTGRES_DSN'];schema='ucloud_shared_poll_'+uuid4().hex
 store=PostgresControlStore(dsn,'test',schema=schema,max_connections=16)
 await store.open();await store.migrate();await store.close()
 store=PostgresControlStore(dsn,'test',schema=schema,max_connections=16)
 state=cls(store);await state.open()
 try:
  regs=[await state.register_rollout('agent-'+str(i)) for i in range(64)]
  times=[];samples=[];store.observe=samples.append
  async def agent(i):
   reg=regs[i]
   for j in range(16):
    await state.enqueue(rollout_id=reg['rollout_id'],endpoint='/v1/responses',body=b'x'*32768,headers={})
    t=time.monotonic();requests=await state.poll(rollout_id=reg['rollout_id'],registration_token=reg['registration_token'],worker_id='worker',timeout_seconds=0,limit=1)
    times.append(time.monotonic()-t);assert len(requests)==1;r=requests[0]
    await state.respond(request_id=r.request_id,registration_token=reg['registration_token'],lease_id=r.lease_id,response=api.RelayWorkerResponse(200,b'answer'))
  wall=time.monotonic();cpu=time.process_time();await asyncio.gather(*(agent(i) for i in range(64)))
  times.sort();print(json.dumps({'version':label,'requests':len(times),'seconds':time.monotonic()-wall,'cpu':time.process_time()-cpu,'poll_p50':statistics.median(times),'poll_p95':times[int(.95*len(times))],'heartbeat_transactions':sum(s.operation=='relay_worker' for s in samples),'claim_transactions':sum(s.operation=='relay_claim_inference' for s in samples)}),flush=True)
 finally:
  await state.aclose()
  async with await AsyncConnection.connect(dsn,autocommit=True) as conn:await conn.execute(sql.SQL('DROP SCHEMA {} CASCADE').format(sql.Identifier(schema)))
async def main():
 for label,cls in [('before',old.PostgresRelayState),('after',PostgresRelayState),('after',PostgresRelayState),('before',old.PostgresRelayState)]:await run(label,cls)
asyncio.run(main())
