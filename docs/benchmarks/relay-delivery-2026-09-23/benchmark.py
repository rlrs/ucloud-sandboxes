import asyncio, ast, json, time, types
from pathlib import Path
import sys
sys.path.insert(0, str(Path.cwd()))
from unittest.mock import patch
from tests.test_postgres_relay import PostgresRelayTests, psycopg
import ucloud_sandboxes.shared_control.relay as relay
old_tree=ast.parse(Path('/tmp/relay-before-dirty-delivery.py').read_text())
old=next(x for c in old_tree.body if isinstance(c,ast.ClassDef) and c.name=='PostgresRelayState' for x in c.body if isinstance(x,ast.AsyncFunctionDef) and x.name=='_deliver_loop')
ns=vars(relay).copy();exec(compile(ast.Module(body=[old],type_ignores=[]),'<baseline-delivery>','exec'),ns)
async def trial(mode):
 t=PostgresRelayTests();await t.asyncSetUp(); waiting=[]; counts=[]; samples=[]
 try:
  state=t.state
  task=next(x for x in state._tasks if x.get_coro().__name__=='_deliver_loop');task.cancel();await asyncio.gather(task,return_exceptions=True);state._tasks.remove(task)
  if mode=='baseline':state._deliver_loop=types.MethodType(ns['_deliver_loop'],state)
  state._tasks.append(asyncio.create_task(state._deliver_loop()))
  reqs=[await t.enqueue() for _ in range(512)]
  original=psycopg.AsyncConnection.execute
  async def execute(conn,q,*a,**kw):
   c=await original(conn,q,*a,**kw)
   if isinstance(q,str) and q.startswith('SELECT request_id,state,delivery_pending,completed_bytes,completed_at'):counts.append(c.rowcount)
   return c
  async def until(pred):
   while not pred():await asyncio.sleep(.0001)
  with patch.object(psycopg.AsyncConnection,'execute',execute):
   waiting=[asyncio.create_task(state.wait_for_response(r,timeout_seconds=10)) for r in reqs]
   await asyncio.wait_for(until(lambda:512 in counts),5);await asyncio.sleep(.01)
   counts.clear();state.store.observe=lambda s:samples.append(s) if s.operation=='relay_delivery_status' else None
   cpu=time.process_time();wall=time.monotonic()
   for r in reqs[:32]:
    before=len(counts);state._signal('r:'+r.request_id);await asyncio.wait_for(until(lambda:len(counts)>before),2)
   await asyncio.sleep(.002)
   result={'mode':mode,'status_queries':len(counts),'returned_rows':sum(counts),'python_cpu_ms':1000*(time.process_time()-cpu),'wall_ms':1000*(time.monotonic()-wall),'status_transaction_ms':1000*sum(s.transaction_seconds for s in samples)}
  print(json.dumps(result),flush=True)
 finally:
  for x in waiting:x.cancel()
  await asyncio.gather(*waiting,return_exceptions=True);await t.asyncTearDown()
async def main():
 for mode in ('baseline','dirty','dirty','baseline'):await trial(mode)
asyncio.run(main())
