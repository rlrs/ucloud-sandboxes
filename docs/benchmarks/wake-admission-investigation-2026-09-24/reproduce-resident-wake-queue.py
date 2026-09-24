from contextlib import ExitStack
from concurrent.futures import ThreadPoolExecutor
import json
from tests.test_managed_growth import ManagedGrowthTests
f=ManagedGrowthTests();f.setUp()
try:
 s=f.service
 s.start_managed_process('one',f.spec)
 s.observe_managed_wait('one',7,'request-one')
 assert s.get('one').state=='running'
 result={'runtime_state':'running','available_memory_mib':f.available,'growth_needed_bytes':s._growth_cost(('one',7),4<<30,request_id='request-one').memory_bytes,'restore_slots':s._restore_slots.capacity}
 with ThreadPoolExecutor(max_workers=1) as pool:
  with ExitStack() as held:
   for i in range(s._restore_slots.capacity):held.enter_context(s._restore_slot(owner=('slow-restore-'+str(i),1)))
   wake=pool.submit(s.admit_managed_continuation,'one',7,'request-one')
   wake.result(1)
   result['queued_behind_restore_slots']=s._restore_slots.waiting
   result['resident_wake_blocked_despite_fitting_memory']=not wake.done()
  wake.result(2)
 result['completed_before_restore_slots_released']=True
 print(json.dumps(result,indent=2))
finally:f.doCleanups()
