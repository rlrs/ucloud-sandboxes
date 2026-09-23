import importlib.util,json,tempfile,time,unittest
from pathlib import Path
from ucloud_sandboxes.direct_registry import DirectSandboxRegistry
from ucloud_sandboxes.sandbox import SandboxSpec
import ucloud_sandboxes.guest_paths as original
import ucloud_sandboxes.sandbox as sandbox
spec=importlib.util.spec_from_file_location('candidate_guest_paths','/tmp/candidate_guest_paths.py');candidate=importlib.util.module_from_spec(spec);spec.loader.exec_module(candidate)
with tempfile.TemporaryDirectory(prefix='owned-registry-benchmark-') as directory:
    registry=DirectSandboxRegistry(Path(directory)/'fixture.sqlite')
    planned=registry.plan(spec=SandboxSpec(id='relay-load-fixture-sandbox-0199',image='registry/image@sha256:'+'a'*64,memory_mb=2048,disk_mb=4096,parkable=True,managed_process=True),sandbox_generation=1,operation_id='create:1',runtime_compatibility_sha256='b'*64)
    row=(planned.sandbox_id,planned.image_id,registry._encode(planned))
    print(json.dumps({'record_bytes':len(row[2]),'python':__import__('sys').version,'fixture':'isolated planned registration, default 17 writable guest paths'}),flush=True)
    for round in range(2):
        for variant,module in [('baseline',original),('lexical',candidate),('lexical',candidate),('baseline',original)]:
            sandbox.validate_setup_path=module.validate_setup_path;sandbox.validate_workspace_path=module.validate_workspace_path
            for label,operation in [('get',lambda:registry.get(planned.sandbox_id)),('decode',lambda:registry._decode(row))]:
                for _ in range(100):operation()
                wall=time.monotonic();cpu=time.process_time()
                for _ in range(4096):operation()
                print(json.dumps({'round':round,'variant':variant,'phase':label,'iterations':4096,'seconds':time.monotonic()-wall,'cpu_seconds':time.process_time()-cpu}),flush=True)
# Exercise the same checked-in policy equivalence tests against the candidate.
import sys
sys.modules['ucloud_sandboxes.guest_paths']=candidate
spec=importlib.util.spec_from_file_location('owned_guest_path_tests','/tmp/test_guest_paths.py');tests=importlib.util.module_from_spec(spec);spec.loader.exec_module(tests)
result=unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromModule(tests))
if not result.wasSuccessful():raise SystemExit(1)
