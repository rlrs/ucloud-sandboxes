# Prime taskset compatibility review — 2026-09-07

All 23 families in [Prime Intellect's article](https://www.primeintellect.ai/blog/scaling-agentic-rl)
are represented in the qualification manifest. Compatibility with every task is
**not established**. Family evidence starts with one sampled task per family,
followed by a separately qualified and deployed gVisor upgrade. The current
review also includes live work on an isolated 32-vCPU node, reported in the
[density and performance review](sandbox-density-performance-2026-09-07.md).
Fresh controlled comparisons exposed an OpenThoughts ACL defect. After the
runtime correction, the unchanged original oracle passes all nine tests on
the isolated node. SWE-rebench's original gold grader fails on both gVisor and
native Linux. The completed evidence is recorded below.

The manifest pins research-environments commit
`cb9cffdd0f08592081d281b3b38f30b62ae99edb`. On September 7, upstream HEAD was
`48cb24baecee7dc76bb55be86bf4a181ffcb60e8`; a Git comparison found no differences
under `environments/swe`, `environments/terminal` or `environments/search`.
This verifies the reviewed source inventory against that upstream revision;
dataset contents, image tags and external dependencies can still change.
The qualification environment used Verifiers
`555a17a62ee36a2ffe96772b8191402e44f30c43` and verifiers-ucloud
`fed54c688896057ddde95bf864babd614150bdb8`.

## Evidence for all 23 families

“Setup passed” means only the sampled setup/no-op check passed. It does not
establish tool use, correct grading, all languages/images, or performance at
128 concurrent sandboxes. Existing family results come from the
[original qualification](sandbox-qualification-2026-09-05.json), with the
later qualifications identified explicitly below.

| Article family | Sample evidence | Current qualification gap |
| --- | --- | --- |
| swesmith | Setup and gold passed | More tasks/languages and dense execution |
| openswe | Dataset access denied | Authorized GAIR/OpenSWE data; then image provisioning and grading |
| swerebench_v2 | Fresh gold fails on both native Linux and August gVisor | Fix or qualify upstream sample/image's deprecation-output and short-timeout failures |
| scaleswe | Setup and gold passed | More tasks and dense execution |
| swelego | Setup passed; gold unchecked | Exercise the actual gold/original grader path |
| multiswe | Setup and gold passed | More languages, images and dense execution |
| r2e_gym | Setup and gold passed | More tasks and dense execution |
| swebench_pro | Setup passed; gold unchecked | Exercise the public oracle and original grader |
| swebench_verified | Setup passed; gold unchecked | Exercise the public oracle and original grader |
| swebench_multilingual | Rejected before runtime | Framework-aware network enforcement and full grader lifecycle |
| senior_swe_bench | Gateway image unavailable; later rebuilt native-verifier sample passed | Publish rebuild, run through gateway, exercise remaining judging stages |
| tmax | Rebuilt sample setup passed; gold unchecked | Public oracle/grader and broader image availability |
| terminal_lego | Rebuilt sample setup and public oracle passed | More task images and dense execution |
| openthoughts_tblite | Corrected August candidate passes original oracle 9/9; native Linux also passes 9/9 | Deploy the complete corrected runtime to production, then broaden task and density coverage |
| terminal_bench_2 | Setup passed; gold unchecked | Public oracle/grader and heterogeneous per-task requirements |
| papersearchqa | Setup passed | Search-capable harness, model answers and judge |
| wideseek | Setup passed | Search-capable harness and table scoring |
| s1_deepresearch | Setup passed | Search-capable harness, model answers and judge |
| openseeker | Setup passed | Search-capable harness, model answers and judge |
| deepdive | Setup passed | Search-capable harness, model answers and judge |
| browsecomp | Setup passed | Search-capable harness, model answers and judge |
| redsearcher | Setup passed | Search-capable harness, model answers and judge |
| browsecomp_plus | Rejected before runtime; corpus/index built separately | Framework-only networking, BM25 tool lifecycle and judge |

The original counts remain **19/23 setup passes**, **4 upstream gold passes**
and **1 supplemental public-oracle pass**. They are historical results on the
then-current bundle, not a certification of the current release. A successful
process exit or an upstream `unchecked` result does not add another pass.

The article's eight search families primarily define tasks and scoring; the
harness supplies retrieval. BrowseComp-Plus adds its controlled BM25 tool.
Their coverage requires an actual harness/tool/model/judge run, beyond starting
a Linux container.

## The original ACL oracle passes after the runtime correction

The initial OpenThoughts task failed on the July runtime. The later
[patched August integration](gvisor-integration-2026-09-05.md) passed ACL access
enforcement and inheritance, UID/GID/supplementary groups and open-file locks
before and after ten hibernation cycles. Broader socket, pipe, event, signal,
timer and memory checks passed twenty cycles. The product Warden lifecycle also
passed with the real ublk/XFS backend.

The August distribution was [deployed as 0.5.28](gvisor-deployment-2026-09-05.md),
with ten further park/resume cycles. It is incorrect to describe POSIX ACLs as
entirely unsupported by the currently pinned runtime. However, the
[fresh OpenThoughts oracle](prime-acl-oracle-2026-09-07.json), using the same
immutable rebuilt image and original solution/grader, returned reward **0** on
the isolated 32-vCPU node. The solution succeeded; pytest reported **6 passed,
3 failed**. Alice and Bob could not append to each other's newly created files,
and Bob could not write into Alice's inherited subdirectory. Package installation,
ACL entry inspection and the other six assertions succeeded. The
[native comparison](prime-acl-native-2026-09-07.json) passed **9/9** and returned
reward **1**, using the same image, unchanged public solution and test scripts,
and the same **1 CPU / 2 GiB memory / 10 GiB writable quota**. Both ran on the
same isolated worker and cleaned up their owned sandboxes/containers. The native
runner stages the original scripts through Docker/runc; it is a controlled
Linux comparison, not a separate Verifiers integration qualification.

The pinned upstream `tmpfs.go` explicitly documents this case as
[gVisor issue 13688](https://github.com/google/gvisor/issues/13688), which remained
open when checked on September 7: the syscall layer applies the process umask before
default ACL inheritance. Linux skips the umask when a default ACL is present.
The earlier focused probe used a read-only inherited mask, which did not expose
loss of group write under a restrictive umask. Successful `setfacl` and
checkpoint persistence alone were insufficient evidence. Config-only `required_features`
admission remains conservative because it does not attest the selected
node/runtime/filesystem. July checkpoints cannot be restored by the August
runtime; changing a fingerprint cannot make their formats compatible.

The second August patch now carries the requested mode and umask separately
through VFS and overlay, allowing tmpfs to apply the default ACL before deciding
whether umask applies. The complete corrected distribution was
[installed on the isolated node](gvisor-acl-installation-2026-09-07.json), with
fresh runtime state and runsc SHA256
`9ebdf83ec7bf37f8be26660a915ccef7d67c9a9f0342896c41364be81a05d3c1`.
The [Linux build attestation](gvisor-acl-build-2026-09-07.json) records the
optimized build, all five passing test targets (including the complete tmpfs
suite), and the full companion-binary manifest. The retained distribution
archive under `dist/gvisor-acl-20260907/` is 149,120,650 bytes with SHA256
`e27d1a86c89837ac06aa08ca2cb4236b161fa564b479ea92b1143c0dec1bb986`.
Its patch-series SHA256 is
`53b928cb582c1c22c508ff590b91cff6f4d47cb871be8573ca31e39945061db1`.
The build attestation's pending-installation status describes build completion;
the separate installation and live qualification attestations record the later
steps.
The [unchanged original oracle](prime-acl-patched-2026-09-07.json) then returned
reward **1**, with **9/9 tests passing**, in 42.22 seconds. The immutable image,
original solution and grader, and **1 CPU / 2 GiB memory / 10 GiB writable
quota** were retained. Its owned sandbox was deleted. This proves the sampled
ACL task passes on the corrected candidate; it does not establish a production
deployment or all OpenThoughts tasks.

The [expanded functional qualification](gvisor-acl-qualification-2026-09-07.json)
also passed its initial checks, capture rollback, and **ten complete
park/wake cycles**. It tests cross-user ACL writes under umasks 0022/0077,
nested directories, FIFOs, Linux's separate socket-bind umask rule, explicit
private modes, setgid ownership and parents without default ACLs on `/tmp` and
`/srv`. Identity, open-file locks, paused handoff and restored CPU quota passed
every cycle. The fixture retained a **0.25-CPU quota and 512-MiB limit**.
The [raw cycle results](gvisor-acl-hibernation-2026-09-07.json) and
[exact input archive](prime-acl-qualification-inputs-2026-09-07.tar.gz) are retained.
The input archive's SHA256 is
`ee2c8a5ccb6c08fd020ba34dfa8654d8d7c69014708acf25f5f729f25d3bd3a7`;
the qualification attestation also records each final input file's hash.
The [original build and qualification output archive](../../dist/gvisor-acl-20260907/gvisor-acl-build-and-qualification-evidence-20260908.tar.gz),
exported from the running worker on September 8, contains 46 source files: build logs/status,
all twelve Bazel test-log/XML pairs, successful qualifier output and compact
failed-attempt diagnostics. It is 21,380 bytes with SHA256
`8d01aa79130208c610c77afbcb65f2e6dd7e425ae3348f27c7245c7a0009794e`.
Its [verification receipt and relative-path manifest](../../dist/gvisor-acl-20260907/gvisor-acl-build-and-qualification-evidence-20260908.json)
record every source hash. The successful result, command log and native setgid
control match the saved review evidence byte for byte. The original Bazel XML
files contain empty suite wrappers; their companion logs and build exit status
record the test outcomes.

Its first attempt exposed a fixture setup error: root lacked `CAP_FSETID`,
so setting group 42's setgid directory cleared the setgid bit. The resulting
child group was 0, failing the existing group-42 assertion. A
[native Linux control](gvisor-acl-native-fsetid-control-2026-09-07.json) confirmed
the same behavior. The final qualifier explicitly grants that capability to
root setup and reports detached guest errors through the verification socket.
It preserves every permission assertion, resource limit, and unprivileged
identity transition. Failed attempts are included in the attestation. Cleanup
found no remaining qualifier processes, cgroups or mounts.

The correction also adds fields to serialized VFS option types. Pre-correction
August checkpoints are not assumed compatible despite the unchanged upstream
commit. Preserve the complete old executable distribution for existing owners;
the runsc and companion hashes in the runtime fingerprint must remain enforced.

The September 8 mount-retention candidate was separately installed in a fresh
dev-node runtime generation. Its [unchanged original ACL oracle](prime-acl-mountfix-2026-09-08.json)
again passes **9/9** with reward **1** and cleanup verified. Its
[ten-cycle hibernation repeat](gvisor-mountfix-hibernation-2026-09-08.json) also
passes all existing ACL, identity, lock, paused-handoff and CPU-quota checks.
The [installation manifest](gvisor-mountfix-installation-2026-09-08.json) records
runsc SHA256 `be491ee25a10a9036b46037dbd21343cc3bf4a4eee90ca21885e7848124bd9e5`
and every companion hash. These repeat passes cover the sampled ACL task and
functional fixture; they do not add family-wide or production qualifications.

The [Senior SWE follow-up](sandbox-compatibility-followup-2026-09-05.md) rebuilt
one public image and passed its four native verifier checks on native Linux and
upstream August gVisor with 4 CPUs and 8 GiB. It did not publish the image or
run the model-based judging stages. That is promising sample evidence, not a
complete gateway taskset pass.

## SWE-rebench fails on native Linux as well

The controlled comparison reran `elastic__synthetics-316`, task key
`f0fe2c516a578bdad76e0817b324ad15917bcaf6d02e34be08567c80d4e8c114`, using immutable
image `swerebenchv2/elastic-synthetics@sha256:43dec4337c7e470deff79db238b03e914f8a5234277b52ec7f3fd414e8978883`
and the task's resolved **4 CPUs / 4 GiB memory / 10 GiB writable quota** on the
same isolated worker. Both retained the original gold patch, test command,
warning handling, timeouts and reward parser.

| Runtime | Original test result | Gold reward | Evidence |
| --- | --- | --- | --- |
| August gVisor, candidate v3 | 13 failed, 83 passed; 3 failed suites | 0 | [Result](prime-rebench-gvisor-2026-09-07.json), [test output](prime-rebench-gvisor-2026-09-07.tests.txt) |
| Native Docker/runc | 30 failed, 66 passed; 11 failed suites | 0 | [Result](prime-rebench-native-2026-09-07.json), [test output](prime-rebench-native-2026-09-07.tests.txt) |

Every named gVisor test failure also appears in the native output. Both hit
Node's `DEP0147` warning, which the sampled CLI tests treat as a failure when
anything reaches stderr. Native additionally hit the original five-second
Jest timeouts and browser-related assertions. The sample therefore does not
establish a gVisor-specific failure or a passing SWE-rebench qualification.
Suppressing warnings or extending the grader timeout would change the evidence.

The native harness runs the pinned task's method bodies and parser through a
Docker adapter; its eleven setup/patch/test operations were compared with the
actual pinned task before execution and matched. This isolates native behavior
without claiming a second full Verifiers integration. The two runs had different
cache/bootstrap costs and do not constitute a general performance comparison.
Both cleaned up their owned runtime resources; the
[comparison cleanup inventory](prime-compatibility-cleanup-2026-09-07.json) verified no sandboxes,
active creates, active ublk devices, storage errors, hard reservations or owned
native containers/networks remained.

## Remaining implementation boundaries

1. **Framework network policy.** The adapter's `UCloudRuntimeConfig` derives from
   `BaseRuntimeConfig`, not the Verifiers `NetworkPolicyConfig`; it only exposes
   `network_access` as a Boolean. Both SWE-bench Multilingual and BrowseComp-Plus
   declare `network_allow=[]`. Verifiers refuses that pairing. The qualification
   manifest correctly blocks these families before downloading data/indexes.
   Verifiers permits framework model/MCP routes after trusted setup, so these
   tasks still need transport to those endpoints. The current node's network
   rules allow public egress for the whole sandbox CIDR; its private exceptions
   are also shared. Existing relay endpoints can transport HTTP to a fixed
   configured upstream, but they do not restrict a sandbox's other traffic.
   `network=none` removes the framework route as well and is only supported by
   a node configured without the bridge manager.

   The smallest correct implementation for these two empty allowlists needs
   durable per-sandbox enforcement: trusted setup may use its setup policy,
   then execution must allow only the dedicated framework relay endpoint and
   deny other egress. Existing global exceptions must not bypass that rule.
   Policy publication, readback and restoration must be fenced to the sandbox
   generation across restart, park/wake and migration. This involves this
   repository's network manager, lifecycle/API and tests, plus an SDK policy
   operation and the peer adapter's `NetworkPolicyConfig`, `prepare_setup` and
   `prepare_execution` implementation. The existing relay can carry the limited
   framework-only case; general URL/glob allow/block policies would need
   additional enforcement and should remain rejected until implemented.

   BrowseComp-Plus also exposes a shared BM25 MCP subprocess on the framework
   host. Pinned Verifiers' `v1/mcp/launch.py` hardcodes `PrimeTunnel` for that
   host-to-remote-harness route. A transport hook in Verifiers or a controlled
   adapter integration must expose it through UCloud while preserving the
   signed per-rollout state callback used for search metrics. Supplying a bare
   external MCP URL loses that managed state integration. Required acceptance
   evidence includes real denied public/private/DNS egress, allowed model and
   state callbacks, shared MCP results and metrics, and the same behavior after
   park/wake and restart. No such network implementation or qualification is
   included in this review.
2. **Data and images.** OpenSWE data authorization remains external; its source
   also explains that its images were built into Prime's registry. Prime image
   handles are not generically pullable OCI names. The checked-in alias only
   reverses SWE-rebench's documented namespace mapping. Exact public rebuilds
   need published manifest digests and source provenance. Senior SWE has a
   pinned public build recipe, but only one recipe is represented here.
3. **Heterogeneous tasks.** Harbor task configs can request GPUs or separate
   verifier environments. The current UCloud adapter explicitly rejects GPUs;
   the supplemental oracle runner explicitly rejects separate verifier
   environments. This review did not inventory every task's generated config,
   so absence of those requirements cannot be assumed. gVisor is still a
   userspace kernel: system boot, nested containers, arbitrary devices and
   unqualified Linux interfaces have no universal compatibility guarantee.
4. **Failed and unchecked grading.** The matched native/gVisor comparison above
   shows SWE-rebench's sample fails on both runtimes. A passing original grader
   is still required. Increasing timeouts or suppressing warnings without tracing
   the original verifier would alter the benchmark rather than establish a pass.
   Harbor wrappers returning `unchecked` need their real public solution and
   grading lifecycle exercised. Preserve grading-material visibility rules.

## Resource requirements and the 128-sandbox target

Seven pinned SWE wrappers—SWE-smith, OpenSWE, SWE-rebench, ScaleSWE, SWE-Lego,
Multi-SWE and R2E-Gym—request **4 CPUs, 4 GiB memory and 10 GiB disk per task**.
128 such resolved requests total **512 requested CPUs, 512 GiB guest memory
and 1.25 TiB writable quotas**, before runtime and host overhead. Quotas are
limits, not necessarily immediately occupied disk blocks. SWE-bench
Multilingual overrides memory to 8 GiB; Harbor task resources otherwise vary.
A 32-vCPU node cannot give 128 simultaneously CPU-bound tasks four physical
cores each. A dense profile needs measured smaller limits, realistic idle
fractions and controlled active concurrency; the upstream resource defaults
must remain visible in the comparison.

The pinned Verifiers resolver has a concrete override trap: it decides whether
to apply task resources by comparing each runtime value to its model default.
An explicitly supplied value equal to that default is overwritten by the task.
This was reproduced locally using the installed qualification dependencies,
without creating a sandbox:

| Requested runtime CPU / memory GiB / disk GiB | Resolved for a 4 / 4 / 10 SWE task |
| --- | --- |
| 0.25 / 2 / 5 | 0.25 / 4 / 10 |
| 1 / 2 / 5 | 4 / 4 / 10 |
| 0.25 / 1 / 10 | 0.25 / 1 / 10 |

Thus a command saying `--runtime.memory 2` is insufficient evidence of a 2 GiB
guest. The qualification wrapper now records the **resolved configuration** in
`<taskset>-runtimes.jsonl` before creation. Use `--cpu`, `--memory-gib` and
`--disk-gib` on the qualification command to require explicit values; if the
task resolver replaces one, qualification rejects that runtime before
provisioning and records the mismatch. Omitted limits preserve upstream defaults.
The record includes source/resolved image names and limits, without environment
variables or credentials; it is not a host attestation. Confirm the actual
sandbox spec when measuring density. Correct precedence belongs in the peer
Verifiers resolver; the peer SDK and adapter repositories were not edited here.

Any 128-sandbox acceptance run should cover distinct cold images as well as
warm shared images, representative compiler/test subprocesses, dirty writable
data and populated memory, park/wake latency at the intended concurrency,
guest progress and correctness after waking, resident host usage and CPU
throttling. Small empty/sleeping sandboxes alone do not establish the target
workload's performance. The per-family source/gold checks and the node-density
benchmark answer different questions and both are required.

## Qualification reliability changes

The previous verdict parser accepted contradictory check counts, such as
`gold={"valid":1,"invalid":1}` with `total=1`; Boolean counts also passed as
integers. A `null` or array summary raised `AttributeError`, ending the entire
23-family sweep. It could accept a smaller completed sample than requested.

`scripts/qualify_prime_tasksets.py` now rejects malformed structures, non-integer
or negative counts, contradictory totals, incomplete terminal counters and a
nonzero requested sample count differing from the reported count. Invalid
summary evidence yields `failed_or_incomplete` and permits subsequent families
to run. The parser preserves the verdicts of the saved real qualification
summaries. Thirteen focused qualification tests (23 including the environment
contract tests) and Ruff passed on September 7. The resource regression proves
the mismatch is recorded before the underlying runtime can create a sandbox,
and verifies that matching values and omitted limits still work. A fresh plan
verified all pinned taskset source hashes and produced 23 checks; execution
was not requested for that plan.

These changes make incomplete evidence harder to misreport. They do not remove
any runtime, dataset, image or grading prerequisite described above.
