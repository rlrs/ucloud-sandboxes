# UCloud sandbox qualification — 2026-09-05

Branch: `codex/sandbox-linux-compatibility`. Real UCloud gVisor nodes and an isolated UCloud builder were used. Production gateway/autoscaler installations were not replaced.

This is a one-task sample per family, not full-dataset certification. Image provisioning and setup alone do not establish grading or kernel-feature compatibility. Exact versions, artifact hashes, task keys and rebuilt image context hashes are in [the evidence JSON](sandbox-qualification-2026-09-05.json).

| Family | Setup | Gold / oracle |
| --- | --- | --- |
| swesmith-env | valid | valid |
| openswe | blocked: dataset access | blocked |
| swerebench-v2 | valid | invalid |
| scaleswe | valid | valid |
| swelego | valid | unchecked |
| multiswe | valid | valid |
| r2e-gym | valid | valid |
| swebench-pro | valid | unchecked |
| swebench-verified | valid | unchecked |
| swebench-multilingual | blocked: network policy | blocked |
| senior-swe-bench | error | error |
| tmax | valid | unchecked |
| terminal-lego | valid | oracle_passed |
| openthoughts-tblite | valid | failed |
| terminal-bench-2 | valid | unchecked |
| papersearchqa | valid | not exercised |
| wideseek | valid | not exercised |
| s1-deepresearch | valid | not exercised |
| openseeker | valid | not exercised |
| deepdive | valid | not exercised |
| browsecomp | valid | not exercised |
| redsearcher | valid | not exercised |
| browsecomp-plus | blocked: network policy | not exercised |

Setup passes: **19/23 families**. Upstream gold passes: **4**. Supplemental public-oracle passes: **1**.

## What the failures establish

- **POSIX ACLs need a different capability boundary.** The OpenThoughts ACL oracle fails at `setfacl` under the pinned gVisor runtime. The same operation succeeds in Docker on the UCloud builder VM. A home/PATH/profile change cannot fix this. Do not emulate ACL success with chmod or relax isolation silently.

- **Public images can often be recovered.** SWE-rebench’s dataset documents a reversible namespace rewrite to `docker.io/swerebenchv2/`. The sampled TMax, Terminal-Lego and OpenThoughts images were rebuilt from their public task Dockerfiles and referenced by manifest digest. Those are rebuilt artifacts, not verified bit-identical Prime exports.

- **Grading differs from setup.** SWE-rebench’s recovered public image starts correctly but the sample gold check fails; the captured output includes Node deprecation warnings rejected by tests and short Jest timeouts. Other upstream task classes return unchecked rather than implementing gold validation. Terminal-Lego’s actual setup, public solution and original grader pass in the supplemental oracle check.

- **Data authorization is separate.** OpenSWE rejects data access with the existing local Hugging Face login. Dataset metadata being public does not grant access to its gated data.

- **Framework-only network policy is not implemented by the adapter.** BrowseComp-Plus and SWE-bench Multilingual declare restricted policies. Verifiers correctly rejects them because `verifiers-ucloud` cannot enforce that policy; the run did not strip or relax it. Its BM25 index was built successfully after explicit cache placement and cleanup of temporary processed datasets.

- **Senior SWE Bench still needs an image rebuild.** Its wrapper injects Prime-hosted image names rather than pullable Docker Hub references. The public [pinned source](https://github.com/snorkel-ai/senior-swe-bench-v2026.06/tree/e30b0e19fdbc4b099e752c6d5324f5b250aee3dc) supplies a Dockerfile under `tasks/better-auth-fix-api-key-run/environment`. It can be built through `Image.from_dockerfile(...)` and used via an exact image alias, as for the terminal samples. That rebuild and subsequent qualification were not completed in this run; an unavailable prebuilt image is not evidence that the task itself cannot run.

## Runtime direction

Keep restricted gVisor `linux_session` for coding workloads whose required Linux features it supports, and preserve explicit `linux_host` for root-oriented benchmark compatibility. Offer a separate dedicated-VM backend for tasks requiring POSIX ACLs or other unsupported kernel/boot semantics. A plain Docker/runc switch on a shared worker would change the isolation boundary; a VM implementation needs explicit placement, image/boot artifacts, a guest agent, networking, lifecycle and checkpoint contracts. It is not implemented by this branch.

The initial taskset runs used the initial branch bundle. After the final cwd containment correction, all four userspace probes were repeated on a fresh UCloud node, including `//workspace//nested/.`; the full taskset run was not repeated.

## Local checks

`scripts/check.sh` passed 721 repository tests (four skips), 82 SDK tests, lint/format/shell checks, Go tests and package builds. The existing connection test was made deterministic by synchronizing the server close; it then passed 25 consecutive runs.

## Cleanup

All three qualification jobs were stopped (initial sandbox, builder, final sandbox); canary gateway/autoscaler/relay services were stopped and copied credentials removed. No canary sandboxes remained before shutdown. Production services were not replaced. Named test images remain in the private registry; source and digest evidence is retained for review.
