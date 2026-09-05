# Sandbox compatibility follow-up — 2026-09-05

Implementation remains on `codex/sandbox-linux-compatibility`. Production gateway,
node artifacts and runtime pins were not replaced.

## Implemented

- Conservative create-time feature requirements and a configuration-only
  environment description endpoint, with no guest environment dump or implicit wake.
- Explicit supplementary groups, shared-memory size, workspace storage selection,
  IPv4 resolver configuration, and inherited default cwd for managed jobs.
- Opt-in static guest file management and restore readiness, reusing the supervisor
  artifact; legacy bundles retain the shell path.
- Behavioral feature probes and a shellless OCI conformance runner.
- Known Prime framework-network requirements fail before dataset/index setup.

## Actual UCloud comparison

A fresh isolated VM, job `12381596`, used the same UCloud VM application and
`cpu-amd-zen5-16-vcpu` product as the earlier builder. Host kernel was
`7.0.0-30-generic`; Docker was `29.1.3`. Neither this VM nor the production gateway
VM exposed `/dev/kvm`. This finding applies to the tested VM class, not all UCloud
products. A dedicated provider VM is a viable architectural alternative to nested
KVM; no production VM sandbox backend was implemented.

The same Python image and probe ran with 1 CPU and 1 GiB, testing both `/tmp`
tmpfs and `/srv` on the image filesystem:

| Runtime | Paths, xattrs, locks, sockets, signals | ACL enforcement + inheritance |
| --- | --- | --- |
| Native Linux / runc | pass on both filesystems | pass on both |
| Upstream gVisor 20260721.0 | pass on both | `EOPNOTSUPP` on both |
| Upstream gVisor 20260817.0 | pass on both | pass on both |

Image: `python@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285`.
The July comparison used the upstream binary, not our patched binary; the earlier
qualification independently observed the ACL failure on our pinned deployment.
Both upstream runtime versions passed a separate shellless test using this branch's OCI builder
and static helper, including UID/GID 1000, supplementary GID 42, denied root writes,
and preservation of existing files on oversized writes. These are direct runtime
checks on UCloud, not a complete gateway/lifecycle qualification.

An initial ACL probe under `/root` was not interpretable because test identities
could not traverse the parent directory. The revised probe reports this prerequisite
as blocked. The final matrix uses `/tmp` and `/srv`. An initial shellless-runner
cleanup exposed runsc's retained `null-netns` bind mount; the checked-in runner now
unmounts that owned namespace after successful container deletion before removing
its work directory.

## Runtime upgrade gates

An upgrade can plausibly resolve the observed ACL issue without a VM. It cannot
be substituted directly into our node bundles:

- New source commit: `50e1502a95d36ad2faf2c7ef33b8bf21fe975293`.
- The first existing checkpoint patch already fails against changed kernel,
  memory save/restore, checkpoint and sandbox code; `runsc/cmd/boot.go` has moved.
- Upstream ACL support changes many filesystem/VFS paths. The tmpfs merge alone
  touches 45 files, so a backport also requires substantial review and testing.
- Upstream commit `abe7f4ad0d1f862fbf35c387522285fdc677ea38` explicitly disables
  save/restore for its POSIX ACL syscall tests because they time out. Passing the
  live ACL probe does not demonstrate persistence across checkpoint/restore.
- Newer runtime packaging includes sidecar binaries; artifact verification must
  cover the complete distribution, not just the runsc executable.

Port the patch series in a separate candidate build, preserve its source and
artifact digests, and require the existing memory/paused-handoff/crash-recovery
qualification plus ACL/identity/file-lock persistence before changing the pin.
See [upstream ACL test change](https://github.com/google/gvisor/commit/abe7f4ad0d1f862fbf35c387522285fdc677ea38)
and [upstream packaging](https://gvisor.dev/docs/user_guide/install/).

## Remaining policy and coverage work

Framework-aware networking remains unsupported. Implement per-sandbox egress
restrictions together with relay/framework access and adapter policy composition.
A proxy environment variable alone is insufficient: enforce the boundary outside
the guest and test raw sockets, DNS, alternate addresses and lifecycle recovery.
Do not remove task restrictions or claim adapter support ahead of enforcement.

OpenSWE data authorization remains external. Search tool/model/grading workflows,
the full task populations, systemd, confined setup symlinks, hostname/hosts and
lifecycle persistence remain unqualified. Requirements intentionally fail closed
for unsupported or unqualified features; ordinary requests without requirements
retain existing behavior.

## Public Senior SWE recovery

The previously unavailable sampled image was rebuilt on the isolated UCloud VM
from public source commit `e30b0e19fdbc4b099e752c6d5324f5b250aee3dc`, context
`tasks/better-auth-fix-api-key-run/environment`. Dockerfile SHA256:
`9fb0f60054ac6b2eee9f307988e447ed53222b204e188a074ea1e8c82a0180ca`.
The local image ID was
`sha256:07367158abb86f11e6a4e36aa68539f622d8593e5017542d09fb06b55539e7ec`.
This is a rebuilt artifact, not a verified identical Prime image or a registry
manifest that another host can pull.

The supplied solution and original verifier script through its native-verifier
stage passed **4/4 checks on native Linux and 4/4 on gVisor 20260817.0**, with
4 CPUs and 8 GiB as specified by the task. Model-based judging stages were not run.
The gVisor run explicitly installed our direct runtime's resolver contents;
Docker's custom-network embedded DNS failed even with `--dns` flags, causing
suppressed pip errors and missing verifier dependencies. The intermediate failed
runs returned process exit 0 but `all_pass: false`, illustrating why grading
results, prerequisite failures and process exit must be tracked separately.
The build used host networking on this isolated builder VM after Docker's default
bridge MTU impeded downloads. Task runs used an isolated Docker network with MTU
1420; host networking was not used for task execution.

`docs/prime-public-builds.json` pins the public recipe.
`scripts/build_public_task_image.py` validates a clean pinned checkout, builds via
the existing UCloud image API when explicitly executed, and emits an exact
source-to-registry-digest alias only for a successfully published available image.
Its source/alias validation is tested locally; this new wrapper was not itself
executed through a gateway during this run. The rebuilt reference image was not
published to the production registry.


## Verification and cleanup

Final `scripts/check.sh`: **733 repository tests** (four skips), **82 SDK tests**,
Go tests, Ruff, shell syntax/ShellCheck, and wheel/package checks passed.
The source/alias planner was also run against the clean pinned public Senior SWE
checkout. No production deployment or registry publication was performed.

Reference VM `12381596` was stopped and UCloud reported `SUCCESS`. Test containers
were deleted; copied temporary UCloud authentication/session files were removed.
The rebuilt image was ephemeral to the reference VM; its recipe and verification
evidence remain available. The peer SDK and adapter checkouts were not edited.

Exact runtime/image/helper identities, probe results, source hashes and remaining
limitations are in [the evidence JSON](sandbox-compatibility-followup-2026-09-05.json).
