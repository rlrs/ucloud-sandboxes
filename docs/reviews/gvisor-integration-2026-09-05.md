# Patched gVisor 20260817 integration

The active build now targets gVisor `20260817.0`, commit
`50e1502a95d36ad2faf2c7ef33b8bf21fe975293`. The port preserves disk-backed
application memory, quota ownership, two-phase hibernation capture, CPU startup
burst, and paused restore. The older five patch files remain historical reference;
the build applies `runtime/gvisor/20260817/0001-ucloud-hibernation.patch`.

This was built and qualified on actual UCloud job **12381696**, in DFM
Pretraining, using `vm-ubuntu:26.04`, 16 AMD Zen 5 vCPUs, 48 GiB memory, and
kernel `7.0.0-30-generic`. Build/cache and test data used the VM's local disk,
not `/tmp` tmpfs or shared `/work`. The test VM was stopped and its final UCloud
state verified as `SUCCESS`; no test containers or sentries remained before shutdown.
Production gateway/runtime configuration was
not replaced. The storage backend was copied read-only from the deployed
UCloud release and verified against its existing build manifest.

The resulting distribution is
`gvisor-hibernate-69386782a596ce54bf0547807208392ea84ba1794f2f2264cb1c580416b28f2f`.
Its main executable SHA256 is
`bf977a610da69323196f681294436c5460b5ad2f2f4e492bcaa7dbd4332b2773`.
The gzip archive SHA256 is
`1580606042d13eb9dc56a24fae8324e44a1085bf82b23354f7857216b62e9a99`.
The full build manifest, companion identities and qualification outputs are in
[gvisor-integration-2026-09-05.json](gvisor-integration-2026-09-05.json).

| Qualification on UCloud | Result |
| --- | --- |
| Focused allocator, loader, CPU startup and paused-status tests | All five tests passed across four Bazel targets |
| Complete optimized upstream release build | Passed with pinned Bazel 8.3.1 |
| Native runc vs patched gVisor, same digest-pinned Python image | All six feature probes passed on both tmpfs and image filesystem |
| ACL access enforcement and default inheritance; open-file flock; persistent UID/GID/supplementary groups | Passed before and after ten hibernation cycles |
| Capture rollback, paused restore, CPU startup burst | Rollback passed; no counter movement before resume; exact `25000 100000` CPU quota restored in all ten cycles |
| TCP/Unix sockets, pipes, epoll/eventfd, timerfd, signals, condition variables, deleted-open file, populated memory | Passed twenty hibernation cycles |
| Shellless scratch image with static helper, UID/GID 1000 and group 42 | All six checks passed, including denied root write and preservation after oversized writes |
| Product Warden with real ublk/XFS and deployed storage backend | Create, exec, park, wake, and post-wake conformance passed |
| Journal recovery model | 100 iterations of each of eleven durable crash states passed; these are simulated states, not 1100 actual process/VM crashes |
| Build output directory containing spaces and an apostrophe | Passed with identical executable identities |

The initial allocator port failed the existing truncated-backing-file regression:
upstream's protobuf import installed allocator state before validating the backing
file size. The final patch validates that size before import. Both positive restore
and damaged-file rejection pass. The broader build initially needed additional
cross-compiler/eBPF prerequisites, and namespace-dependent tests needed Linux
root on Ubuntu 26.04; no gVisor isolation policy was weakened to make them pass.

The old Warden qualifier also assumed an image lease would always receive the
original tag. The current product resumes using the canonical image ID. Its fixed
fixture accepts either identity for the same immutable image; the final Warden
run passed using the real pinned runtime and companion fingerprint.

Deployment now stages and verifies the whole executable distribution. Missing,
extra, symlinked, or modified companions fail validation. Bootstrap verifies the
exact companion set and installs it beside `runsc` in its own directory. Repacking
preserves and verifies those files. Runtime checkpoint fingerprints include the
installed companion hashes, so an unchanged `runsc` with a changed sentry cannot
reuse the same checkpoint identity. Legacy runtimes retain their old fingerprint
when no companion directory is present.

The repository check passed **740 tests** (four skips), **82 SDK tests**, managed
process Go tests, Ruff, shell checks, and installed-wheel checks. Additional
regressions cover companion tampering, complete offline bundle construction,
bootstrap rejection, and checkpoint fingerprint changes. The pinned build script
also passes ShellCheck directly.

To reproduce, build the exact checkout using `runtime/gvisor/build_pinned.sh` and
keep its complete output directory together. On a disposable Linux node, run
`scripts/qualify_gvisor_hibernation.py --runsc /absolute/distribution/runsc
--output /absolute/new-evidence-directory --cycles 10`. The adjacent
`compatibility_workload.py` is the guest fixture. The existing conformance and
storage Warden qualifiers cover the other lifecycle checks; the latter now accepts
`--runsc-commit` to record the actual source pin.

July and August checkpoint memory formats are incompatible. Existing sandboxes
must retain the July distribution until drained, deleted, or migrated at the
workload-data level. Do not rewrite checkpoint fingerprints to bypass this fence.
The code supports deploying the new distribution to fresh nodes with the new
explicit commit pin; this qualification did not replace production sandboxes.

This closes the observed ACL runtime gap without claiming universal Linux or
Prime taskset compatibility. Full Prime tasksets, registry migration, framework
network policy, and inaccessible task data/images were not requalified here.
Configuration-only feature admission remains conservative until it can attest the
selected node/runtime/filesystem. gVisor still provides a userspace kernel, not a
booted Linux VM.
