# Sandbox realism and environment contract review

Date: 2026-09-05. Scope: sandbox specification, direct OCI construction,
provisioning, exec/file APIs, managed processes, SDK defaults, and pinned runtime.
This records the baseline review before implementation. See
[implemented environment contracts](../linux-environments.md) for branch changes
and qualification instructions. Line numbers below refer to the baseline.

## Recommendation

Keep gVisor as the default isolation backend. Add versioned environment presets
that make userspace behavior predictable, with security policy configured
separately. Introduce a VM backend only for workloads that require a real guest
kernel or machine lifecycle. A single numerical “Linux realism” slider would
hide independent choices about identity, services, persistence, and privileges.

The current `linux_host` profile is a convenience bootstrap script, not a booted
Linux machine. Many improvements are possible without replacing gVisor.

## Findings in the current implementation

### 1. Workspace preparation can change permissions on system directories

`sandbox.py:994` uses one restrictive path validator for working directories,
workspace destinations, and file paths. It rejects `/`, spaces, and Unicode,
while accepting `/etc`, `/proc`, repeated leading slashes, and trailing `/.`.
`direct_oci.py:366` then unconditionally changes the configured workspace to
mode 0777. A local temporary-rootfs probe confirmed that choosing `/etc` changes
its mode from 0755 to 0777. This concerns the sandbox filesystem; it is not
evidence of a host escape.

Workspace preparation already uses descriptor-relative traversal with
`O_NOFOLLOW`, which is a good foundation for host-side safety. Preserve it.
Add destination-specific rules: reject root, runtime-reserved paths, and
system-directory collisions for managed workspaces; never chmod an arbitrary
existing image directory as a side effect. Create owned workspaces with explicit
UID/GID and modes, with a separate deliberate shared-directory option.

Use POSIX guest-path types independently of the controller OS. Accept ordinary
Linux filenames for file operations and `/` for cwd. Define normalization and
symlink semantics separately: lexical cleanup alone does not establish
filesystem containment, and collapsing `..` across symlinks changes meaning.
For setup destinations, rejecting ambiguous components is appropriate.

### 2. “Linux host” mixes compatibility and security defaults

`sandbox.py:796` disables `no_new_privileges`, removes the PID limit, preserves
the image user, and restores the default capability set. Root images consequently
run as root. These changes are not required simply to supply a home directory,
standard paths, or common Linux tools.

`SandboxSpec.from_dict` applies these profile defaults only when the whole
security object is absent. A partial object such as
`security={"read_only_rootfs": false}` instead receives container security
defaults, including UID/GID 1000 and dropped capabilities. This was reproduced
locally. Direct dataclass construction also does not apply the profile factory
defaults automatically.

Resolve each field through one versioned preset resolver, shared by all create
paths. Keep finite outer resource limits for every preset. Make root and required
capabilities explicit policy choices and show the resolved specification.
Preserve legacy behavior through a named version rather than changing existing
requests silently.

### 3. Services can report success without working

`sandbox.py:817` installs a `service` shim that returns success for unsupported
operations and failed starts. Cron and SSH startup swallow failures. Cron may
be started through both `service` and a direct invocation. Host startup replaces
the image entrypoint/command with the wrapper and uses only `spec.command`.

Replace this with explicit lifecycle modes: image process, supervised session,
or experimentally supported system init. Required services must fail readiness
with useful diagnostics; optional services should report degraded status. Use
foreground processes under supervision, restart policies, signal forwarding,
and bounded logs. Keep benchmark directories such as `/tests`, `/oracle`, and
`/logs/verifier` in an integration preset rather than the generic Linux layout.

The existing managed supervisor is a useful base, but currently rejects
`linux_host` and has its own primary-job protocol. Service supervision needs an
explicit extension and lifecycle qualification, not just relaxing that check.

### 4. Identity and process environment are only partially specified

`direct_oci.py:501` merges image and request environment. It does not establish
coherent HOME, USER, LOGNAME, SHELL, locale, or a fallback PATH.
`direct_oci.py:524` accepts numeric users only and maps a UID without a GID to
the same numeric GID. It does not resolve the image passwd/group databases or
provide supplementary groups. Default UID 1000 may have no account or home.

Initial cwd falls back to image cwd or `/`; managed jobs default independently
to `/workspace` (`managed_process.py:32`, also repeated in the SDK). Configuring
a different workspace does not update that default. Missing custom cwd is not
created by the workspace preparation step.

Define one resolved identity and environment contract for initial processes,
exec, jobs, file operations, and future terminals. Preserve image settings in
the image preset; provision accounts/homes only in an explicitly managed preset.
Resolve named users from bounded image-local files without executing image NSS
helpers on the host. Define precedence, supplementary groups, home ownership,
cwd existence, login-shell behavior, and environment overrides. Normal exec
should use argv directly; shell interpretation should be an explicit API mode.

### 5. Management operations depend on image tools

`direct_service.py:1326` reads files using `/bin/cat`; writes require `/bin/sh`,
`mkdir`, `mktemp`, `cat`, `chmod`, `mv`, and `rm`. `direct_warden.py:235` uses
`/bin/true` for readiness. Minimal or distroless images therefore lack management
features even when their own application can execute.

The write script also computes an empty parent for `/file`, then calls
`mkdir -p ""`; this follows directly from `${target%/*}`. Every write forces
mode 0600. The API needs documented behavior for mode preservation, executable
files, symlinks, directory targets, and atomic replacement.

Add a small static management helper executed inside the sandbox under the
resolved workload identity. Reuse the existing pinned-artifact installation
mechanism, with architecture checks, bounded I/O, and structured errors.
Distinguish runtime readiness from application readiness. Do not replace guest
file operations with unrestricted host filesystem access. A helper in a
root-writable guest filesystem is a functionality dependency, not a trusted
security authority; guest root can modify it.

### 6. Mount and resolver behavior is surprising

`direct_oci.py:559` mounts fixed-size `/tmp`, `/run`, and `/dev/shm` tmpfs
filesystems (shared memory is fixed at 64 MiB). With `enforce_disk_quota=true`,
it overlays the workspace with tmpfs sized to `disk_mb`, hiding image contents
at that location and introducing memory-backed storage. The normal writable
rootfs already has storage-native quota authority. The option name obscures
its actual effect.

Expose workspace storage and tmpfs sizing explicitly, including memory charging
and persistence across restart, park/wake, detach, and clone. Retire the ambiguous
option through versioning. Do not assume tmpfs is lost on park: checkpoint
restoration is different from reboot and must be tested for this runtime.

`direct_oci.py:425` overwrites resolv.conf with public resolvers 1.1.1.1 and
8.8.8.8 and requires an existing, non-symlink `/etc`. This need not work under
the configured egress policy and does not establish a coherent hosts/hostname
contract. Configure DNS through node network policy, validate reachability,
define offline behavior, and create hostname/hosts/resolver state consistently.
Permit standard distro symlink layouts only through resolution confined to the
guest rootfs; never follow an image absolute symlink as a host path.

## Configurable levels

These are proposed presets, not fields currently accepted by the API.

| Preset | Behavior | Isolation and limitations |
| --- | --- | --- |
| `oci-v1` | Image entrypoint, image environment, explicit process policy; minimal mutation | gVisor; management should not depend on shell tools |
| `linux-session-v1` | Qualified distro image, real user/home, coherent cwd/PATH, standard directories, optional supervised services | gVisor; recommended default for interactive coding and agent tasks |
| `linux-system-v1` | Experimental distro init/systemd and service lifecycle | gVisor; qualified capability subset, no promise of full kernel semantics |
| `linux-vm-v1` | Booted guest kernel and distro init, deliberate reboot/device/filesystem contract | Separate KVM VM backend and resource class |

Allow structured overrides for identity, workspace, services, temporary storage,
and environment. Keep isolation/backend selection and security policy separate.
Requests for unsupported features should fail with actionable errors rather
than silently relaxing policy or moving to another backend.

Current upstream gVisor lists systemd as **experimental**, requiring
`--in-sandbox-cgroup=v2`. Its compatibility documentation notes that in-sandbox
cgroups do not enforce limits between guest processes, block-device filesystems
cannot be mounted inside the sentry, and several kernel facilities have gaps.
This repository pins patched release `20260721.0`; upstream documentation is not
proof that this exact build supports those features. Qualify the pinned build,
including park/wake, before offering the system preset. See
[upstream application matrix](https://gvisor.dev/application-compatibility/) and
[compatibility details](https://gvisor.dev/docs/user_guide/compatibility/).

A VM backend such as Firecracker provides a real guest kernel/rootfs boundary;
it still needs explicit choices about devices, kernel configuration, and reboot
semantics. See its [getting-started documentation](https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md).
First verify accessible KVM/nested virtualization on each provider node class.
Provider VMs used to host gVisor today do not establish that nested KVM is usable.
If unavailable, a dedicated provider VM per sandbox is an alternative with a
different cost and startup profile.

VM work requires boot artifacts, a guest agent, networking, capacity admission,
health checks, and separate checkpoint compatibility. Reuse gateway routing,
generation fencing, and ownership principles, but do not assume the patched
runsc memory/checkpoint format or current storage integration transfers to a VM.
Changing gVisor's execution platform to KVM alone does not add a guest Linux
kernel or solve its syscall compatibility gaps.

## Implementation order and verification

1. **Environment correctness:** centralize preset resolution and guest paths;
   fix `/` cwd and `/file` writes; prevent workspace permission collisions;
   unify job/exec cwd; replace silent service success with diagnostics.
2. **Image-independent management:** static helper, image preflight, resolved
   environment description, and structured unsupported-feature errors.
3. **Qualified Linux session:** a pinned distro image with account/home,
   certificates, locale, tooling, and explicit service supervision. Offer root
   package installation as policy, while retaining outer resource bounds.
4. **System-mode experiment:** validate systemd on the exact patched runtime,
   including service timers and suspension semantics. Ship only tested features.
5. **VM proof of concept:** only if representative workloads still need kernel
   or boot semantics. Measure density, startup, cost, park/wake, and recovery
   before committing to a production backend.

Add an `environment.describe()`-style response containing effective backend,
runtime/image digest, preset version, identity, paths, mount persistence,
resource limits, services, and qualified features. Return selected environment
facts rather than dumping environment values or secrets. Keep requested,
resolved, and probed facts distinct.

Build a live conformance matrix covering a qualified Debian/Ubuntu image,
Alpine/BusyBox, and a minimal image. Include named and numeric users, arbitrary
workspace locations, spaces/Unicode, image symlinks, absent home/PATH/cwd,
read-only roots, direct argv versus shells, file modes, root-level files,
signals/zombies, service failures, DNS, and disk/tmpfs exhaustion. Repeat
applicable probes after park/wake and migration. Compare representative
workloads against a real Linux VM to measure compatibility instead of promising
generic machine equivalence. Especially test timers/cron and broken network
connections across suspension; an idle parked sandbox cannot provide ordinary
always-on server behavior without a scheduling policy.

## Evidence collected

Read the implementation and SDK call defaults listed above. Ran:

```text
.venv/bin/python -m unittest tests.test_direct_oci tests.test_sandbox_exec tests.test_managed_process
Ran 28 tests — OK (skipped=1)
```

Additional disposable local probes confirmed path-validation behavior, `/etc`
workspace chmod, and partial-profile security defaults. No live Linux sandbox,
systemd, KVM, or provider qualification was performed. Passing existing tests
does not establish machine compatibility; the proposed conformance suite fills
that gap.
