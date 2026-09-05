# Linux environment compatibility

The sandbox backend is the pinned, patched gVisor runtime. It provides a Linux
userspace and a virtualized syscall interface, not a booted guest Linux kernel.
The UCloud VM hosting it does not change this distinction.

## Choose userspace behavior separately from isolation

| API `profile` | Startup and environment | Default process policy |
| --- | --- | --- |
| `container` | Image entrypoint/command and cwd; minimal environment defaults | UID/GID 1000, capabilities dropped, no-new-privileges, 256 PIDs |
| `linux_session` | Persistent shell session, workspace cwd, image PATH, passwd-derived HOME (workspace fallback when no home is declared) | Same restrictions as `container` |
| `linux_host` | Persistent benchmark session, image user/cwd, conventional benchmark directories | Legacy root-capable policy; no inner PID limit |

All profiles retain the same outer runtime, resource admission and host network
policy. `linux_host` does not supply systemd, reboot, mounts, devices or a full
kernel. A security override is independent of the profile. Partial overrides
inherit the remaining profile defaults. Direct Python and JSON construction use
the same resolution.

For example, the create API accepts:

```json
{
  "id": "coding-session",
  "image": "ubuntu:noble-20260324",
  "profile": "linux_session",
  "memory_mb": 2048,
  "disk_mb": 5120,
  "cpus": 1,
  "filesystem": {"workspace_path": "/workspace"}
}
```

The gateway and node must both run this branch to use `linux_session`. Older
SDKs may restrict the profile type or serialize their own defaults; use the
create API directly until an SDK release exposes the new preset. Existing SDK
benchmark requests continue to use `linux_host`.

Named users and groups resolve only against the image's `/etc/passwd` and
`/etc/group`. Numeric UID-only requests use the image's primary GID when known,
otherwise the legacy UID=GID fallback. Explicit `uid:gid` remains available.
Host accounts are never consulted. Supplementary groups are not inferred. No
account or home directory is created: choose an image with a suitable account
or supply HOME explicitly. A system account such as `nobody` may deliberately
have `/nonexistent` as its home.

Image PATH is preserved, including Conda/virtualenv prefixes; startup does not
source login profiles. Explicit request environment takes precedence. Session
HOME follows the selected identity rather than an unrelated image HOME value.

## Paths and startup failures

File paths and cwd use Linux/POSIX rules independently of the client OS. `/` is
a valid cwd; spaces, Unicode, colons and commas are accepted for file/cwd APIs.
Control characters, relative paths and `..` are rejected. Bootstrap/workspace
paths additionally require canonical components and exclude serialized
separators. Managed workspaces cannot overlap reserved system/runtime roots.
Host-side preparation uses descriptor-relative traversal without following
symlinks; an image with a symlink at a required setup path is rejected.

Existing directory modes are preserved. Newly created shared workspaces are
sticky mode 1777. Guest writes to root-level files such as `/eval.sh` work;
uploads reject directory targets, replace files atomically and create mode
0600 files. Set executable permissions explicitly when needed. These operations
still require shell tools inside the image; distroless compatibility is not
claimed.

Requested cron or SSH services fail startup when dependencies or configuration
are missing. There is no fake successful `service` shim and no blanket chmod
of benchmark/system directories. Service supervision, distro installation,
systemd and reboot remain separate work.

## Qualification

`docs/prime-tasksets.json` pins the source and hashes for all 23 environment
families in Prime Intellect's [Scaling Agentic RL article](https://www.primeintellect.ai/blog/scaling-agentic-rl).
It is a coverage inventory, not a claim that every task passes.

Install the pinned research tasksets editable alongside a compatible Verifiers
fork and `verifiers-ucloud` in an isolated environment, then:

```sh
python scripts/qualify_prime_tasksets.py \
  --source /path/to/pinned/research-environments \
  --python /path/to/qualification/bin/python --output /tmp/prime-plan
```

Add `--execute` to provision real sandboxes using `UCLOUD_SANDBOX_URL` and
`UCLOUD_SANDBOX_API_TOKEN`. Output directories must be new. Default sampling is
one task per family; `--num-tasks 0` requests the complete datasets. SWE and
terminal checks require valid setup and gold grading. Search checks qualify
setup only, not model-based answering, search providers or grading. Missing,
unchecked and error results never count as success. Dataset loading and image
access require their own credentials; Prime image handles are not automatically
interchangeable with OCI registry references.

`qualify_linux_environment.py --output /tmp/linux-results.json` exercises the
live gateway's userspace contract on Ubuntu, Alpine and named users, including
root cwd, root-level uploads, PATH and Unicode filenames. It creates disposable
sandboxes and reports deletion failures. Run it with this repository installed
and the SDK available. `qualify_guest_userspace.py` is a complementary Docker
shell-script check, not a gVisor conformance test.

Use a separate deployment ID, state directory and `node_package_root` for a
canary. The optional absolute `node_package_root` selects that deployment's
sandbox/builder release bundles; omitted, it preserves `/work/ucloud-sandboxes/release`.
This avoids replacing production bundles during runtime qualification.

## Further realism

A qualified distro image and explicit tooling/service contracts can address
most agent userspace assumptions without relaxing isolation. A static guest
management helper would remove shell-tool assumptions. Configurable DNS,
shared-memory sizing, workspace persistence and capability reporting need
separate contracts and live park/wake tests.

Systemd support should be an experimental preset qualified against the exact
patched runtime. Workloads requiring unsupported syscalls, devices or boot
semantics need a distinct VM backend with its own kernel, guest agent and
checkpoint contract. Do not silently fall back to a less isolated backend.

### Public image sources and oracle checks

Use `--image-aliases docs/prime-image-aliases.json` to reverse the documented
SWE-rebench namespace rewrite. Mappings are scoped to a taskset, recorded in the
plan, and logged when applied. Unrecognized names remain unchanged. For locally
rebuilt task Dockerfiles, an alias can instead specify an exact `source` and
`target` image reference; use the build's immutable manifest digest. Rebuilds
are distinct artifacts and must be identified as such in qualification results.

For Harbor tasksets whose upstream `validate()` returns unchecked,
`qualify_harbor_oracle.py --taskset NAME --task-dir DIR --image REF --output FILE`
runs the task's actual setup hook, public `solution/solve.sh`, and original
shared verifier. It does not support separate verifier environments. An oracle
pass supplements setup evidence; it still tests only the selected task.

The live September 5 qualification found that POSIX ACLs are **unsupported** by
the pinned gVisor runtime: the OpenThoughts ACL oracle fails at `setfacl`. The
same operation succeeds in Docker on the UCloud builder VM. No choice of these
userspace profiles fixes that kernel-interface gap. Consequently this branch
does **not** claim compatibility with every task in the 23 families. Covering
ACL-dependent tasks requires a backend providing those Linux semantics (for
example a dedicated VM), or implementing and qualifying ACL support in gVisor.
Silently ignoring ACL errors or substituting `chmod` would change the task and
its permission semantics.

See the [UCloud qualification report](reviews/sandbox-qualification-2026-09-05.md)
for the 23-family sample matrix and remaining blockers. The existing
`verifiers-ucloud` adapter also lacks the framework-aware network policy needed
by BrowseComp-Plus and SWE-bench Multilingual; disabling all network access is not a substitute for
allowing only framework services.
