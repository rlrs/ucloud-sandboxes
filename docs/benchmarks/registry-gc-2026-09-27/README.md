# Registry GC writer-fence qualification — 2026-09-27

Passed on an isolated Hetzner CPX42, Linux 7.0.0-30-generic, using Docker
Distribution 3.1.1 and an ext4 filesystem. Production remained spun down.
The disposable VM was deleted after qualification.

The actual packaged registry service holds the shared writer fence while its
Docker process runs. The collector stops that service, takes the exclusive
fence, checks that no named container remains (including created/stopped states),
and only then scans and deletes. Startup, including stale-container cleanup,
waits for the exclusive fence to be released. Reference pruning stays online;
physical collection makes registry reads and writes unavailable for its duration.

`runtime/storage_native/qualify_registry_gc.py` exercises real HTTP uploads,
manifest publication and deletion, then the real service stop/collect/restart
path. The results in [qualification.json](qualification.json) confirm:

- Live manifest and layer remain readable; only unreachable content is removed.
- New publication works after collection.
- An exception in collection restarts the registry.
- SIGKILL of the collector triggers the packaged GC unit's marker-based
  `ExecStopPost` recovery; the live blob remains readable.

The tiny five-blob fixture collected two blobs and one stale repository link in
15 ms. Stop, collection and HTTP readiness together took 1.23 seconds. **This is
not a downtime bound for a large registry.** Larger inventories require a longer
registry maintenance window. There is no safe fixed settling interval for a
paused manifest PUT; fully online physical GC requires writer participation.

The targeted registry, systemd, deployment and configuration suites passed on
both Linux and macOS: **85 tests**, no skips. Ruff passed on the changed collector,
coordinator, writer-fence tests and native qualification script. Regression tests
cover a writer paused between validation and manifest commit, startup waiting for
collection, refusal when a container remains, refreshed links whose ancestor
mtime did not change, unknown/missing manifest references, interrupted journals,
and conditional abnormal-termination recovery.

The native script requires an empty disposable registry and explicitly named
hostname. Install the packaged registry/GC units with the fixture interpreter
and deployment config first, then run:

```sh
PYTHONPATH=. python runtime/storage_native/qualify_registry_gc.py \
  --config /path/to/fixture-deployment.json \
  --expected-hostname sandboxes-gc-qualification --output /tmp/gc-result.json
```

Its SIGKILL scenario temporarily overrides only the fixture GC unit's ExecStart,
then removes that override and reloads systemd. Never run it against a production
registry. Source hashes and the registry image digest are retained alongside the
result.
