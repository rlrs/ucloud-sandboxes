# Fresh Hetzner production, 2026-09-27

Deployed the v0.7.0 release from `a8da55500336c5d7c3fe1f5dd4fb972308f1727b`.
The previous deployment had been deleted. This deployment has an empty initial
registry, new database state, new client tokens and new environment signing keys.

- Gateway: `167683409`, CPX32, public `77.42.92.27`, private `10.42.0.2`.
- Registry: Volume `106968761`, requested size 1000 GB (provider reports
  1,073,741,824,000 block-device bytes), ext4 at `/mnt/ucloud-registry`.
- HTTPS SDK endpoint: `https://77.42.92.27`.
- HTTPS relay endpoint: `https://77.42.92.27/relay`.
- Generated deployment and resource ledger: gitignored `build/hetzner-prod/`.
- Client environment: gitignored `build/hetzner-prod/client.env`, mode 0600.
  Source this file to select the fresh deployment; the top-level `.env` was not
  changed. Administrative token is kept separately in the local tokens folder.

Workers scale from zero to three CCX63 nodes; builders scale from zero to four
CCX33 nodes. Idle nodes stop after five minutes. Both use snapshot `436561313`
with freshly repacked 0.7.0 bundles. Native backend and runsc are unchanged.
The observed worker NBD pool is 1024 devices.

## Live checks

Gateway, placement, relay, autoscaler and registry were active, with no failed
systemd units. Gateway, worker and builder health endpoints reported 0.7.0.
Registry GC has the writer fence and `registry-recover` ExecStopPost installed.

The public SDK triggered worker `167684176` and builder `167684177` automatically.
A pinned Python image import plus cold sandbox creation took 81.025 seconds,
including scale-up from no workers/builders. Exec and file upload/download passed.
The administrative lifecycle check then reused that sandbox: park, publication
and detach, restore from the registry, file preservation, exec, and deletion all
passed. See `lifecycle.jsonl`. Explicit park/detach use the administrative token;
the initial public SDK token was correctly denied that administrative endpoint.

A Dockerfile derived from the imported Python image built in 5.541 seconds; its
added file was read successfully in a new sandbox. See `build.jsonl`.
An external SDK relay worker also served a real sandbox request through the
public HTTPS relay, preserving the upstream path, body and authorization header.
All test sandboxes were deleted.

These are deployment smoke checks, not a concurrent-load qualification. The
release's GitHub CI still has unrelated script/test lint failures and a Python
3.10-only test using `asyncio.timeout`; this bring-up does not claim CI is green.
