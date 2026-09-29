The controller applied the tested candidate at18:47:27UTC; see deployment-receipt.json.
The instructions below document staging, guards and rollback, not an instruction
to repeat the completed deployment.

Fixed candidate root: `/work/ucloud-sandboxes/gateway-dispatch-optimization-20260928`.
Create this directory with mode0755 so unprivileged autoscaler reads future-node bundles. The rollback subdirectory is0700; receipts/config backup remain private. Place the new wheel, this controller, and `scripts/repack_node_bundle.py` there. Use the existing gateway Python3.14.4 interpreter, never the broad installer script.

Known current source root: `/work/ucloud-sandboxes/gateway-inventory-optimization-20260928`.
Current qualified source hashes:
- builder: `d7aa96fbcaafba16305937241fd2df208d0d40f1b711f0e3964693a9a9817f01`
- sandbox: `67fdfd24afb2dea3bfd2bab5cdf18efe9f2a37aaee8977ddb4d9101e1866b00d`
- repacker source: `9fddb39307442fabea50f78c80b296a5c32b2e00952e7624af7c02709bd0da5b`

Stage, after substituting the independently checked new wheel digest:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python /work/ucloud-sandboxes/gateway-dispatch-optimization-20260928/gateway_dispatch_deploy.py stage \
  --wheel-sha256 NEW_WHEEL_SHA256 \
  --repacker-sha256 9fddb39307442fabea50f78c80b296a5c32b2e00952e7624af7c02709bd0da5b \
  --builder-source-sha256 d7aa96fbcaafba16305937241fd2df208d0d40f1b711f0e3964693a9a9817f01 \
  --sandbox-source-sha256 67fdfd24afb2dea3bfd2bab5cdf18efe9f2a37aaee8977ddb4d9101e1866b00d
```

Review the printed receipt and independently hash `staging-receipt.json`. Keep that digest outside the remote staging directory. Both commands below pin the entire reviewed receipt, including the candidate wheel/bundle/controller/repacker hashes and unchanged live config hash:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python /work/ucloud-sandboxes/gateway-dispatch-optimization-20260928/gateway_dispatch_deploy.py check --receipt-sha256 REVIEWED_RECEIPT_SHA256
/work/ucloud-sandboxes/gateway-venv/bin/python /work/ucloud-sandboxes/gateway-dispatch-optimization-20260928/gateway_dispatch_deploy.py apply --receipt-sha256 REVIEWED_RECEIPT_SHA256
```

Apply copies the full currentvenv/config into a fresh `rollback` directory, rechecks idle state, stops autoscaler/placement/gateway/relay units (KillMode=control-group), installs the pinned wheel using `pip --no-index --no-deps --force-reinstall`, verifies all non-package venv files/modes unchanged, and runs `pip check`. The only config change is `node_package_root`;64GiB relay budget and all other settings remain as read. Config publication uses validation and atomic rename, preserving mode/ownership. PostgreSQL, nginx, registry, native binaries, OS packages, current workers, and provider resources are untouched.

Checks require gateway/relay publicHTTPS and local health, nonempty gateway metrics, healthy relay stats with64GiB budget and available PostgreSQL pool metrics. Apply failures restore the exact backup and verify original health. A pre-stop idle/config guard failure aborts without stopping services. Rollback artifacts are never overwritten.

Manual rollback, after stopping/cleaning the qualification harness and verifying no activework:

```sh
/work/ucloud-sandboxes/gateway-venv/bin/python /work/ucloud-sandboxes/gateway-dispatch-optimization-20260928/gateway_dispatch_deploy.py rollback
```

Outputs: staging-receipt.json; deployment-receipt.json; rollback/backup-manifest.json; automatic-rollback-receipt.json if needed. Failed runtime directory is retained for inspection. No database schema change or migration is performed; apply is suitable only for a Python-only, dependency-compatible patch.

Tests: `/tmp/test_gateway_dispatch_deploy.py`,6mock tests covering idle/lifecycle rejection, successfulpackage/rootchange, dependency-corruption rollback, arrivalofnewworkbeforestop, atomic config validationfailure, and corruptedbackup refusal. These do not replace staging verification of the actual candidate bundles.
