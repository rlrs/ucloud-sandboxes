# M1 gate run (2026-10-03)

The chunk store's M1 gate ([design §9](../../chunk-store-design.md#9-build-plan)) on S10's 181-image sample, run by `scripts/chunk_store_gate.py`.

**Result: fail.** Run `20261002t2212`, S3 prefix `spike/m1/20261002t2212`.

| Criterion | Status | Measured | Gate |
| --- | --- | --- | --- |
| full_tree | pass | 181/181 | 181/181 equal (contents, modes, owners, mtimes, xattrs) |
| stored_bytes | fail | 22.89 GB | 17.5 GB +-5% |
| cold_commands | fail | 20/21 within limit (over: 72:pip_version) | <= 1.3x S10 (pip: <= 1.3x S12 local, else 2.5 s), trace replayed |
| burst_20 | fail | 8.88 s (20 sandboxes) | <= 5.5 s (S11) |
| crash_injection | pass | {"failed_steps": [], "steps": 11} | every write-path step: killed, no visible partial image, rerun converges |
| rollback_10 | not run | - | 10 images unpacked, tree-equal and rebuilt by today's builder |

## Resources

29.52 VM-hours, about EUR 8.26 at list price (billed per started hour).

| Server | Type | Hours | Billed |
| --- | --- | ---: | ---: |
| sandboxes-m1-20261002t2212-converter | ccx63 | 10.37 | 11 |
| sandboxes-m1-20261002t2212-store | ccx43 | 10.38 | 11 |
| sandboxes-m1-20261002t2212-w1 | ccx43 | 7.07 | 8 |
| sandboxes-m1-20261002t2212-w2 | ccx43 | 1.7 | 2 |

## Cold first commands (traced)

| Image | Command | Wall s | Limit s | Ratio |
| ---: | --- | ---: | ---: | ---: |
| 0 | import_sys | 0.223 | 0.598 | 0.48 |
| 0 | git_status | 0.041 | 0.325 | 0.16 |
| 0 | pip_version | 1.106 | 2.327 | 0.62 |
| 60 | import_sys | 0.174 | 0.559 | 0.4 |
| 60 | git_status | 0.095 | 0.403 | 0.31 |
| 60 | pip_version | 0.992 | 2.327 | 0.55 |
| 63 | import_sys | 0.169 | 0.533 | 0.41 |
| 63 | git_status | 0.086 | 0.65 | 0.17 |
| 63 | pip_version | 1.056 | 2.431 | 0.56 |
| 72 | import_sys | 0.163 | 0.65 | 0.33 |
| 72 | git_status | 0.083 | 0.676 | 0.16 |
| 72 | pip_version | 0.167 | 2.5 | - |
| 90 | import_sys | 0.157 | 0.468 | 0.44 |
| 90 | git_status | 0.038 | 0.338 | 0.15 |
| 90 | pip_version | 0.941 | 2.5 | - |
| 130 | import_sys | 0.154 | 0.52 | 0.38 |
| 130 | git_status | 0.042 | 0.338 | 0.16 |
| 130 | pip_version | 0.72 | 2.5 | - |
| 170 | import_sys | 0.164 | 0.52 | 0.41 |
| 170 | git_status | 0.046 | 0.338 | 0.18 |
| 170 | pip_version | 0.803 | 2.5 | - |

Raw results: `summary.json` here, and `build/m1-gate/<run>/raw/` on the operator machine.

## Reading the result

The verdict above is the driver's, unedited. What each miss means:

- **stored_bytes (22.89 GB):** the live data meets the target, but the store carries
  dead bytes. Unique live chunks total 16.81 GB, 4% under 17.5 GB. The other 5.08 GB
  are duplicate chunk bytes:
  - 2.28 GB in 34 packs that no chunk references;
  - 2.80 GB of dead space inside 108 partly live packs.

  All of it was written in the first 12-way convert pass (22:40 to 23:26). The cause
  is a race. A converter looks up which chunks exist once per layer, after
  `nydus-image create`, and commits only after all of that layer's packs are
  uploaded, which takes minutes for a big layer. Converters packing different layers
  with the same files upload the same chunks, and the index keeps one copy.

  M3 GC would reclaim this, but M2's parallel migration would waste about 25% until
  then. Fix before M2: commit each pack as it lands and look chunks up again before
  packing the next one. That changes the §3 write path, so crash injection must run
  again.
- **cold_commands (20/21):** the miss is `72:pip_version`, which exits 127 because the
  image has no `pip` on its PATH. It has no S10 baseline either, so the 2.5 s default
  limit applied. The 20 other commands, replayed from traces, ran at 0.15–0.62× of
  S10.
- **burst_20 (8.88 s):** creates dominate, not reads. With traces replayed:
  - creates: median 5.1 s, maximum 8.1 s;
  - `import sys`: median 0.42 s, maximum 0.72 s;
  - `pip --version`: median 1.9 s.

  Without traces (demand mode) the wall time was 19.0 s, with create medians of 9.1 s.
  Attach is serial by default (`attach_concurrency=1`, 0.8.4). A rerun on the same
  worker with `--attach-concurrency 8` ([raw/bench-burst-attach8.json](raw/bench-burst-attach8.json))
  made creates a little faster, maximum 6.3 s. First commands got much slower, though
  (`import sys` median 3.3 s), and the wall time rose to 11.2 s. That is the 0.8.3
  regression again, so serial attach stays the default.

  The S11 target is not like-for-like. S11 timed bare `runsc` on local image trees,
  with no node agent and no remote store. A fair baseline is this harness on the
  current production path, prepared volumes on the same worker type. The burst needs
  that baseline before it counts as a regression. S11's other inputs still apply:
  concurrent attach without read contention, and a read path out of Python (C2.1).
- **rollback_10 (not run in the verdict):** the rollback step ran, but it failed, so
  the driver never recorded its results ([raw/rollback.jsonl](raw/rollback.jsonl)).
  - 8 of 10 images were unpacked, tree-equal and rebuilt by today's builder.
  - Images 170 and 177 differ in 2,076 entries each, all conda terminfo symlinks.
    `nydus-image unpack` drops `.` components from link targets (`.././61/adm1178`
    becomes `../61/adm1178`). Those resolve to the same file, but they are not
    byte-equal.
  - The forward path keeps the targets: its full-tree check compares them and passed
    for all 181 images. To make rollback exact, write the link targets from the RAFS
    inodes rather than from `nydus-image unpack`.

## Defects the gate found

Fixed during the run:

| Commit | Defect |
| --- | --- |
| `31ae8f8` | The verification mount read the index's writer locators, which name S3. With 12 converters, S3's tail outlasted the 30 s NBD timeout (EIO), so it now reads through the store node. The verifier also misread conda's hardlinked `.pyc` files after a later layer replaced one member of the group. |
| `2f2cc28` | `nydus-image` drops whiteouts when a layer returns to a directory it already left, as OpenSWE's slim layers do. Images kept files their layers had deleted, and the verifier caught it. Layers not in path order are now rewritten first, and the converter identity gains `;order=path`. The verifier also warms the store node before mounting: S3 fills under 12 converters had a p99 of 12.8 s and a maximum of 58.5 s. |
| `eac8851` | `hz.py` started private-only servers before their network was attached, so Hetzner refused and left them off. |
| `ad8f50a`, `63db821`, `3042115`, `756b39e`, `1b78db9`, `c446896`, `2a2de4f` | Gate driver and bench fixes: the canary config's build cache ref; init-vm's environment (`sudo -E` is ignored on the gateway); the node agent's create operation, sizes, delete fence, 503 retries and tombstoned ids; and the retained-mount drain before a cold reset. |

Open:
- the convert race behind stored_bytes, above;
- exact symlink targets in rollback;
- a same-harness burst baseline on prepared volumes.
