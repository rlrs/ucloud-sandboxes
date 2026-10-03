# Rollout start from zero on 0.8.4: the "before" baseline (2026-10-03)

The first full C9.2 `rollout` run (plan W9) is the "before" for every later
image-path and control-plane change. It was taken on production as it stood
before any M2 switch.

## Setup

| | |
| --- | --- |
| Release | gateway and workers 0.8.4; worker snapshot `438866767`; `attach_concurrency` 1 |
| Gateway | Hetzner CCX23 `77.42.92.27`, which also hosts the registry Volume (2.8 TB used) |
| Workers | autoscaled CCX63 (48 vCPU, 192 GB), `policy.max_nodes` 3, from **zero**: no sandbox node heartbeated at start |
| Image path | immutable environments: registry-backed EROFS components through the Python NBD backend, 128 GiB chunk cache per worker, metadata hints and traces on |
| Driver | `scripts/bench_rl_scale.py rollout`, run on the gateway, SDK 0.4.34; the report records the harness sha256 |
| Tasks | 512 task rows sampled from the 2026-10-01 selection (`--seed 1`, row-weighted), covering **195 distinct images** with 220 GB of EROFS |
| Shape | `linux_host`; all 512 creates at once (no client cap, no ramp); 8 turns with 5–30 s sleep think time, mixing grep, test and edit; sandboxes not grouped by task |

```sh
python3 bench_rl_scale.py rollout --gateway-url https://77.42.92.27 \
  --api-token-file /var/lib/ucloud-sandboxes/state/sandbox-api-token \
  --operator-token-file /var/lib/ucloud-sandboxes/state/gateway-token \
  --selection all-cached-training-tasks-with-terminal-lego-2026-10-01.zip \
  --tasks 512 --seed 1 --fleet-state zero --output zero-512.json
```

## Results

**511 of 512 ready.** The run took 510 s end to end.

| Measure | p50 | p95 | p99 | max |
| --- | ---: | ---: | ---: | ---: |
| Time to ready (s) | 120.1 | 147.1 | 148.0 | 151.1 |
| Time to first command (s) | 121.4 | 147.8 | 150.8 | 155.8 |
| First command alone (s) | 1.13 | 7.8 | 10.7 | 13.1 |
| Worker-side create, `manager_create` (ms) | 382 | 532 | | 591 |

- **Fleet.** No worker heartbeated at 60 s. All three did by 105 s.
- **Peak.** 511 live sandboxes at 151 s, split 183, 182 and 146 across the
  three nodes. At peak a node used about 17 GB of its 189 GB, so memory was
  nowhere near binding. No pauses or parks happened, as expected in sleep mode.
- **Image bytes.**
  - Workers downloaded about 7.0 GB of chunks, **3.2%** of the distinct
    images' EROFS bytes.
  - The chunk-cache hit ratio was 0.90, with 0 corruptions and 0 fetch
    retries.
- **Turns** (p50 / p95 / p99, s):

  | Turn | p50 | p95 | p99 |
  | --- | ---: | ---: | ---: |
  | edit | 0.04 | 0.45 | 0.83 |
  | grep | 0.12 | 0.88 | 1.8 |
  | test | 0.05 | 1.3 | 55.9 |

  The test tail is the tests themselves.
- **Families.** Ready p50 ran from 108 s (ScaleSWE, n = 11) to 127 s (TMax
  and Terminal-Lego). The ordering is by arrival into the queue, not by image.
- **The one failure.** A SWE-rebench v2 task named an upstream image
  (`prime/primeintellect/getmoto-moto:…`) that has no prepared copy. Its import
  from Docker Hub was refused (`pull access denied`), giving `image_import_failed`
  at create. That is a catalog gap, not a capacity one.

## Reading it

- **Where the two minutes go.** There are two parts:
  1. **Provisioning from zero:** 60–105 s until the three CCX63 heartbeat.
  2. **Queueing on each node:** a further 20–80 s after its node is up, though
     a worker's own create takes 0.38 s.

  About 180 creates per node at 0.38 s each is about 70 s. So creates look
  roughly serialized per node: storage prepare (174 ms p50), runsc create and
  start, and serial component attach. That is the hypothesis to test before
  changing it. The image bytes moved are small (3%), so data movement was not
  the limit at this scale, despite W9's expectation.
- **Levers this points to:**
  - warm capacity or faster provisioning for scheduled runs;
  - per-node create concurrency (C5.2 follow-ups, `attach_concurrency`
    without the 0.8.3 read contention);
  - a larger `max_nodes`. At 17 GB per 183 sandboxes, density is bounded by the
    create path and the node cap, not by memory.
- **What this run does not cover:**
  - **Rollout groups:** each sandbox is an independent task row. A
    `--group-size` run (for example 64 × 8) is the baseline for C3.2.
  - **Recipe builds:** 262 of 512 tasks were OpenSWE, TMax and Terminal-Lego
    recipe entries that fell back to their prepared base
    (`prepared_reference_fallback`), so their task delta and live build were
    not exercised.
  - **Pause and park:** sleep think mode only. Relay and park runs measure
    those.
  - **Repetition:** this is one run, and the plan asks for three comparable
    runs before citing externally.

Raw reports:
- [raw/zero-512.json](raw/zero-512.json): the full report, including the
  density timeline, per-node `environment_io` deltas, create phases and
  samples;
- [raw/zero-8-smoke.json](raw/zero-8-smoke.json): the 8-sandbox smoke from
  zero just before it, with ready at about 61 s, all of it provisioning.
