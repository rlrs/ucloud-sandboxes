# Phase 3c porting spec: one node admission function in `ucloud-noded`

Status: decided 2026-10-11 (decisions in §7); lands after 0.9.72 is applied on an idle fleet. Sources: C5.3 in `docs/rl-scale-architecture-plan.md:897-910`, and the phases, "State ownership is the key decision", "Phase 1 boundaries" and "Phase 3" sections of `docs/rust-node-daemon-plan.md` (`:89-107`, `:157-220`, `:515-532`). Background: `docs/managed-growth-admission.md`, `docs/transition-admission.md` and `docs/managed-process-reads.md` (the current in-flight design).

## 0. Where the code differs from the docs

These corrections change the spec, so they come first.

1. **The registry schema is v9, not v5.** See `ucloud_sandboxes/direct_registry.py:47` and `runtime/noded/src/registry/schema.rs:18`. v3–v8 migrate in place (`direct_registry.py:2298-2321`).
   - `managed_growth` arrived in v5 (`:2309-2310`), and `relay_wake_fences` arrived in v4 (`:2311-2312`).
   - 3c needs **no schema change**, and that is what makes a unit-swap rollback possible.
2. **Python's registry writes do not go over a UDS RPC.** With noded as owner, the agent is a *foreign* SQLite writer:
   - it uses SQLite's cross-process locking, and its reads come from a revalidated cache (`direct_registry.py:465-469`, `cached_reads=True`);
   - phase 1 chose this over option 1's "thin RPC client" (`rust-node-daemon-plan.md:165-170`).
   - Today the agent is the **only writer of `managed_growth`**:
     - noded's Rust port of the writer exists (`runtime/noded/src/registry/mod.rs:993-1029`, `:1191-1316`) and is covered by cross-language tests (`runtime/noded/tests/registry_python_compat.rs:271-353`, `:381-453`);
     - but nothing outside `registry/` calls it.
3. **Heartbeats are still built and sent by the Python agent** (`node_agent.py:227-275`, `:1984-1997`, `heartbeat_sender.py:61`). This is true despite phase 0's "sends heartbeats" (`rust-node-daemon-plan.md:116`).
4. **There are no "FairCapacity property tests".** `tests/test_fair_capacity.py` has four deterministic tests (`:11`, `:42`, `:55`, `:70`). C5.3's gate ("carry over unchanged") needs property tests written first (step 3c.1).
5. **`/internal/v1/managed/{launch,bind,terminal}` do not exist yet.** Neither do noded's job routes; `docs/managed-process-reads.md` is design only. A grep of the repo finds no route.
6. **`wake_admission.py` is the gateway's placement reservation**, not node admission (`control_plane.py:166`, `wake_placement.py:23`). It is out of 3c's scope; see §5, risk 9.
7. **`transition_admission_snapshot()` has no production caller.** Only tests use it (`direct_service.py:2620`).
8. **The managed growth suite has grown.** `tests/test_managed_growth.py` has 33 tests; the doc still says "fourteen".
9. **`_active_admission_guard` is 139 lines** (`direct_service.py:2869-3007`).
   - All three callers pass `check_cpu=False` (`:2021`, `:3032`, `:3067`), so its CPU-retry branch (`:2988-2995`, `:3004-3007`, `_CPU_ADMISSION_DEADLINE_SECONDS` at `:87`) is dead for node admission.
   - It still applies these memory checks:
     - memory PSI full avg10 ≥ 10 (`resource_admission.py:152-155`);
     - the 2 GiB physical floor (`:161-162`);
     - RAM+swap headroom (`:163-172`);
     - the ledger projections (`direct_service.py:2943-2968`).

---

## 1. Inventory: admission state and every reader and writer

### 1.1 State in `DirectSandboxService`, in the agent's process

| State | Defined | Kind |
|---|---|---|
| `_capacity_guard` / `_admission_changed` (Lock + Condition) | `direct_service.py:349-350` | the node-wide admission lock and its wakeups |
| `_transitions: TransitionLedger` | `:326`; class `transition_admission.py:55-228` | claims, waits, growth forecasts |
| `_startup_slots = FairCapacity(max_concurrent_startups)` | `:327` | FIFO create slots |
| `_restore_slots = FairCapacity(max_concurrent_restores)` | `:325` | FIFO restore slots |
| `_management_read_slots = FairCapacity(max_concurrent_startups)` | `:328` | runsc `ctl` concurrency (legacy managed control) |
| `_publication_slots = FairCapacity(16)` | `:373` | publication lanes |
| `_startup_admission_state` (thread-local: `admitted`, `deadline`) | `:334`, `:2632-2653` | reentrancy, and the deadline shared with the capacity wait |
| `_active_capacity`, `_runtime_metrics_provider` | `:336-339`, `:390-405` | shape and pressure source; `None` disables pressure checks (`:2898-2913`) |
| `_admission_open` | `:353`, `:2539-2555` | the drain gate |
| `_activity_epoch` | `:348`, `:3035`, `:3041`, `:3074`, `:3080`, `:3102` | the transient addend of the heartbeat activity clock |
| `_active_exec_reservations` | `:340-342`, `:3073`, `:3079` | zero-resource exec leases (drain fence) |
| `_growth_intents` (cache of durable rows, loaded at `__init__`) | `:377-380` | managed growth phases |
| `_provisional_growth` | `:383`, `:2026`, `:2035` | admitted growth whose commit is in flight |
| `_growth_credit_expires` | `:386`, `:1826-1828`, `:1837`, `:1844` | forecast recompute deadline |
| `_growth_turns` / `_growth_turns_guard` | `:387-388`, `:1847-1870` | per-key ordering of durable plus in-memory growth |
| `_resident_memory: ResidentMemorySampler`, `_resident_cgroup_paths` | `:329-331`; class `resident_memory.py:52-205` | observations behind growth credit |
| `_locks` / `_held_lifecycle` | `:335`, `:351-352`, `:3120-3213` | per-(id, gen) lifecycle locks; `_unlocked_for_admission` `:3156-3168` |
| Constants | `:86-93` | `_CPU_ADMISSION_RETRY_SECONDS=0.21`, `_GROWTH_OBSERVATION_MAX_AGE_SECONDS=30`; `admission_wait_seconds=30` (`:333`); `PHYSICAL_MEMORY_FLOOR_MB=2048` (`resource_admission.py:11`); `PREFETCH_MAX_BYTES=1 GiB` (`pause_tier.py:61`) |

### 1.2 Durable state, in the registry (one file, noded the flock owner)

- **`managed_growth`** (`direct_registry.py:505-513`).
  - Python writer: `growth_intent` / `growth_intent_batch` / `_growth_intent` (`:1359-1470`). Reader: `growth_intents()` (`:1354-1357`).
  - Deleted with the registration: `commit_deleted` (`:1308-1309`).
  - Rust twin: `registry/mod.rs:313-317`, `:741`, `:993-1029`, `:1191-1316`.
- **`relay_wake_fences`** (`:499-504`): insert-only.
  - Writers: `relay_wake_fence(record=True)` (`:1317-1352`), and `_growth_intent(action="activate", request_id)` (`:1461-1466`).
  - Rust twin: `mod.rs:967-989`, `:1302-1305`.
- **The drain row** (`registry_metadata.drain_json`, `:482`). Python writes it (`save_drain`, `:964`; `node_runtime.py:1365`, `:1379`). noded's exec front reads it (`runtime/noded/src/exec_front.rs:244-246`).
- **Disk claims** (`registration_disk`, `workspace_capacity`, `reflink_overlaps`). Already one durable authority, enforced inside registry transactions (`DirectRegistryCapacityUnavailable`). **Not part of 3c**; this is C5.3's "project-quota sum", already single-owner.

### 1.3 Cross-process state today

| Item | Writer | Reader |
|---|---|---|
| `<state_root>/noded/agent-demand.json` `{seq, admission_open, physical_bytes, ram_backing_bytes}` | agent `DemandPublisher` (`pause_handoff.py:57-110`, from `reclaim_demand` `direct_service.py:2713-2722`) | noded `AgentDemandSource` (`pause_policy/files.rs:142-201`, wired `pause_policy/mod.rs:282-283`) |
| `<state_root>/noded/status.json` | noded (`files.rs:65-115`) | agent heartbeat (`node_runtime.py:596-607`) |
| `POST /internal/v1/growth/events` (wait/activate batches every 50 ms; failures dropped) | noded `send_growth` (`pause_runtime.rs:80-99`; event `local_wait/scheduler.rs:74-96`) | agent `record_growth_events` → `record_local_wait_growth` (`node_agent.py:674-677`; `node_runtime.py:548-566`; `direct_service.py:1916-1942`) |
| `POST /internal/v1/creates/admit` / `{token}/finish` | noded `create.rs:254`, `:321-337` | agent `CreateHandoff` (`create_handoff.py:81-212`; routes `node_agent.py:699-760`) |
| `GET /internal/v1/creates/config` (with digest) | agent `create_config` (`create_handoff.py:215-302`) | noded `pipeline.rs:166` |
| Memory placement mode (RAM→file) | Python `MemoryBackingStore._remember_mode` (`memory_backing.py:346-375`) | per-process caches; Rust `memory_backing.rs:1085-1087`. Python's `observe_allocation` (`:356-363`) re-reads the journal |

### 1.4 Readers and writers by caller

**Create.**
- *Python path* (forwarded creates: relay egress, DNS-named egress, unported image kinds; `rust-node-daemon-plan.md:205-213`):
  - `create` → `create_admission` (`direct_service.py:686-728`);
  - the order is `_startup_demand` (ledger wait) → `startup_admission` (startup slot, `_admission_open`) → `_reserve_active_capacity` (guard, then a claim with `requested`; activity epoch +1) → `_request_lock`;
  - the rollback is `roll_back_capacity_rejection` (`:730-739`).
- *noded path*:
  - `create.rs:254` admit → `CreateHandoff._hold` (`create_handoff.py:189-212`) holds `create_admission` on its own thread;
  - finish (`:119-157`) does a fresh registry read, `adopt_created` and `created_record` (`direct_service.py:741-745`), then releases;
  - session release is in `observe_session` (`:66-79`); expiry is 600 s (`:32`, `:194-199`).
- **Costs.** A managed create costs 0 (`_startup_cost`, `:2586-2599`). Other creates cost the requested bound.

**Managed launch, bind and terminal.**
- `start_managed_process` (`direct_service.py:1690-1741`):
  - records `launch` (`:1714-1716`);
  - if the intent is queued, runs `_admit_managed_growth(startup=True, launch=…)` (`:1717-1732`);
  - then dispatches;
  - records `bind` (`:1736-1739`) and `_observe_managed_terminal` (`:1740`, `:1944-1949`).
- Status and log reads also observe terminal through `managed_process_status` / `logs` (`:2060-2110`).
- Under the in-flight design, noded calls agent `launch`/`bind`/`terminal` instead (`managed-process-reads.md` §"Python agent: /internal/v1/managed/…").

**The one growth function.** `_admit_managed_growth` (`:1982-2037`):
1. Takes the per-key turn.
2. Runs the covered check under the guard (`:2001-2005`); a covered continuation records its wake fence (`:2010`).
3. Registers ledger demand (`:2018`).
4. Runs the guard with `transition_cost_provider` (requote, `:2019-2022`).
5. Charges a provisional claim before releasing the guard (`:2026-2027`).
6. Makes the durable `activate` (`:2030-2032`).
7. Clears the provisional claim (`:2033-2037`).

Costs come from `_growth_sample` / `_growth_remaining` / `_continuation_growth` / `_growth_cost` (`:1743-1818`).

**Local wait and activate.**
- noded local waits send growth events.
- The agent applies them in one batch with per-key turns in key order (`direct_service.py:1916-1942`). Wait is `observe_managed_wait` (`:1898-1903`); activate is `resume_managed_continuation` (`:1905-1914`), which is **not admission-gated**.
- The agent's own (non-Rust) local-wait path queues the same events (`node_runtime.py:506-546`).

**Relay continuation, through the agent's wake route.**
1. `wake_with_activity_revision` (`node_runtime.py:1128-1189`) calls `admit_managed_continuation` **before** the lifecycle exclusive (`:1146-1153`).
2. `admit_managed_continuation` (`direct_service.py:1951-1980`) first runs a `_try_lock` probe and `prepare_restore_memory` for parked checkpoints (`:1965-1973`), then admits growth with `startup=False`.
3. A capacity failure becomes `SandboxStartupBusyError` (`:1976-1980`).
4. The relay wake fence is recorded at `node_runtime.py:1171-1174`.

**Park and capture.**
- `park` (`direct_service.py:1003-1087`) observes capture as an unknown-cost claim (`:1071-1074`, `_observe_transition` `:2560-2569`).
- `_observe_managed_park` (`:1891-1896`) forgets samples and records `park`. It is called from `start()` for recovered parked records (`:410-414`), from the parked early return (`:1025`) and after capture (`:1081`).
- A relay park records the wait (`node_runtime.py:966-969`). Escalation (`node_runtime.py:733-745`) goes through the same park.

**Wake and restore.**
- `_restore_admission` (`direct_service.py:2667-2698`):
  1. calls `prepare_restore_memory` (`:2675`);
  2. quotes `_restore_cost` (`:2601-2618`, from authenticated checkpoint metadata);
  3. registers demand, then releases the lifecycle lock (`_unlocked_for_admission`, `:2686`) while it takes the restore slot and runs `_reserve_active_capacity`;
  4. revalidates the owner and the hibernation generation (`:2694-2697`).
- It is called from `wake` (`:1232`) and from `ensure_running_with_timings` (`:2831`; the exec/file path for parked sandboxes).
- After restore, the managed `activate` is recorded at `:2791-2793`, and samples are forgotten at `:2751`.

**Publication.** `_start_storage_publication` (`:1126-1196`):
- skips when a foreground transition waits and a publication is active (`:1140-1145`);
- takes a non-blocking publication slot (`:1146`);
- claims an unknown cost (`:1148-1150`) and releases it in `finally` (`:1173-1177`, `:1191-1195`).

Callers: `park(background)`, `request_storage_publication` (`:1105-1117`), and `node_agent.py:1045`. `publish_parked` (`:1314-1359`) takes the lifecycle lock only and has no ledger claim.

**Migration import.**
- `stage_storage_native_import`, `activate_import`, `abort_import`, `finalize_moved_source` and `prepare_storage_native_move` (`:1268-1298`, `:1537-1613`) take lifecycle locks only, with **no memory admission**.
- Imported managed primaries get `job_id=""` through the wait/activate paths (`direct_registry.py:1444-1452`), and the first wake goes through restore admission.

**Delete and drain.**
- `try_delete` (`:784-811`) and `delete` (`:813-861`) cancel restore and startup waiters for the owner, then `_forget_growth` (`:1883-1889`).
- `close_admission` (`:2539-2546`) cancels all startup and restore waiters.
- Drain is driven from `node_runtime.configure_drain` (`:1347-1384`). Boot calls `open_admission` or `close_admission` (`:395-401`).
- Image operations check `admission_open` (`:1386-1398`).
- Stale intents are pruned in `refresh_resident_memory` (`direct_service.py:2255-2258`).

**Heartbeat and snapshot.**
- `_heartbeat_snapshot_locked` (`node_runtime.py:1416-1500`) reads `activity_snapshot()` (transient) **before** `inventory_snapshot()` (durable) (`:1420-1421`). It adds `resource_reservations` for owners not yet registered (`:1479-1488`) and composes the activity revision (`:1489-1492`).
- `activity_snapshot` (`direct_service.py:3082-3097`): `active_sandbox_creates = len(resource_reservations)` counts restores too.
- Resident-wait metrics: `resident_demand_snapshot` (`:2735-2742`) via `node_runtime.py:596-607` and `node_agent.py:1773-1783`.
- Drain readiness is checked in `NodeDrainSnapshot.ready` (`sandbox.py:884-891`); the activity clock is `durable + transient` (`sandbox.py:866-873`).

**Resident sampler.**
- `refresh_resident_memory` (`direct_service.py:2249-2282`) samples owned, parkable, managed sandboxes (and paused ones unless `rust_pause_tier`) every 1 s from `_relay_parking_loop` (`node_runtime.py:609-625`), then refreshes every forecast.
- Other readers: `reclaim_resident_wait` (`:929-1001`), `park_with_activity_revision` (`node_runtime.py:982`), `_recheck_relay_parks` (`:758-760`), and the calibration sample (`node_agent.py:1090`).
- noded samples only paused sandboxes (`pause_policy/resident.rs:1-4`).

**Execs (not ledger state).**
- `acquire_exec_capacity` (`direct_service.py:3044-3075`) checks the floor, the owner and drain, with a zero-resource lease. `_file_exec` retries on `_admission_changed` (`:2499-2537`).
- noded's own execs check the drain row and the memory floor (`exec_front.rs:244-246`, `:274-275`).

---

## 2. Invariants that must survive, where they are enforced, and the tests that encode them

| # | Invariant | Enforced today | Tests |
|---|---|---|---|
| I1 | FIFO slots: no barging; a grant that races a timeout is never lost; an interrupt returns a granted slot; cancel by owner never revokes a granted slot; a weighted head that times out lets a fitting follower through | `admission.py:44-106` | `test_fair_capacity.py:11,42,55,70`; `test_startup_admission.py:31`; `test_transition_admission.py:130` |
| I2 | Each claim has an exact token lifetime; duplicate owners get independent tokens; release of a peer's token fails | `transition_admission.py:72-97` | `test_transition_admission.py:102,118` |
| I3 | Per owner, the charge is the max of its forecast and its claims, never the sum (no double charge across overlapping operations) | `_total` `:121-131` | `:118`, `:78`; `test_startup_admission.py:295,344`; `test_managed_growth.py:140` |
| I4 | Concurrent admissions cannot spend the same headroom; the 2 GiB physical floor holds; free swap never satisfies the floor | `direct_service.py:2943-2955`; `resource_admission.py:161-162` | `test_transition_admission.py:313,392` |
| I5 | RAM backing is projected separately; configured backing with absent or unknown evidence counts as 0 available (queue, never unlimited); file-backed restores spend physical bytes but no tmpfs | `direct_service.py:2960-2968`; `resource_evidence.py:64-101` | `test_transition_admission.py:247,339,372`; `test_managed_growth.py:427` |
| I6 | A startup may spend only what every queued restore leaves (running rollouts first) | `transition_admission.py:137-148` | `test_transition_admission.py:60,206,232` |
| I7 | Reclaim bills admitted work plus only the **next** eligible FIFO owner; closed admission bills nothing | `:166-179`; `direct_service.py:2700-2733`; `files.rs:180-201` | `test_transition_admission.py:39`; `test_startup_admission.py:259,359`; `test_rust_pause_tier.py:332` |
| I8 | Memory waits keep their demand visible until headroom returns, within the operation deadline; no lifecycle lock is held while waiting; delete and drain can interrupt | `direct_service.py:2977-3003`, `:2686` | `test_transition_admission.py:267,300,412,444,475` |
| I9 | Restore revalidates generation and hibernation generation after the queue; two queued wakes restore once; a changed checkpoint is requoted | `:2694-2697`, `:2744-2750` | `test_transition_admission.py:498,664`; `test_hybrid_memory_admission.py:73-148` |
| I10 | Only pre-dispatch failures are recast as safe retries (startup-busy, restore-busy); dispatch and restore failures stay ambiguous | `:1717-1733`, `:2773-2790`, `:1974-1980` | `test_transition_admission.py:180,191`; `test_managed_start_retry.py:38,58`; `test_managed_growth.py:587,607` |
| I11 | One immutable primary per generation: a queued launch can be replaced, an active one never; replacement fences the old dispatch; imported (unknown) primaries bind only through the supervisor | `direct_registry.py:1404-1424`; `direct_service.py:1730-1732` | `test_managed_start_retry.py:71,88,112,131`; `test_managed_growth.py:283,501` |
| I12 | Launches carry the whole bound until their first safe wait; a wait suspends growth; a continuation is forecast to its peak (current excluded; RAM credits only shmem; swap owes only the prefetch); observations count for 30 s; park and restore forget them; a file-backed continuation is unknown, not 0 | `direct_service.py:1743-1818` | `test_managed_growth.py:86,95,237,319,345,362,383,408` |
| I13 | A continuation's grant and its wake fence commit together; a late safe wait cannot erase the grant; while it queues, the wait stays reclaimable | `direct_registry.py:1440-1443`, `:1461-1466`; `direct_service.py:2000-2011` | `test_managed_growth.py:204,227,480,294,119` |
| I14 | Durable growth commits never hold the node-wide guard; per-key turns order durable before in-memory changes; admitted bytes stay charged while the commit is in flight | `:1847-1881`, `:2026-2037` | `test_managed_growth.py:152,171` |
| I15 | Matching terminal or delete retires only the matching generation; delete waits out an in-flight commit; drain cancels undispatched growth but keeps active launches | `:1883-1889`, `:805-807` | `test_managed_growth.py:395,443,489` |
| I16 | Capture is observed, not charged and not queued; publication keeps one lane during foreground bursts | `:1071-1074`, `:1140-1145` | `test_transition_admission.py:521,544,688` |
| I17 | Managed restore reserves max(allocation, bound); a crossed capture requotes the full bound | `:2601-2618` | `test_managed_growth.py:455,518`; `test_transition_admission.py:535-541` |
| I18 | Drain: once close returns, every admitted create is in the transient snapshot or finished; the heartbeat reads transient before durable; the noded create claim is released only after the row is visible to the heartbeat's reader | `:2539-2546`; `node_runtime.py:1417-1421`; `create_handoff.py:125-129` | `tests/test_local_fleet_drain.py:35,74`; `test_rust_create_handoff.py:179,222,250`; `test_exec_admission_contract.py:73` |
| I19 | The activity clock is monotonic across process restarts (CLOCK_MONOTONIC base) and composes as durable + transient | `direct_service.py:343-348`; `sandbox.py:866-873` | `test_rust_create_handoff.py:179` (epochs) |
| I20 | Durable rows survive restart; ambiguous launches rehydrate; parked managed records are re-observed at start from canonical Warden results | `direct_service.py:377-380`, `:410-414` | `test_managed_growth.py:283,620` |
| I21 | v3/v4→v9 migration keeps registrations; legacy schemas are validated | `direct_registry.py:2287-2335`; `schema.rs:217-285` | `test_managed_growth.py:636`; `test_direct_registry.py:624`; Rust `registry/tests.rs` |
| I22 | Growth batches commit together and fail alone | `direct_registry.py:1373-1391`; `mod.rs:1009-1029` | `test_managed_growth.py:102`; `test_rust_pause_tier.py:290`; `registry/tests.rs:490` |
| I23 | Python and Rust stay byte-compatible on growth rows and fences in both directions | `registry_python_compat.rs` | `:271`, `:381`, `:488` |
| I24 | The growth forecast refresh is O(in-flight) per admission and O(sandboxes) once per sampler pass | `transition_admission.py:66-69`; `direct_service.py:1831-1845` | `test_transition_admission.py:78`; `test_managed_growth.py:383` |
| I25 | Wire contract: refusal bodies and status codes are byte-identical (`node_admission_closed`, `node_active_admission_deferred` for creates, `node_startup_busy`, `node_restore_busy` with `lifecycle_state:"parked"`, `primary_already_owned`) | `node_agent.py:632-649`, `:868-880`, `:1919-1950` | `test_rust_create_handoff.py:222`; `test_managed_start_retry.py:131,183-226` |

---

## 3. Target boundary

### 3.1 What noded owns after 3c

One module: `runtime/noded/src/admission/`. It contains:

- **`ledger.rs`**: an exact port of `TransitionLedger` (`transition_admission.py:55-228`), with running growth totals.
- **`fair.rs`**: `FairCapacity` on tokio.
  - It is a weighted FIFO with `cancel(owner)`.
  - It handles the grant/timeout race explicitly: a granted ticket whose waiter's future was dropped returns its weight, which covers a hyper client disconnect.
- **Pools:** startup (`max_concurrent_startups`), restore (`max_concurrent_restores`) and publication (16).
- **`growth.rs`**: the cost functions `_growth_sample`, `_growth_remaining`, `_continuation_growth`, `_growth_cost`, `_startup_cost` and `_transition_memory_cost`; the in-memory intents and provisional claims; credit expiry; per-key turns as a `tokio::sync::Mutex` map.
  - Durable writes go through the existing `Registry::growth_intent` / `growth_intent_batch` (`registry/mod.rs:993-1029`), so they join noded's group commit.
- **`decide.rs`**: the one admission function, a pure decision plus an async driver:

  ```rust
  pub struct Request {
      owner: (String, u64),
      kind: Kind,                // Startup | Restore | Growth | Capture | Publication
      requested: Shape,          // vcpu, memory_mb (shape check)
      cost: Cost,                // memory_bytes: Option<u64>, ram_backing_bytes, read/write_bytes, provenance
      requote: Option<Requote>,  // growth only (transition_cost_provider)
      checks: Checks,            // shape, memory_psi; CPU removed (see §0.9)
      slot: Option<Pool>,
      deadline: Instant,
      validate_owner: Validate,  // registry owner/generation/phase
      holder: Holder,            // Noded | Agent { session }
  }
  pub fn decide(m: &Metrics, l: &Ledger, r: &Request) -> Decision; // Grant | Wait(reason) | Refuse(reason)
  pub async fn admit(&self, r: Request) -> Result<Grant, Refusal>;  // Closed | Busy(kind) | Capacity(msg) | Conflict
  ```

  - `decide` is a line-for-line port of `direct_service.py:2895-3003` minus the CPU branches. The rules it keeps:
    - `capacity == None` means slot-only;
    - `metrics == None` refuses at once;
    - `memory_total == 0` is a projected wait;
    - the retry deadline holds after a late sample.
- **Metrics:** extend `pause_policy/pressure.rs` with MemTotal and memory PSI **full** avg10. Today it reads only "some" (`pressure.rs:179`).
  - Admission uses a 200 ms single-flight cache, which matches `runtime_metrics.py:25`.
  - RAM-backing evidence reuses `sample_memory_backing` (`pressure.rs:116`).
- **Resident observations of managed candidates** (3b): owned, parkable, managed sandboxes every 1 s. This extends `pause_policy/resident.rs` beyond paused sandboxes, and the forecast refresh follows each pass (as `direct_service.py:2281-2282` does).
- **Drain gate:** `admission_open`, with the drain row as truth. The agent stays the drain *writer* (§3.3), and noded's close is synchronous.
- **The transient activity counter:** CLOCK_MONOTONIC ns at start, +1 per claim and per release (as `:3035`, `:3041`).
- **Demand for the pause policy:** a `LedgerDemandSource` implementing `DemandSource` (`files.rs:176-178`) replaces `AgentDemandSource`, and `agent-demand.json` is retired.
- **Local-wait growth:** `LocalWaitEvents::growth` (`scheduler.rs:97-102`) applies to the in-process ledger through a batched writer (same 50 ms coalescing, same per-item errors). `send_growth` (`pause_runtime.rs:80-99`) and its "dropped batch" risk go away.
- **Managed launch, bind, terminal and continuation admission** become in-process calls from noded's job routes. The `/internal/v1/managed/*` routes from `managed-process-reads.md` disappear (see decision C).

### 3.2 What stays in the Python agent

- The lifecycle locks (`_locks`, `_unlocked_for_admission`) and `DirectLifecycle`. These belong to phase 4.
- **Restore cost quoting.** It needs authenticated checkpoint metadata (`warden.artifacts.load_published_metadata`, `:2608-2611`) and `prepare_restore_memory` (`direct_warden.py:917-927`).
- `_management_read_slots`, used only by legacy `_managed_control` (`:2154`). These are deleted with the legacy handlers.
- Exec leases (`_active_exec_reservations`) and `_file_exec` retries (local; no ledger).
- Heartbeat assembly and sending. It now composes noded's transient snapshot.
- Park, wake, publication and migration execution. Each requests or reports to noded as listed in §3.3.

### 3.3 RPC surface: agent → noded

**Transport.** A new noded UDS listener, `--internal-unix <state_root>/noded/noded.sock`:
- mode 0600, the existing bearer token, plus a `SO_PEERCRED` uid check;
- the same pooled HTTP/1 framing as `agent_rpc.rs`;
- the TCP port is not used (it is externally reachable).

**Sessions.** Every agent request carries `X-UCloud-Agent-Session: <uuid per agent start>`. Every noded response carries `X-UCloud-Noded-Session` (the existing `SESSION_HEADER`, `agent_rpc.rs:21`).

**Owner and errors.** All bodies are JSON. `owner = {"sandbox_id", "generation"}`. Errors are `{"error", "error_code", "retryable"}`.

| Route | Request | Response | Semantics |
|---|---|---|---|
| `POST /internal/v1/admission/hello` | `{agent_session}` | `{noded_session, admission_open, config_sha256}` | Called at agent start and whenever the agent sees a new noded session. noded releases every claim and cancels every waiter held by any other agent session. |
| `POST /internal/v1/admission/startup` (blocking) | `{owner, requested:{vcpu,memory_mb}, managed:bool, wait_seconds:null\|f64}` | `200 {token, phases:{startup_admission_ms, active_capacity_ms}}`; `503 node_admission_closed`; `503 node_active_admission_deferred` | Only for creates the agent still runs (forwarded kinds). Same order as `create_admission`: demand, slot, capacity. The agent then takes `_request_lock` itself. |
| `POST /internal/v1/admission/restore` (blocking) | `{owner, requested, cost:{memory_bytes,ram_backing_bytes,read_bytes,provenance}, deadline_ms}` | `200 {token}`; `503 node_restore_busy`; `503 node_admission_closed`; `409 generation_mismatch` | Called inside `_unlocked_for_admission`. The agent revalidates generation and checkpoint after `200` and releases on any failure. |
| `POST /internal/v1/admission/observe` | `{owner, kind:"capture", cost:{memory_bytes:null, provenance}}` | `200 {token}` | Never waits or refuses (I16). |
| `POST /internal/v1/admission/publication` | `{owner}` | `200 {token}` or `409 publication_deferred` | Non-blocking. Applies the `foreground_waiting && active publication` gate and the 16-lane pool atomically. |
| `POST /internal/v1/admission/{token}/release` | `{}` | `200 {}`; `404 admission_token_unknown` | Idempotent. A 404 after a noded restart is success for the caller. |
| `POST /internal/v1/admission/cancel` | `{owner}` | `200 {cancelled}` | Delete and try_delete call it (replacing `:805-807`, `:856-858`). Wakes queued waiters for that owner. |
| `POST /internal/v1/admission/close` / `open` | `{drain_token}` | `200 {admission_open, transient_revision}` | Synchronous. On return of `close`, every noded-admitted create and restore is in the snapshot or finished (I18). The agent calls it inside `_drain_guard`, before `save_drain` (`node_runtime.py:1362-1371`). |
| `GET /internal/v1/admission/snapshot` | none | `{noded_session, admission_open, transient_revision, resource_reservations:[{owner, vcpu, memory_mb}], active_reservations, transitions:{…snapshot()}, resident_demand:{…demand_snapshot()}}` | Read first in `_heartbeat_snapshot_locked`, replacing `activity_snapshot()` at `:1420` for the ledger parts. Heartbeat transient reservations = this list, filtered by registered keys as today (`:1479-1488`). |
| `POST /internal/v1/growth/events` (reversed direction) | `{items:[{action:"park"\|"wait"\|"activate"\|"terminal"\|"forget", owner, request_id?, job_id?}]}` | `{results:[{intent}\|{error, error_code}]}` | Python's remaining writers: park observations (`:414`, `:1025`, `:1081`), relay wait (`node_runtime.py:968`), restore activate (`:2791-2793`), legacy terminal (`:1740`, `:1944-1949`) and forget on delete (`:807`, `:858`). One batch commit with per-key turns. |
| `POST /internal/v1/growth/continuation` (blocking) | `{owner, request_id, deadline_ms}` | `200 {intent}`; `503 node_startup_busy`; `409 generation_mismatch` | Replaces `_admit_managed_growth(startup=False)`. The agent keeps the `_try_lock` and `prepare_restore_memory` probe (`:1965-1973`) and sends this request afterwards. |
| `POST /internal/v1/growth/launch` (blocking; legacy only) | `{owner, job_id, launch_sha256, deadline_ms}` | `200 {job_id}`; `503 node_startup_busy`; `409 primary_already_owned {job_id}` | Only for legacy managed sandboxes, which keep Python start (decision J). Bind goes through `growth/events` with `{action:"bind", job_id, launch_sha256}`. |

**Changed agent route.** `POST /internal/v1/creates/admit` gains an optional `"admitted": {"token": <noded claim>}`. When it is present, the agent skips demand, slot and capacity (`create_admission`) and holds only `_request_lock` plus the layout decisions. Finish is unchanged, and noded releases its claim **after** finish returns (I18; see the fresh read at `create_handoff.py:129`).

**Memory placement.** Before quoting a restore or growth cost, noded re-reads the allocation's `active_mode` for that key from the memory journal (the Rust equivalent of `observe_allocation`, `memory_backing.py:356-363`). Python's RAM→file switch is then never seen stale.

**Configuration.** `GET /internal/v1/creates/config` gains an `admission` block, covered by the digest:
- `max_concurrent_startups` and `max_concurrent_restores`;
- `admission_wait_seconds`;
- `active_capacity`: `{vcpu, memory_mb}` or null;
- `physical_memory_floor_mb`;
- `application_memory_root` (already present);
- `growth_observation_max_age_seconds` (30);
- `prefetch_max_bytes`;
- `publication_lanes` (16);
- `retry_seconds` (0.21).

### 3.4 Idempotency and tokens

- Claim tokens are noded-issued (128-bit hex) and bound to `(holder session, owner, kind)`. A release of an unknown token is a no-op that answers 404, which the agent treats as success.
- Blocking admissions are **not** idempotent by request. A lost response is pre-dispatch by construction, because the agent dispatches only after `200`. The agent maps `RpcError::Lost` to the safe-retry error (`node_restore_busy`, `node_startup_busy`); this is I10.
  - For continuation and launch, a granted-then-lost answer has already committed `activate`. The retry takes the covered path (`:2001-2011`), so it is idempotent through the durable row.
- Growth events are idempotent by row state: the transitions in `growth_intent_step` are fixpoints for a repeated action.
- Fences and launch identity stay in SQLite (I11, I13).

### 3.5 Crash semantics

**noded restarts while the agent runs.**
- noded's own creates die. The agent releases their handoffs on the new session (`create_handoff.py:66-79`; unchanged).
- At start, noded:
  1. loads `growth_intents()`;
  2. prunes rows without an owned registration (as `:2255-2258`);
  3. enters **recovering**: no grants, waiters queue, and `snapshot` answers `503 admission_recovering`.
- Recovery ends when the agent's `hello` from the current agent session arrives with `reassert: [{owner, kind, cost, requested, agent_token}]`. These are all claims the agent believes it holds. Recovery also ends after a 2 s grace with the agent unreachable; then the agent's claims are presumed gone, because the agent is down too.
- Reasserted claims are accepted unconditionally; they are already-running work.
- In-flight blocking waits from the agent see `Lost` and become safe retries.
- Forecasts charge whole bounds until the first sampler pass (conservative; I12).
- The heartbeat **skips its sample** while the snapshot is unavailable (the sender's retry path, `tests/test_heartbeat_sender.py:195`). It never publishes zero reservations.

**The agent restarts while noded runs.**
- The new agent's `hello` makes noded drop every claim and waiter of the old session.
- The new agent's `service.start()` re-observes parked managed records (`:410-414`) through `growth/events` `park`.
- It re-applies the drain row (`node_runtime.py:395-401`) through `close` or `open`.

**Both restart, or the node reboots.** Durable rows are the only state, and both sides rebuild from them.

**Ambiguous launch across a crash.** The row stays `active` until terminal, delete, or the same job and digest being retried (covered). This is unchanged (I11, I20).

### 3.6 Heartbeat transient reservations, concretely

The order inside `_heartbeat_snapshot_locked`:
1. `noded.snapshot()`, which carries the transient reservations and revision.
2. The agent's own exec leases (`_active_exec_reservations`).
3. `inventory_snapshot()` (durable).

The fields then compose as follows:
- `transient_revision = agent_epoch + noded_epoch`. Both are monotonic, so the sum is, and I19 holds.
- `active_sandbox_creates = len(noded.resource_reservations)`.
- `active_operations = noded.active_reservations + agent exec leases`.
- Resident-wait metrics merge `noded.resident_demand` where `resident_demand_snapshot()` is used today (`node_runtime.py:597`).

---

## 4. Phasing, gates and rollback

Each step lands behind `sandbox.direct_node_rust_admission`. The flag:
- requires `front_door`, `rust_create`, `rust_exec` and `rust_pause` (`config.py:307-441` pattern; tests modeled on `test_rust_pause_tier.py:442,472`);
- maps to agent `--rust-admission` and noded `--rust-admission`.

With the flag off, behavior is byte-identical.

**3c.0: prerequisites (separate PRs).**
- (a) **3b, resident sampling in noded** for managed candidates. noded publishes `<state_root>/noded/resident.json` for the agent's remaining readers (`node_runtime.py:669`, `:758-760`, `:982`; `node_agent.py:1090`), and the agent stops `refresh_resident_memory`.
  - Gate: sample parity against Python on one worker, with byte-equal fields on the same cgroup (the `resident.rs` header contract).
- (b) The noded internal UDS listener, the Python `NodedClient`, and both session headers. No routes yet.
- (c) `Pressure` gains MemTotal and PSI full.
- (d) The `admission` config block and digest.

**3c.1: pure Python refactor, then the pure Rust port (no wiring).**
- **Python:** extract `decide()` from `_active_admission_guard` without any behavior change. Add a `hypothesis` property suite, so the C5.3 gate has property tests to carry over (`hypothesis` is in `.venv`). It covers:
  - FairCapacity, through random acquire, release, cancel and timeout traces: FIFO, conservation, no lost grant;
  - TransitionLedger: totals equal a recount; per-owner max; next-owner demand.
- **Rust:** `ledger.rs`, `fair.rs`, `growth.rs` and `decide.rs`, checked against **Python goldens**, generated like `local_wait/testdata/goldens.py`. The goldens are recorded traces from the Python classes and `decide()`.
- Port these as Rust tests:
  - `test_fair_capacity.py`: 4 tests;
  - `TransitionLedgerTests`: 6;
  - the property traces;
  - the growth cost cases of `test_managed_growth.py:319-427`.
- **Gate:** golden equality on every trace; `cargo test`; the Python suite unchanged.

**3c.2: the noded admission service (dead behind the flag).**
- Build the in-process API, the §3.3 routes, the recovering state, `LedgerDemandSource`, in-process local-wait growth, and in-process managed launch/continuation behind an `AdmissionPort` trait. Its `AgentAdmission` impl is today's RPC; `LocalAdmission` is new.
- Add `ucloud-noded --admission-only --internal-unix P --registry R` for tests: it serves only the admission routes.
- **Gate:** Rust integration tests in the style of `tests/create_front.rs`: session churn, reassert, recovering, close-while-waiting, and a client disconnect that returns a granted slot.

**3c.3: the agent's `AdmissionBackend`.** Move today's code verbatim into `LocalAdmission` and add `NodedAdmission` (RPC). Route every call site in §1.4 through the backend. Heartbeat composition per §3.6. With the flag on, `DemandPublisher` and the agent's growth executor are off.
- **Gate:** the `TransitionServiceTests`, `StartupAdmissionTests`, `test_managed_growth.py`, `test_managed_start_retry.py`, `test_hybrid_memory_admission.py`, `test_local_fleet_drain.py` and `test_rust_create_handoff.py` classes run **parameterized over both backends**. The NodedAdmission runs use a real `--admission-only` noded.
  - Tests that patch `_transitions` (for example `test_managed_growth.py:57`) switch to `snapshot` assertions. Expected: every test name above passes in both modes.

**3c.4: noded creates use in-process admission** plus the agent's `admitted` mode of `creates/admit`.
- **Gate:** `test_rust_create_handoff.py` in both modes, and the drain-during-create test (`test_local_fleet_drain.py:74`) against a real noded.

**3c.5: canary and gates, flag on for one worker, then the fleet.**
1. Hot-swap the binary and run the 64-task smoke (`rust-node-daemon-plan.md:244-249`).
2. **The rc-style pressure trial**, as in `managed-growth-admission.md` and `benchmarks/architecture-load-2026-09-23/rc15-pressure-memory/README.md`:
   - 256 managed primaries, each a 1.5 GiB heap with a 2 GiB limit and 8 natural model-wait cycles, on 4 workers of ~87.8 GiB.
   - Pass requires:
     - every cycle completes;
     - zero guest SIGBUS and zero kernel OOM;
     - no untyped 503 escapes;
     - the minimum physical available never goes below the 2 GiB floor;
     - the minimum RAM-backing available stays above 0;
     - the 2 s backing and physical time series are retained.
   - The rc15 baseline: minimum physical 10.71–13.81 GiB, minimum backing 12.06–15.09 GiB, 172 parks.
3. **The density shape** (density-rc38: ~180 sandboxes per node). The `sandbox.wake.growth_admission` span p95 must be well under 1 s; the failure mode is a 20–30 s response hold.
4. **Warm 1,024 relay rollouts on 3×64-vCPU workers.** 0.9.48 numbers to match or beat (`rust-node-daemon-plan.md:413-432`, `:322-347`):
   - all ready by 30 s (27.6 s);
   - answer→resume p95 ≤ 46 ms (34 ms);
   - relay overhead p95 ≤ ~95 ms;
   - 984/1,024;
   - `startup_admission` p95 ≤ 1.9 s;
   - agent GIL waiters during the burst (bpftrace on `PyEval_RestoreThread`, `:23-31`) below 10–15;
   - `agent_admit` p50 below 64 ms (`:403-408`).
5. Drain a canary worker mid-burst. Drain must not report ready until creates finish (I18).

**3c.6: delete after one release with an idle-fleet apply.** Delete:
- `transition_admission.py`;
- the ledger, pools and growth code in `direct_service.py` (~600 lines: `:325-328`, `:373`, `:377-388`, `:1743-2037`, `:2539-2742`, `:2868-3042`);
- `DemandPublisher` (`pause_handoff.py:57-110`);
- the agent's `growth/events` handler;
- `agent-demand.json`.

`admission.py` **stays**: the gateway uses `FairCapacity` and `FairRLock` (`control_plane.py:57`, `:6307-6312`; `gateway/placement.py:13`, `:40`). So "five modules to two" means the node side ends with `runtime/noded/src/admission/` alone, while the gateway keeps `admission.py`, `wake_admission.py` and `resource_admission.py`.

**Rollback by unit swap.** Turn the flag off, which restarts both units:
- the agent loads `growth_intents()` again (`:377-380`) and restarts `DemandPublisher`;
- noded reads `agent-demand.json` again.

There is no schema change (v9 both ways), so a pre-3c binary rolls back too. In-flight claims are lost exactly as in any agent restart today; the gateway retries pre-dispatch refusals.

**What must move together** (one flag flip, otherwise the ledger is split-brain):
1. The `managed_growth` writer (all actions) **and** the in-memory forecasts. Python loads rows only at `__init__`, so a second writer goes unseen.
2. The ledger claims **and** the startup and restore pools. Creates and wakes must spend one headroom (`rust-node-daemon-plan.md:176-180`).
3. `close`/`open` admission, the heartbeat transient reservations and the transient revision addend. The drain empty-proof depends on all three.
4. Reclaim demand, which must come from the same ledger.
5. The resident observations that the forecasts credit. 3b must land first, or ship in the same flip.

What need *not* move together:
- `relay_wake_fences`: insert-only and transactional, so many writers are safe;
- disk claims: already one SQLite authority;
- exec leases;
- `_management_read_slots`.

---

## 5. Risks, ranked

1. **Drain empty-proof across two processes (data loss on retire).** Transient state comes from noded and durable state from the agent's cached foreign index, which can be ≤1 s stale (`direct_registry.py:465-469`).
   - Mitigation:
     - keep "release after finish" for creates;
     - read transient first;
     - `close` is synchronous;
     - the snapshot carries `noded_session`, and a session change between snapshot and inventory aborts that heartbeat;
     - a test drives a drain against a real noded mid-create.
2. **Over-admission after a noded restart.** Mitigated by the recovering gate plus reassert (§3.5). A 2 s grace is an open decision (F).
3. **Drift in the decision function lets SIGBUS back in** (the rc12 failure). Mitigation: Python goldens, property traces, both-backend service tests, and the pressure trial as a hard gate.
4. **Agent threads blocked in long admission RPCs** (up to 30 s each for restores and continuations). Same thread count as today's Condition waits, but HTTP adds connection pressure and `http_request_capacity_exhausted` (`create.rs:325-329`). Mitigation: bounded deadlines; noded cancels on disconnect and returns granted slots.
5. **Stale placement-mode caches across processes** (RAM→file) over-charge continuations and stall them. Mitigation: re-read the journal per key before quoting (§3.3).
6. **Missing or stale managed samples** (3b not landed, or a slow pass) charge whole bounds, which brings back the 20–30 s holds (`managed-growth-admission.md` density note). Mitigation: 3b first; credit-expiry semantics as in `:1820-1845`; the density gate.
7. **The payoff for creates is small.** 0.9.49 measured 3c's create saving at ~5% of slot time (`rust-node-daemon-plan.md:403-408`), and finish stays (decision A). The real value is:
   - one ledger, for correctness;
   - no managed launch or growth RPCs;
   - no `agent-demand.json` lag;
   - less GIL load (growth batches, the demand publisher, the sampler, forecast refresh).
   - Rehearsals should be judged on those, not on create p50.
8. **Byte compatibility of refusal bodies and timing phases** (`startup_admission`, `active_capacity` in `phase_timings`). Rehearsal comparability depends on them. Mitigation: golden response tests.
9. **Scope confusion with C5.3's "wake_admission.py duplicates".** That is gateway work; leave it out of 3c and record it as a follow-up.
10. **Legacy managed sandboxes** keep a Python start path, plus one more RPC (`growth/launch`) until they age out.

## 6. Open decisions

- **A.** The lifecycle lock for noded creates. Recommended: keep the agent's `creates/admit` as lock plus layout ("admitted" mode) in 3c. The alternative is the T flock (`exec_fence.py:9-12`), which belongs to phase 4 with `adopt_created` and `note_image_used`.
- **B.** The agent → noded transport. Recommended: a new UDS with peer-cred. The rejected alternative is admitting at noded's front door before forwarding wakes: it cannot quote restore cost or see running-vs-parked, and internal wakes (`ensure_running_with_timings`, `:2807-2866`) never pass the front door.
- **C.** Ship `/internal/v1/managed/{launch,bind,terminal}` with 0.9.72 (`managed-process-reads.md`) behind noded's `AdmissionPort` trait, then swap to `LocalAdmission` in 3c. Recommended over delaying reads.
- **D.** Ship 3b separately, first. Recommended.
- **E.** Python quotes restore cost until phase 4 moves wake. Recommended.
- **F.** The recovering grace (2 s) and whether a recovering noded forwards creates to the agent or refuses them.
- **G.** Drop CPU from node admission formally. All callers already pass `check_cpu=False`; confirm with the owners and delete `dynamic_cpu_pressure_retryable` from the node path.
- **H.** Require `rust_pause` and `rust_exec`. Recommended: this excludes the warm-park policy path (`node_runtime.py:384-387`, `:986-1000`) from 3c.
- **I.** Shadow mode, where the agent mirrors the ledger to noded and noded diffs state at heartbeat. It is useful but adds GIL load. Recommended only for one canary run, comparing state rather than decisions.
- **J.** Delete legacy managed handlers before 3c. That removes `growth/launch` and `_management_read_slots` from scope.

### Critical files for implementation
- `/home/alex-admin/ucloud-sandboxes/ucloud_sandboxes/direct_service.py`
- `/home/alex-admin/ucloud-sandboxes/ucloud_sandboxes/transition_admission.py`
- `/home/alex-admin/ucloud-sandboxes/ucloud_sandboxes/node_runtime.py`
- `/home/alex-admin/ucloud-sandboxes/runtime/noded/src/registry/mod.rs`
- `/home/alex-admin/ucloud-sandboxes/runtime/noded/src/pause_runtime.rs`, with `/home/alex-admin/ucloud-sandboxes/runtime/noded/src/create.rs` and `/home/alex-admin/ucloud-sandboxes/ucloud_sandboxes/create_handoff.py`

## 7. Decisions (2026-10-11)

The rule is to take the simpler option wherever the choice is ours.

- **A:** Keep the agent's `creates/admit` as lock plus layout ("admitted" mode).
  The T flock waits for phase 4.
- **B:** A new noded UDS with a peer-cred check.
- **C:** 0.9.72 ships `/internal/v1/managed/{launch,bind,terminal}` behind noded's
  `AdmissionPort`, and 3c swaps in `LocalAdmission`.
- **D:** 3b (resident sampling in noded) lands first, as its own step.
- **E:** Python quotes restore cost until phase 4 moves wake.
- **F:** A recovering noded refuses grants with a retryable 503 until the agent's
  `hello` (reassert) or a 2 s grace with the agent unreachable. It never forwards.
- **G:** CPU leaves node admission. Every caller already passes
  `check_cpu=False`, so delete the dead branch in 3c.1.
- **H:** `--rust-admission` requires `rust_pause` and `rust_exec`.
- **I:** No shadow mode. The Python goldens, the both-backend service tests and
  the pressure trial are the gates.
- **J:** 3c deletes the legacy managed handlers. It ships only after 0.9.72 has
  been applied on an idle fleet, so no legacy managed sandbox can exist. This
  drops `growth/launch` and `_management_read_slots` from scope.


## Progress

**3c.2, the noded admission service, dead behind the flag (2026-10-11).**
Nothing changes with the flag off. With it on, the agent does not call the
service yet (3c.3), so it is not deployable on its own.

- **Service** (`admission/service.rs`): `AdmissionService` holds the ledger
  and `Growth` under one lock; a watch channel is `_admission_changed`. The
  three pools are `FairCapacity`s (lock order: ledger, then pool). Each held
  claim is a 128-bit hex token bound to its holder (`Noded` or an agent
  session), owner and kind, with its demand and slot.
  - `admit` is the guard's driver: the gate under the lock, metrics (200 ms
    single-flight cache), owner validation and the growth requote outside
    it, then `current_forecasts`, `refresh_wait_cost` and `decide`; a `Wait`
    waits on the watch channel. Startup and restore keep their demand until
    release, as `create_admission` and `_restore_admission` do.
  - Growth: the covered check (and a covered continuation's wake fence)
    under the key's turn; demand; the guard with requotes; the provisional
    charge under the lock; the durable `activate` (which commits the fence
    with the grant); then the provisional clear. Every durable write and its
    in-memory publication run in a detached task, so a disconnect cannot
    leave a committed row unpublished or release a turn mid-commit.
  - Quotes re-read the allocation's placement first
    (`MemoryBackingStore::observe_allocation`, §3.3 "Memory placement").
  - The transient revision starts at CLOCK_MONOTONIC ns and moves on claims
    and releases that carry a reservation shape (creates and restores), as
    `_activity_epoch` does; captures and publications do not move it.
  - At start the service loads `growth_intents()`, drops rows whose key has
    no registration in any phase (as `_prune_growth_intents` does), charges
    whole bounds until the first sampler pass, and is recovering.
- **Routes** (`admission/routes.rs`, served by `internal.rs` once the loaded
  node runs admission; `503 admission_recovering` while the node loads, hello
  included; 404 as before without node admission). Errors are
  `{error, error_code, retryable}`, with `Retry-After: 1` on retryable 503s.
  - `startup` `{owner, requested:{vcpu,memory_mb}, managed, wait_seconds?}` →
    `200 {token, phases:{startup_admission_ms, active_capacity_ms}}`;
    `503 node_admission_closed`; `503 node_active_admission_deferred` (a
    capacity refusal, and also a slot wait that ends without a grant:
    Python's `SandboxStartupBusyError`, which creates report this way).
  - `restore` `{owner, requested, cost, deadline_ms}` → `200 {token}`;
    `503 node_restore_busy` (slot or capacity); `503 node_admission_closed`;
    `409 generation_mismatch` (the owner lost its incarnation while queued).
  - `observe` `{owner, kind:"capture", cost}` → `200 {token}`, never queued.
  - `publication` `{owner}` → `200 {token}`; `409 publication_deferred`
    (retryable) when a foreground transition waits and a publication is
    active, or every lane is taken.
  - `{token}/release` `{}` → `200 {}`; `404 admission_token_unknown`, also for
    another session's token.
  - `cancel` `{owner}` → `200 {cancelled}`: the owner's queued slot waiters,
    and a wake for capacity waiters, which revalidate.
  - `close` / `open` `{drain_token}` → `200 {admission_open,
    transient_revision}`; close also cancels every queued startup and
    restore.
  - `snapshot` → `200 {noded_session, admission_open, transient_revision,
    resource_reservations:[{owner, vcpu, memory_mb}], active_reservations,
    transitions:{startup|restore|capture|publication: {active, waiting,
    memory_bytes, read_bytes, write_bytes}}, resident_demand:{…}}`;
    `active_reservations` is the number of reservations
    (`active_sandbox_creates`).
  - `growth/events` `{items:[{action, owner, request_id?, job_id?}]}` (at
    most 4,096; actions `park`, `wait`, `activate`, `terminal`, `forget`) →
    `200 {results:[{intent} | {error, error_code}]}`, `intent` a growth row
    or null. A missing or unowned registration is `sandbox_unavailable`; a
    sandbox that is not managed is `{intent: null}`; a registry refusal is
    `growth_conflict`. One commit, turns in key order; `park` forgets the
    key's samples first; `forget` is in memory only.
  - `growth/continuation` `{owner, request_id, deadline_ms}` → `200 {intent}`
    (null when not managed); `503 node_startup_busy`;
    `503 node_admission_closed`; `409 generation_mismatch`.
  - `deadline_ms` is the wait budget from receipt. Every grant answers
    `503 admission_recovering` while recovering and `503 agent_hello_required`
    to a session without a hello.
  - A grant's body carries it: if the connection drops before the body is
    handed to it, the claim and slot are released; a request dropped while
    queued leaves the queue (hyper drops it on disconnect).
- **Sessions and recovery.** `hello` takes `{agent_session, reassert?}` with
  `reassert: [{owner, kind, cost, requested, agent_token}]` and answers as
  before. A hello under a new session releases every claim of other agent
  sessions; their waiters see the change and give up. Reasserted claims are
  accepted unconditionally, under the agent's tokens, with a slot while the
  pool has one. Recovery ends with any hello, or when the 2 s grace passes
  and the agent's socket does not accept a connection; while it does, the
  hello is awaited (decision F; noded refuses, never forwards).
- **noded's own callers.** `admission/port.rs`: `AdmissionPort` with
  `AgentAdmission` (the agent's `/internal/v1/managed/*`, byte for byte as
  before), `LocalAdmission`, and `NodeAdmission`, which uses the local one
  only once the loaded node runs admission. The job start's launch and bind
  and the terminal queue go through it. Creates still use the agent (3c.4).
- **Pause tier.** With node admission, `LedgerDemandSource` (the ledger, and
  the drain row) is the reclaim demand instead of `agent-demand.json`; local
  waits' growth events apply in process (`apply_local_growth`, 50 ms
  batches, per-item errors logged); after each managed sampler pass the
  service prunes and refreshes its forecasts. The sampler is shared
  (`ResidentSampler::historical` serves growth credit).
- **`--admission-only`** (`admission/standalone.rs`): `--internal-unix`,
  `--registry`, `--node-control-token-file`, optional `--admission-config`
  (the `admission` block, optionally with `application_memory_root`; default
  slots only) and `--metrics-file` (JSON metrics, re-read per sample).
- **Tests:** `runtime/noded/tests/admission_service.rs` (12): recovery by
  hello and by grace, session churn, reassert, close while waiting (slot and
  memory), a disconnected waiter and an undelivered grant, the publication
  gate, cancel and revalidation, snapshot composition, growth events and
  continuation (covered and uncovered, the fence with the grant), the ledger
  as reclaim demand with local-wait growth, in-process launch/bind/terminal,
  and a restart reloading durable intents, also under `--admission-only`.
  Plus `observe_allocation` in `tests/memory_backing.rs` and the sampler
  hook in `pause_policy::tests`.
- **Deviations:**
  - No `growth/launch` and no `bind` event (decision J); noded's own launch
    is in process.
  - The startup route reports a slot timeout as
    `node_active_admission_deferred`, as the table says, though the service
    distinguishes it (`Refusal::Busy`).
  - `agent_hello_required`, `sandbox_unavailable`, `not_managed_process`,
    `admission_unavailable` and the events' `growth_conflict` /
    `registry_unavailable` codes are new.
  - Recovery refuses rather than queues (decision F over §3.5's "waiters
    queue"). Release, cancel, close/open and growth events are served while
    recovering; observe is never refused for recovery.

**3c.0(a), 3b: resident sampling in noded (2026-10-11).** Behind
`sandbox.direct_node_rust_admission`, the flag of 3c.0(b, c, d) below (agent
and noded `--rust-admission`, default off). noded samples managed sandboxes
only when its pause tier runs and the agent's `admission` block says
`rust_admission_enabled` (masked by noded's own flag). The `pause` block only
gains the informational `resident_path`.

- **noded** (`pause_policy`): with `PolicyConfig::resident_sampling`, the 1 s
  pass (`Engine::sample_residents`) samples owned, parkable sandboxes that are
  managed or paused, then replaces `<state_root>/noded/resident.json`
  (`files::ResidentFile`):
  `{seq, session, samples: [{sandbox_id, generation, <every ResidentMemorySample field>}]}`.
  `seq` starts at 1 in each session. Samples are sorted by key, and a sandbox
  the pass cannot measure is absent. `SystemClock::monotonic` is now
  CLOCK_MONOTONIC itself, so `sampled_at` is the agent's `time.monotonic()`.
- **Agent:** `DirectNodeRuntime(rust_admission=True)` swaps the service's
  sampler for `PublishedResidentMemory`, so every reader in §1.4 reads noded's
  observations through the same `get`/`historical` calls:
  - growth (`_growth_sample`);
  - `reclaim_resident_wait`, `park_with_activity_revision` and
    `_recheck_relay_parks` (`resident_memory_sample`);
  - the heartbeat's calibration (`cached_resident_memory_sample`).

  `_relay_parking_loop` polls the file every tick (250 ms).
  `refresh_published_resident_memory` runs the rest of
  `refresh_resident_memory` only when `(session, seq)` moves: it prunes stale
  intents and refreshes the forecasts.
  - Missing data: a missing, invalid or group/world-writable file is no
    observation, and so is an invalid sample. Age fences stay with the
    readers (2.5 s live, 30 s growth credit), so a stopped noded's samples
    age out through `_current_growth_forecasts_locked` and charge whole
    bounds (§5 risk 6).
  - `forget` (park, restore, delete) fences out observations taken before it,
    until a newer pass arrives.
  - The agent's reclaims still measure on demand (`sample()`), and the newer
    of that measurement and noded's wins.
- **Parity gate:** `runtime/noded/tests/resident_python_parity.rs` builds 26
  fake cgroup trees and samples each with noded's sampler and with
  `ResidentMemorySampler`. The trees cover:
  - peak and swap present, absent, zero and negative;
  - tmpfs, values up to `i64::MAX`, whitespace and duplicate counters;
  - negative or missing counters, garbage values, PID reuse;
  - the wrong OCI cgroup, legacy container-ID cgroups, a shared parent,
    `..`, two unified lines, and a symlink out of the root.

  Accepted and refused trees must match, and fields must be equal except for
  the instant of the read. noded writes `resident.json`, and Python's
  `PublishedResidentMemory` must read back noded's samples exactly, with
  `sampled_at` bit-equal. The gate found one divergence, now fixed: Rust
  reported `peak_bytes` 0 for a negative `memory.peak`, where Python reports
  the current charge.
- **Tests:** `tests/test_rust_resident_sampling.py` covers the reader, the
  forecasts following passes, staleness, forget, the flag's validation and
  rendering, and the parking loop's cadence. `pause_policy::tests` covers
  managed sampling and the file.
- **Not done here:** the spec's "one worker" parity run on real cgroups. The
  fake trees use the same reads.

**3c.1, Rust half and property suite (2026-10-11).** Pure ports, nothing wired;
no existing Python source changed.

- `tests/test_admission_properties.py`: `hypothesis` stateful suites (a declared
  dev dependency).
  - FairCapacity: random acquire, release, cancel, timeout and interrupt traces.
    It checks no barging, FIFO grants, weight conservation, no lost grant, that
    an interrupt returns a grant, and that cancel never revokes one. A harness
    gates each waiter's wakeup, so a grant and its waiter's return are separate
    steps and the grant/timeout race is an ordinary trace.
  - TransitionLedger: totals equal a recount, the per-owner max, startup
    projections that leave every queued restore its headroom, next-eligible-owner
    reclaim, and the snapshots.
- `runtime/noded/src/admission/`:
  - `ledger.rs` is an exact `TransitionLedger` port;
  - `fair.rs` is `FairCapacity` on tokio (`enter` / `Waiter::finish`, with a
    dropped `Waiter` or `acquire` future returning its grant);
  - `growth.rs` holds the cost functions, the in-memory intents, provisional
    claims, credit expiry and per-key `Turns`. Durable writes sit behind the
    `GrowthStore` trait, which `Registry` implements by delegation;
  - `decide.rs` is `gate` (the pre-sample section) plus the pure
    `decide(metrics, ledger, request) -> Grant | Wait | Refuse`, without CPU
    (decision G).
- Goldens: `admission/testdata/goldens.py` writes `python_goldens.json` from the
  Python code itself. It covers 8 FairCapacity traces, 8 ledger traces, 600
  growth-cost cases, the startup and transition cost grid, 12 forecast traces,
  the `test_managed_growth.py:319-427` cases, and 800 guard runs.
  - The guard runs drive `_active_admission_guard` on a `DirectSandboxService`
    built without `__init__`, with a fake clock, scripted metrics, sample
    latency and requotes. The Rust tests replay every trace and match exactly.
- The Rust tests also port `test_fair_capacity.py` (4), `TransitionLedgerTests`
  (6) and the growth cases, and add random-trace invariant tests.
- Deviations from §3.1:
  - `Request` carries the node state (`admission_open`, shape, RAM backing
    configured) and the retry state (`now`, `previous_error`), so `decide` stays
    pure. Slot, holder and owner validation belong to the 3c.2 driver.
  - `refresh_wait_cost` (requote) and the aged-forecast refresh also belong to
    the driver, applied before `decide`.

**3c.1, Python half (2026-10-11).** 3c.1 is complete.

- `ucloud_sandboxes/admission_decision.py` holds the pure decision, split as in
  `decide.rs`:
  - `gate(request)` is the pre-sample lock section;
  - `decide(metrics, ledger, request) -> Grant | Wait | Refuse` is the
    post-sample one;
  - `fences(request)` is the drain and shape check that both start with.
  - `AdmissionRequest` carries the same node and retry state as the Rust
    `Request`. `Refuse.error()` gives the exception the guard raises.
- `_active_admission_guard` is now a thin driver around them. It keeps:
  - the locking and sampling outside the lock;
  - owner validation and the requote (`transition_cost_provider`, then
    `refresh_wait_cost`);
  - the aged-forecast refresh and the "metrics provider changed" refusal;
  - the waits on `_admission_changed`.
- Decision G is applied. Removed: the `check_cpu` parameter, the CPU-retry
  loop, the `dynamic_cpu_pressure_retryable` call, and both
  `_CPU_ADMISSION_RETRY_SECONDS` and `_CPU_ADMISSION_DEADLINE_SECONDS`.
  - `_file_exec`'s retry now uses `admission_decision.RETRY_SECONDS` (0.21 s,
    unchanged).
  - A guard called without a deadline (only exec, which carries no cost and so
    never waits) now has a deadline of "now" instead of now + 1 s. Without a
    wait, that deadline was never read.
  - `dynamic_cpu_pressure_retryable` had no caller left and is deleted from
    `resource_admission.py`; the gateway's `check_cpu` stays.
- The goldens' `decide` traces now come from `gate` and `decide` through
  `goldens.py`'s `drive_decide`, the same loop `tests.rs` replays.
  - Every case also runs the guard and asserts the same outcome.
  - The regenerated `python_goldens.json` is byte-identical; no field changed,
    because the old traces already ran with `check_cpu=False`.
  - The Rust replay test is now `admission_decisions_replay_python_decide`.
- `tests/test_admission_decision.py` holds focused unit tests of the pure
  functions.

**Remaining for 3c.1:** extract a pure `decide()` from
`DirectSandboxService._active_admission_guard` in Python and delete the CPU
branch there (decision G). Then point the decide goldens at it instead of the
guard; the traces should not change. Until then, the goldens drive the guard
itself with `check_cpu=False`.

**3c.0 (b), (c) and (d) (2026-10-11).** Plumbing only; no admission behaviour
changed. With the flag off, nothing changes.

- **The flag.** `sandbox.direct_node_rust_admission` (default off) requires
  `front_door`, `rust_create`, `rust_exec` and `rust_pause`, in `config.py` and
  `vm_init`. It renders:
  - agent `--rust-admission`, which `build_direct_node_agent_server` refuses
    without Rust creates, execs and pause tier;
  - noded `--rust-admission --internal-unix <state>/direct-runtime/noded/noded.sock`.
    noded refuses `--rust-admission` without `--rust-create --rust-exec --rust-pause`,
    and either flag without the other.
- **(b) The listener**, in `runtime/noded/src/internal.rs`:
  - `bind` creates the directory 0700 and replaces only a stale *socket*
    (anything else is refused). The socket is 0600.
  - `serve` closes connections from any uid other than noded's own euid
    (`SO_PEERCRED`; the agent runs as the same user) before reading them. It
    checks the bearer token on every request, and every answer carries
    `X-UCloud-Noded-Session`, the same session as `agent_rpc`.
  - Every request must carry `X-UCloud-Agent-Session`.
  - Routes:
    - `POST hello` takes `{agent_session}`, which must name the header's
      session, records it, and answers `{noded_session, admission_open,
      config_sha256}`. `admission_open` comes from the registry's drain row
      (`drain_since`) and the digest from the loaded `CreateConfig`; both are
      null until the node is loaded.
    - `GET ping` answers `{noded_session, hello}`.
    - Any other path under `/internal/v1/admission/` is `404
      admission_route_unknown`, and other paths are `404 not_found`.
  - Errors are `{error, error_code, retryable}`.
  - Releasing the claims of other sessions waits for 3c.2, when there are
    claims.
- **The Python client**, in `ucloud_sandboxes/noded_client.py`:
  - `NodedClient` keeps pooled HTTP/1 connections. Before reusing one, it
    makes a non-blocking peek, the equivalent of hyper's `ready()`; it drops
    connections idle for more than 10 s.
  - It sends both headers and reports a changed noded session through
    `on_session`.
  - Its errors are `NodedUnavailable` (connect failed), `NodedLost` (written,
    no answer, or timed out), `NodedRefused` (`status`, `error_code`,
    `retryable`) and `NodedProtocolError`.
  - `NodedLink` says hello at agent start on a background thread, retrying
    with backoff while noded is down. It says hello again whenever a new noded
    session appears, either in an answer or on noded's own requests to the
    agent (`_check_node_control_authorized`).
  - The timeout applies per socket operation, not to the whole call.
- **(c) Pressure.** `Pressure` gains `memory_total_bytes` (0 when unknown) and
  `memory_full_stall` (`None` when unreadable, as `memory_psi_full_avg10`).
  No existing consumer reads them.
- **(d) Configuration.**
  - `create_handoff.admission_config` adds the `admission` block to
    `GET /internal/v1/creates/config`, read from the live service:
    - `rust_admission_enabled`;
    - the `FairCapacity` capacities of the startup, restore and publication
      pools;
    - `admission_wait_seconds`;
    - `_active_capacity`;
    - `PHYSICAL_MEMORY_FLOOR_MB`;
    - `_GROWTH_OBSERVATION_MAX_AGE_SECONDS`;
    - `PREFETCH_MAX_BYTES`;
    - `_CPU_ADMISSION_RETRY_SECONDS` as `retry_seconds`.
  - `application_memory_root` stays top-level.
  - The block is under `config_sha256`, so the digest changes once.
  - noded parses it into `pipeline::AdmissionConfig` and keeps it, with
    `config_sha256`, on `CreateConfig`. It masks `rust_admission_enabled` with
    its own flag, as it does for `rust_pause_enabled`.
- **Tests:**
  - `runtime/noded/tests/internal_admission.rs` covers the handshake,
    keep-alive, every refusal, the peer-uid close and the socket's modes. It
    also runs the Python `NodedClient` against the real listener, and parses a
    Python `create_config` into `CreateConfig`.
  - There are unit tests in `pressure.rs` and `pipeline.rs`.
  - `tests/test_noded_client.py` covers the client against a fake listener,
    `NodedLink`, the agent's hello with and without the flag, the block and its
    digest, and the flag plumbing.
