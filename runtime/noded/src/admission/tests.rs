//! Node admission: every Python golden (testdata/goldens.py) replayed; the
//! ports of tests/test_fair_capacity.py, `TransitionLedgerTests` in
//! tests/test_transition_admission.py and the growth forecast cases of
//! tests/test_managed_growth.py:319-427; and random traces checking the
//! invariants of tests/test_admission_properties.py on the Rust types.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::Owner;
use super::decide::{Checks, Decision, Metrics, Node, Refusal, Request, decide, gate};
use super::fair::{Entry, FairCapacity, FairError, Waiter};
use super::growth::{
    Growth, MemoryMode, Observations, continuation_growth, growth_cost, growth_remaining, growth_sample, startup_cost,
    transition_memory_cost,
};
use super::ledger::{Claim, Cost, Kind, Ledger, Limits, NotOwned, Resource, Shape};
use crate::pause::prefetch::PREFETCH_MAX_BYTES;
use crate::pause_policy::resident::ResidentSample;
use crate::pause_policy::{MemoryBackingCapacity, MemoryDemand, Pressure, decide_resident_wait};
use crate::registry::GrowthIntent;

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

fn goldens() -> &'static Value {
    static GOLDENS: std::sync::OnceLock<Value> = std::sync::OnceLock::new();
    GOLDENS.get_or_init(|| serde_json::from_str(include_str!("testdata/python_goldens.json")).unwrap())
}

fn owner(value: &Value) -> Owner {
    (value[0].as_str().unwrap().to_string(), value[1].as_u64().unwrap())
}

fn key(name: &str, generation: u64) -> Owner {
    (name.to_string(), generation)
}

fn opt_u64(value: &Value) -> Option<u64> {
    if value.is_null() { None } else { Some(value.as_u64().unwrap()) }
}

fn cost(value: &Value) -> Option<Cost> {
    if value.is_null() {
        return None;
    }
    Some(Cost {
        kind: Kind::parse(value["kind"].as_str().unwrap()).unwrap(),
        memory_bytes: opt_u64(&value["memory_bytes"]),
        read_bytes: opt_u64(&value["read_bytes"]),
        write_bytes: opt_u64(&value["write_bytes"]),
        provenance: value["provenance"].as_str().unwrap().to_string(),
        ram_backing_bytes: opt_u64(&value["ram_backing_bytes"]),
    })
}

fn encode_cost(cost: Option<&Cost>) -> Value {
    match cost {
        None => Value::Null,
        Some(cost) => json!({
            "kind": cost.kind.as_str(), "memory_bytes": cost.memory_bytes, "read_bytes": cost.read_bytes,
            "write_bytes": cost.write_bytes, "provenance": cost.provenance, "ram_backing_bytes": cost.ram_backing_bytes,
        }),
    }
}

fn shape(value: &Value) -> Option<Shape> {
    if value.is_null() {
        return None;
    }
    Some(Shape {
        vcpu: value["vcpu"].as_f64().unwrap(),
        memory_mb: value["memory_mb"].as_u64().unwrap(),
        disk_mb: value["disk_mb"].as_u64().unwrap(),
    })
}

fn limits(value: &Value) -> Vec<(Kind, usize)> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| (Kind::parse(item[0].as_str().unwrap()).unwrap(), item[1].as_u64().unwrap() as usize))
        .collect()
}

fn sample(current: u64, peak: u64, shared: u64, swap: u64, sampled_at: f64) -> ResidentSample {
    ResidentSample {
        current_bytes: current,
        anonymous_bytes: 0,
        file_bytes: current,
        dirty_bytes: 0,
        writeback_bytes: 0,
        refault_file_pages: 0,
        cgroup_path: "/owned".into(),
        cgroup_device: 1,
        cgroup_inode: 2,
        sentry_pid: 10,
        sentry_start_time_ticks: 20,
        sampled_at,
        shared_memory_bytes: shared,
        peak_bytes: peak,
        swap_bytes: swap,
    }
}

fn decode_sample(value: &Value) -> Option<ResidentSample> {
    if value.is_null() {
        return None;
    }
    let field = |name: &str| value[name].as_u64().unwrap();
    Some(sample(
        field("current_bytes"),
        field("peak_bytes"),
        field("shared_memory_bytes"),
        field("swap_bytes"),
        value["sampled_at"].as_f64().unwrap(),
    ))
}

fn mode(value: &Value) -> MemoryMode {
    MemoryMode::parse(value.as_str().unwrap()).unwrap()
}

fn snapshot_json(ledger: &Ledger) -> Value {
    let mut map = serde_json::Map::new();
    for (kind, row) in ledger.snapshot() {
        map.insert(kind.as_str().into(), serde_json::to_value(row).unwrap());
    }
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// Python goldens

#[test]
fn fair_capacity_replays_python_traces() {
    let traces = goldens()["fair"].as_array().unwrap();
    assert!(!traces.is_empty());
    let mut steps = 0;
    for (index, trace) in traces.iter().enumerate() {
        let capacity = FairCapacity::<String>::new(trace["capacity"].as_u64().unwrap() as usize).unwrap();
        let mut waiters: BTreeMap<u64, Waiter<String>> = BTreeMap::new();
        for step in trace["steps"].as_array().unwrap() {
            steps += 1;
            let at = format!("trace {index}, step {step}");
            let owner = step["owner"].as_str().map(str::to_string);
            match step["op"].as_str().unwrap() {
                "acquire" => {
                    let weight = step["weight"].as_u64().unwrap() as usize;
                    let entered = capacity.enter(weight, owner, step["blocking"].as_bool().unwrap());
                    let expected = &step["result"];
                    match (entered, expected["outcome"].as_str().unwrap()) {
                        (Err(FairError::Weight), "invalid") => {}
                        (Ok(Entry::Granted), "granted") | (Ok(Entry::Refused), "refused") => {}
                        (Ok(Entry::Queued(waiter)), "queued") => {
                            waiters.insert(expected["id"].as_u64().unwrap(), waiter);
                        }
                        (other, outcome) => panic!("{at}: {other:?} != {outcome}"),
                    }
                }
                "release" => {
                    let result = capacity.release(step["weight"].as_u64().unwrap() as usize);
                    let expected = step["result"].as_str().unwrap();
                    assert_eq!(result.is_ok(), expected == "ok", "{at}");
                }
                "cancel" => {
                    assert_eq!(capacity.cancel_waiters(owner.as_ref()) as u64, step["result"].as_u64().unwrap(), "{at}");
                }
                "observe" => {
                    let waiter = waiters.remove(&step["id"].as_u64().unwrap()).unwrap();
                    assert_eq!(waiter.finish(), step["result"].as_bool().unwrap(), "{at}");
                }
                "interrupt" => {
                    drop(waiters.remove(&step["id"].as_u64().unwrap()).unwrap());
                    assert_eq!(step["result"], "interrupted");
                }
                op => panic!("unknown op {op}"),
            }
            let state = &step["state"];
            assert_eq!(capacity.available() as u64, state["available"].as_u64().unwrap(), "{at}");
            let queue: Vec<u64> = capacity
                .queue()
                .iter()
                .map(|ticket| *waiters.iter().find(|(_, waiter)| Arc::ptr_eq(waiter.ticket(), ticket)).unwrap().0)
                .collect();
            let expected: Vec<u64> = state["queue"].as_array().unwrap().iter().map(|id| id.as_u64().unwrap()).collect();
            assert_eq!(queue, expected, "{at}");
            let tickets = state["tickets"].as_object().unwrap();
            assert_eq!(tickets.len(), waiters.len(), "{at}");
            for (id, waiter) in &waiters {
                let expected = &tickets[&id.to_string()];
                let ticket = waiter.ticket();
                assert_eq!(
                    (ticket.granted(), ticket.cancelled(), ticket.ready()),
                    (
                        expected["granted"].as_bool().unwrap(),
                        expected["cancelled"].as_bool().unwrap(),
                        expected["ready"].as_bool().unwrap()
                    ),
                    "{at}"
                );
            }
        }
    }
    assert!(steps > 100);
}

fn check_ledger_view(ledger: &Ledger, view: &Value, at: &str) {
    assert_eq!(ledger.known_memory_bytes(), view["known_memory_bytes"].as_u64().unwrap(), "{at}");
    assert_eq!(ledger.total(Resource::RamBacking, &[]), view["total_ram_backing_bytes"].as_u64().unwrap(), "{at}");
    for kind in Kind::ALL {
        assert_eq!(ledger.active_count(kind) as u64, view["active"][kind.as_str()].as_u64().unwrap(), "{at}");
    }
    assert_eq!(ledger.foreground_waiting(), view["foreground_waiting"].as_bool().unwrap(), "{at}");
    for probe in view["projected"].as_array().unwrap() {
        let (who, what) = (owner(&probe["owner"]), cost(&probe["cost"]).unwrap());
        assert_eq!(ledger.projected_memory_bytes(&who, &what, Resource::Memory), probe["memory_bytes"].as_u64().unwrap(), "{at}");
        assert_eq!(
            ledger.projected_memory_bytes(&who, &what, Resource::RamBacking),
            probe["ram_backing_bytes"].as_u64().unwrap(),
            "{at}"
        );
    }
    for next in view["next"].as_array().unwrap() {
        let limits = limits(&next["limits"]);
        assert_eq!(ledger.next_memory_demands(&limits, Resource::Memory), next["memory_bytes"].as_u64().unwrap(), "{at}");
        assert_eq!(ledger.next_memory_demands(&limits, Resource::RamBacking), next["ram_backing_bytes"].as_u64().unwrap(), "{at}");
        assert_eq!(serde_json::to_value(ledger.demand_snapshot(&limits)).unwrap(), next["demand_snapshot"], "{at}");
    }
    assert_eq!(snapshot_json(ledger), view["snapshot"], "{at}");
    let reservations: Vec<Value> = ledger
        .resource_reservations()
        .into_iter()
        .map(|(owner, shape)| json!([[owner.0, owner.1], shape]))
        .collect();
    assert_eq!(Value::Array(reservations), view["resource_reservations"], "{at}");
}

#[test]
fn transition_ledger_replays_python_traces() {
    let traces = goldens()["ledger"].as_array().unwrap();
    assert!(!traces.is_empty());
    for (index, trace) in traces.iter().enumerate() {
        let mut ledger = Ledger::new();
        let mut claims: HashMap<u64, Claim> = HashMap::new();
        for (number, step) in trace.as_array().unwrap().iter().enumerate() {
            let at = format!("trace {index}, step {number}: {}", step["op"]);
            match step["op"].as_str().unwrap() {
                "wait" => {
                    let token = ledger.wait(owner(&step["owner"]), cost(&step["cost"]).unwrap());
                    assert_eq!(token, step["result"].as_u64().unwrap(), "{at}");
                }
                "unwait" => ledger.unwait(step["token"].as_u64().unwrap()),
                "refresh_wait_cost" => ledger.refresh_wait_cost(&owner(&step["owner"]), &cost(&step["cost"]).unwrap()),
                "claim" => {
                    let claim = ledger.claim(owner(&step["owner"]), cost(&step["cost"]).unwrap(), shape(&step["requested"]));
                    assert_eq!(claim.token, step["result"].as_u64().unwrap(), "{at}");
                    claims.insert(claim.token, claim);
                }
                "release" => {
                    let mut claim = claims[&step["token"].as_u64().unwrap()].clone();
                    claim.owner = owner(&step["owner"]);
                    let result = match ledger.release(&claim) {
                        Ok(()) => "ok",
                        Err(NotOwned) => "not_owned",
                    };
                    assert_eq!(result, step["result"].as_str().unwrap(), "{at}");
                }
                "set_growth_forecast" => ledger.set_growth_forecast(&owner(&step["owner"]), cost(&step["cost"])),
                "set_growth_forecasts" => ledger.set_growth_forecasts(
                    step["forecasts"].as_array().unwrap().iter().map(|item| (owner(&item[0]), cost(&item[1]).unwrap())),
                ),
                op => panic!("unknown op {op}"),
            }
            check_ledger_view(&ledger, &step["view"], &at);
        }
    }
}

#[test]
fn growth_costs_match_python() {
    let cases = goldens()["growth_costs"].as_array().unwrap();
    assert!(cases.len() > 100);
    for (index, case) in cases.iter().enumerate() {
        let (mode, now, bound) = (mode(&case["mode"]), case["now"].as_f64().unwrap(), case["bound"].as_u64().unwrap());
        let kind = Kind::parse(case["kind"].as_str().unwrap()).unwrap();
        let request_id = case["request_id"].as_str().unwrap();
        let sample = decode_sample(&case["sample"]);
        let sample = sample.as_ref();
        assert_eq!(growth_sample(sample, now).is_some(), case["growth_sample"].as_bool().unwrap(), "case {index}");
        assert_eq!(growth_remaining(mode, bound, sample, now), case["growth_remaining"].as_u64().unwrap(), "case {index}");
        let (physical, ram) = continuation_growth(mode, bound, sample, now);
        assert_eq!(json!([physical, ram]), case["continuation_growth"], "case {index}");
        let cost = growth_cost(mode, bound, kind, request_id, sample, now);
        assert_eq!(encode_cost(Some(&cost)), case["growth_cost"], "case {index}");
    }
}

#[test]
fn startup_and_transition_costs_match_python() {
    let costs = &goldens()["costs"];
    for case in costs["startup"].as_array().unwrap() {
        let cost = startup_cost(mode(&case["mode"]), case["memory_mb"].as_u64().unwrap(), case["managed"].as_bool().unwrap());
        assert_eq!(encode_cost(Some(&cost)), case["cost"], "{case}");
    }
    for case in costs["transition"].as_array().unwrap() {
        let cost = transition_memory_cost(
            Kind::parse(case["kind"].as_str().unwrap()).unwrap(),
            mode(&case["mode"]),
            opt_u64(&case["memory_bytes"]),
            opt_u64(&case["ram_backing_bytes"]),
            opt_u64(&case["read_bytes"]),
            "p",
        );
        assert_eq!(encode_cost(Some(&cost)), case["cost"], "{case}");
    }
}

/// The samples and placements a forecast reads.
struct Observed {
    samples: HashMap<Owner, ResidentSample>,
    modes: HashMap<Owner, MemoryMode>,
    default: MemoryMode,
}

impl Observed {
    fn new(default: MemoryMode) -> Observed {
        Observed { samples: HashMap::new(), modes: HashMap::new(), default }
    }
}

impl Observations for Observed {
    fn historical(&self, key: &Owner) -> Option<ResidentSample> {
        self.samples.get(key).cloned()
    }

    fn memory_mode(&self, key: &Owner) -> MemoryMode {
        self.modes.get(key).copied().unwrap_or(self.default)
    }
}

fn intent(key: &Owner, job_id: &str, memory_bytes: u64, phase: &str, request_id: &str) -> GrowthIntent {
    GrowthIntent {
        sandbox_id: key.0.clone(),
        generation: key.1 as i64,
        job_id: job_id.into(),
        launch_sha256: String::new(),
        memory_bytes: memory_bytes as i64,
        phase: phase.into(),
        request_id: request_id.into(),
    }
}

fn encode_float(value: f64) -> Value {
    if value.is_finite() {
        json!(value)
    } else if value > 0.0 {
        json!("inf")
    } else {
        json!("-inf")
    }
}

#[test]
fn growth_forecasts_replay_python_traces() {
    let traces = goldens()["growth_forecasts"].as_array().unwrap();
    assert!(!traces.is_empty());
    for (index, trace) in traces.iter().enumerate() {
        let limits = limits(&trace["limits"]);
        let mut observed = Observed::new(mode(&trace["default_mode"]));
        let (mut ledger, mut growth, mut now) = (Ledger::new(), Growth::new([]), 1000.0);
        for (number, step) in trace["steps"].as_array().unwrap().iter().enumerate() {
            let at = format!("trace {index}, step {number}: {}", step["op"]);
            let key = owner(&step["key"]);
            match step["op"].as_str().unwrap() {
                "intent" => {
                    let item = &step["intent"];
                    let intent = intent(
                        &key,
                        item["job_id"].as_str().unwrap(),
                        item["memory_bytes"].as_u64().unwrap(),
                        item["phase"].as_str().unwrap(),
                        item["request_id"].as_str().unwrap(),
                    );
                    growth.record(&key, Some(intent), &mut ledger, &observed, now);
                }
                "forget" => growth.forget(&key, &mut ledger, &observed, now),
                "provisional" => growth.charge_provisional(&key, cost(&step["cost"]).unwrap(), &mut ledger, &observed, now),
                "clear_provisional" => growth.clear_provisional(&key, &mut ledger, &observed, now),
                "sample" => match decode_sample(&step["sample"]) {
                    Some(sample) => {
                        observed.samples.insert(key, sample);
                    }
                    None => {
                        observed.samples.remove(&key);
                    }
                },
                "mode" => {
                    observed.modes.insert(key, mode(&step["mode"]));
                }
                "advance" => now += step["seconds"].as_f64().unwrap(),
                "refresh" => growth.refresh_forecasts(&mut ledger, &observed, now),
                "demand" => {
                    growth.current_forecasts(&mut ledger, &observed, now);
                    let demand =
                        [ledger.next_memory_demands(&limits, Resource::Memory), ledger.next_memory_demands(&limits, Resource::RamBacking)];
                    assert_eq!(json!(demand), step["result"], "{at}");
                }
                op => panic!("unknown op {op}"),
            }
            assert_eq!(now, step["now"].as_f64().unwrap(), "{at}");
            let view = &step["view"];
            let mut forecasts: Vec<(Owner, Value)> =
                ledger.growth_forecasts().map(|(owner, cost)| (owner.clone(), encode_cost(Some(cost)))).collect();
            forecasts.sort_by(|a, b| a.0.cmp(&b.0));
            let forecasts: Vec<Value> = forecasts.into_iter().map(|(owner, cost)| json!([[owner.0, owner.1], cost])).collect();
            assert_eq!(Value::Array(forecasts), view["forecasts"], "{at}");
            assert_eq!(
                json!({"memory_bytes": ledger.growth_total(Resource::Memory), "ram_backing_bytes": ledger.growth_total(Resource::RamBacking)}),
                view["growth_totals"],
                "{at}"
            );
            assert_eq!(ledger.growth_unknown(), view["growth_unknown"].as_u64().unwrap(), "{at}");
            assert_eq!(encode_float(growth.credit_expires()), view["credit_expires"], "{at}");
        }
    }
}

const LIMITS: &Limits = &[(Kind::Startup, 2), (Kind::Restore, 1)];

#[test]
fn managed_growth_cases_replay_python() {
    let cases = goldens()["managed_growth"].as_array().unwrap();
    assert_eq!(cases.len(), 6);
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let observed_mode = mode(&case["mode"]);
        let mut observed = Observed::new(observed_mode);
        let (mut ledger, mut growth, mut now) = (Ledger::new(), Growth::new([]), 1000.0);
        for step in case["steps"].as_array().unwrap() {
            match step["op"].as_str().unwrap() {
                "intent" => {
                    let key = owner(&step["key"]);
                    let intent = intent(
                        &key,
                        step["job_id"].as_str().unwrap_or("job"),
                        step["memory_bytes"].as_u64().unwrap(),
                        step["phase"].as_str().unwrap(),
                        step["request_id"].as_str().unwrap_or(""),
                    );
                    growth.record(&key, Some(intent), &mut ledger, &observed, now);
                }
                "observe" => {
                    let s = &step["sample"];
                    let current = s["current"].as_u64().unwrap();
                    let shared = s.get("shared").and_then(Value::as_u64).unwrap_or(current);
                    let age = s.get("age").and_then(Value::as_f64).unwrap_or(0.0);
                    let peak = s.get("peak").and_then(Value::as_u64).unwrap_or(0);
                    let swap = s.get("swap").and_then(Value::as_u64).unwrap_or(0);
                    observed.samples.insert(owner(&step["key"]), sample(current, peak, shared, swap, now - age));
                    growth.refresh_forecasts(&mut ledger, &observed, now);
                }
                "advance" => now += step["seconds"].as_f64().unwrap(),
                "demand" => {
                    growth.current_forecasts(&mut ledger, &observed, now);
                    let demand =
                        [ledger.next_memory_demands(LIMITS, Resource::Memory), ledger.next_memory_demands(LIMITS, Resource::RamBacking)];
                    assert_eq!(json!(demand), step["result"], "{name}");
                    assert_eq!(
                        ledger.demand_snapshot(LIMITS).unknown_transition_memory_costs,
                        step["unknown"].as_u64().unwrap(),
                        "{name}"
                    );
                }
                op => panic!("unknown op {op}"),
            }
        }
    }
}

fn decode_metrics(value: &Value) -> Option<Metrics> {
    if value.is_null() {
        return None;
    }
    let backing = &value["memory_backing"];
    Some(Metrics {
        memory_total_mb: value["memory_total_mb"].as_u64().unwrap(),
        memory_available_mb: value["memory_available_mb"].as_u64().unwrap(),
        swap_total_mb: value["swap_total_mb"].as_u64().unwrap(),
        swap_free_mb: value["swap_free_mb"].as_u64().unwrap(),
        memory_psi_full_avg10: value["memory_psi_full_avg10"].as_f64(),
        memory_backing: (!backing.is_null()).then(|| MemoryBackingCapacity {
            total_bytes: opt_u64(&backing["total_bytes"]),
            available_bytes: opt_u64(&backing["available_bytes"]),
            identity: backing["identity"].as_str().map(str::to_string),
        }),
    })
}

/// The guard's loop around [`gate`] and [`decide`] (goldens.py `drive_decide`),
/// with the generator's fake clock: each sample takes its step's time, each
/// wait times out.
#[test]
fn admission_decisions_replay_python_decide() {
    let cases = goldens()["decide"].as_array().unwrap();
    assert!(cases.len() > 100);
    let (mut grants, mut waited) = (0, 0);
    for (index, case) in cases.iter().enumerate() {
        let mut ledger = Ledger::new();
        for op in case["prefix"].as_array().unwrap() {
            let (who, what) = (owner(&op["owner"]), cost(&op["cost"]).unwrap());
            match op["op"].as_str().unwrap() {
                "claim" => {
                    ledger.claim(who, what, None);
                }
                "wait" => {
                    ledger.wait(who, what);
                }
                "forecast" => ledger.set_growth_forecast(&who, Some(what)),
                other => panic!("unknown prefix {other}"),
            }
        }
        let steps = case["steps"].as_array().unwrap();
        let requotes = steps.iter().any(|step| !step["cost"].is_null());
        let mut request = Request {
            node: Node {
                admission_open: case["admission_open"].as_bool().unwrap(),
                capacity: shape(&case["capacity"]),
                ram_backing_configured: case["ram_backing_configured"].as_bool().unwrap(),
            },
            owner: owner(&case["owner"]),
            requested: shape(&case["requested"]).unwrap(),
            cost: cost(&case["cost"]),
            checks: Checks { shape: true, memory_psi: case["check_memory_pressure"].as_bool().unwrap() },
            deadline: case["deadline"].as_f64().unwrap(),
            now: case["start"].as_f64().unwrap(),
            previous_error: None,
        };
        let (mut now, mut attempt, mut waits) = (request.now, 0usize, Vec::<f64>::new());
        let outcome = loop {
            request.now = now;
            if let Err(refusal) = gate(&request) {
                break Decision::Refuse(refusal);
            }
            let step = &steps[attempt.min(steps.len() - 1)];
            attempt += 1;
            let metrics = if request.node.capacity.is_some() {
                now += step["sample_seconds"].as_f64().unwrap();
                decode_metrics(&step["metrics"])
            } else {
                None
            };
            if requotes {
                request.cost = cost(&step["cost"]);
            }
            request.now = now;
            if requotes
                && let (Some(_), Some(_), Some(cost)) = (&request.node.capacity, &metrics, &request.cost)
            {
                ledger.refresh_wait_cost(&request.owner, cost);
            }
            match decide(metrics.as_ref(), &ledger, &request) {
                Decision::Wait { reason, seconds } => {
                    waits.push(seconds);
                    request.previous_error = Some(reason);
                    now += seconds;
                }
                decision => break decision,
            }
        };
        let expected = &case["outcome"];
        let at = format!("case {index}: {outcome:?}");
        match outcome {
            Decision::Grant => {
                assert_eq!(expected["grant"], true, "{at}");
                grants += 1;
            }
            Decision::Refuse(Refusal::Closed(error)) => {
                assert_eq!((expected["refuse"].as_str(), expected["error"].as_str()), (Some("closed"), Some(error.as_str())), "{at}")
            }
            Decision::Refuse(Refusal::Capacity(error)) => {
                assert_eq!((expected["refuse"].as_str(), expected["error"].as_str()), (Some("capacity"), Some(error.as_str())), "{at}")
            }
            Decision::Wait { .. } => unreachable!(),
        }
        waited += !waits.is_empty() as usize;
        assert_eq!(json!(waits), expected["waits"], "{at}");
        assert_eq!(now, expected["end"].as_f64().unwrap(), "{at}");
    }
    assert!(grants > 50 && waited > 50, "grants {grants}, waited {waited}");
}

// ---------------------------------------------------------------------------
// tests/test_fair_capacity.py

async fn wait_queued(capacity: &FairCapacity<String>, count: usize) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while capacity.waiting() != count {
        assert!(Instant::now() < deadline, "admission did not queue");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_only_wakes_granted_head() {
    let capacity = FairCapacity::<String>::new(1).unwrap();
    assert!(capacity.try_acquire(1, None).unwrap());
    let done = Arc::new(tokio::sync::Semaphore::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let tasks: Vec<_> = (0..32)
        .map(|_| {
            let (capacity, done, entered) = (capacity.clone(), done.clone(), entered.clone());
            tokio::spawn(async move {
                assert!(capacity.acquire(1, None, Some(Duration::from_secs(3))).await.unwrap());
                entered.notify_one();
                let permit = tokio::time::timeout(Duration::from_secs(3), done.acquire()).await;
                capacity.release(1).unwrap();
                assert!(permit.is_ok());
            })
        })
        .collect();
    wait_queued(&capacity, 32).await;
    let tickets = capacity.queue();
    capacity.release(1).unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified()).await.unwrap();
    assert!(tickets[0].ready());
    assert!(tickets[1..].iter().all(|ticket| !ticket.ready()));
    assert!(!capacity.try_acquire(1, None).unwrap());
    done.add_permits(32);
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(3), task).await.unwrap().unwrap();
    }
    assert!(capacity.try_acquire(1, None).unwrap());
    capacity.release(1).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_weighted_head_grants_fitting_follower() {
    let capacity = FairCapacity::<String>::new(10).unwrap();
    assert!(capacity.try_acquire(6, None).unwrap());
    let head = {
        let capacity = capacity.clone();
        tokio::spawn(async move { capacity.acquire(8, None, Some(Duration::from_millis(200))).await.unwrap() })
    };
    wait_queued(&capacity, 1).await;
    let follower = {
        let capacity = capacity.clone();
        tokio::spawn(async move { capacity.acquire(4, None, Some(Duration::from_secs(2))).await.unwrap() })
    };
    assert!(!head.await.unwrap());
    assert!(follower.await.unwrap());
    capacity.release(4).unwrap();
    capacity.release(6).unwrap();
    assert!(capacity.try_acquire(10, None).unwrap());
}

fn queued(capacity: &FairCapacity<String>, weight: usize, owner: Option<&str>) -> Waiter<String> {
    match capacity.enter(weight, owner.map(str::to_string), true).unwrap() {
        Entry::Queued(waiter) => waiter,
        other => panic!("not queued: {other:?}"),
    }
}

#[test]
fn grant_racing_timeout_is_not_lost() {
    let capacity = FairCapacity::<String>::new(1).unwrap();
    assert!(capacity.try_acquire(1, None).unwrap());
    let waiter = queued(&capacity, 1, None);
    // The release grants the ticket as its wait times out.
    capacity.release(1).unwrap();
    assert!(waiter.finish());
    assert!(!capacity.try_acquire(1, None).unwrap());
    capacity.release(1).unwrap();
    assert_eq!(capacity.available(), 1);
}

#[test]
fn interruption_returns_an_already_granted_reservation() {
    for grant in [false, true] {
        let capacity = FairCapacity::<String>::new(1).unwrap();
        assert!(capacity.try_acquire(1, None).unwrap());
        let waiter = queued(&capacity, 1, None);
        if grant {
            capacity.release(1).unwrap();
        }
        drop(waiter);
        if !grant {
            capacity.release(1).unwrap();
        }
        assert_eq!(capacity.waiting(), 0, "grant {grant}");
        assert!(capacity.try_acquire(1, None).unwrap(), "grant {grant}");
    }
}

#[test]
fn a_dropped_acquire_future_returns_its_grant() {
    // A hyper client disconnect drops the admission future while it waits.
    let capacity = FairCapacity::<String>::new(1).unwrap();
    assert!(capacity.try_acquire(1, None).unwrap());
    let mut context = Context::from_waker(Waker::noop());
    let mut acquire = Box::pin(capacity.acquire(1, None, None));
    assert!(acquire.as_mut().poll(&mut context).is_pending());
    assert_eq!(capacity.waiting(), 1);
    capacity.release(1).unwrap();
    assert_eq!(capacity.available(), 0);
    drop(acquire);
    assert_eq!(capacity.available(), 1);
    // Polled after the grant, the same future would have kept it.
    let mut acquire = Box::pin(capacity.acquire(1, None, None));
    assert_eq!(acquire.as_mut().poll(&mut context), Poll::Ready(Ok(true)));
    assert_eq!(capacity.available(), 0);
}

#[test]
fn invalid_weights_and_releases_are_refused() {
    assert_eq!(FairCapacity::<String>::new(0).unwrap_err(), FairError::Capacity);
    let capacity = FairCapacity::<String>::new(2).unwrap();
    assert_eq!(capacity.try_acquire(3, None).unwrap_err(), FairError::Weight);
    assert_eq!(capacity.try_acquire(0, None).unwrap_err(), FairError::Weight);
    assert_eq!(capacity.release(1).unwrap_err(), FairError::Release);
    assert_eq!(capacity.release(0).unwrap_err(), FairError::Release);
}

// ---------------------------------------------------------------------------
// TransitionLedgerTests (tests/test_transition_admission.py)

fn kind_owner(kind: Kind, index: u64) -> Owner {
    (kind.as_str().to_string(), index)
}

#[test]
fn reclaim_prices_next_owner_not_the_configured_slot_wave() {
    let mut ledger = Ledger::new();
    let limits: &Limits = &[(Kind::Startup, 8), (Kind::Restore, 8)];
    for (kind, _) in limits {
        for index in 0..8 {
            ledger.wait(kind_owner(*kind, index), Cost::new(*kind, Some(2 * GIB)));
        }
    }
    // Sixteen pending 2 GiB guarantees do not justify 32 GiB of evictions.
    assert_eq!(ledger.next_memory_demands(limits, Resource::Memory), 2 * GIB);
    let pressure = Pressure::new(16.0 / 90.0, 0.0, 30.0, 16 * GIB);
    let demand = ledger.next_memory_demands(limits, Resource::Memory);
    let demand = MemoryDemand { physical_bytes: demand, ram_backing_bytes: demand };
    assert!(!decide_resident_wait(&pressure, demand, false, false).reclaim());
    let active = ledger.claim(kind_owner(Kind::Restore, 0), Cost::new(Kind::Restore, Some(2 * GIB)), None);
    assert_eq!(ledger.next_memory_demands(limits, Resource::Memory), 4 * GIB);
    let snapshot = ledger.demand_snapshot(limits);
    assert_eq!(snapshot.admitted_demand_bytes, 2 * GIB);
    assert_eq!(snapshot.pending_demand_bytes, 2 * GIB);
    assert_eq!(snapshot.unknown_transition_memory_costs, 0);
    ledger.release(&active).unwrap();
}

#[test]
fn launches_leave_headroom_for_every_queued_wake() {
    let mut ledger = Ledger::new();
    let restore = Cost::new(Kind::Restore, Some(4096));
    let start = Cost::new(Kind::Startup, Some(2048));
    let head = ledger.wait(key("wake", 1), restore.clone());
    ledger.wait(key("wake", 2), restore.clone());
    ledger.set_growth_forecasts([(key("wake", 1), Cost::new(Kind::Restore, Some(1024)))]);
    // Running rollouts first: a launch fits only beside both queued wakes.
    assert_eq!(ledger.projected_memory_bytes(&key("new", 1), &start, Resource::Memory), 4096 + 4096 + 2048);
    // The actual wake spends the same owner reservation, not both stages.
    assert_eq!(ledger.projected_memory_bytes(&key("wake", 1), &restore, Resource::Memory), 4096);
    ledger.unwait(head);
    assert_eq!(ledger.projected_memory_bytes(&key("new", 1), &start, Resource::Memory), 1024 + 4096 + 2048);
    // Full restore slots do not let a launch take a queued wake's headroom.
    let claim = ledger.claim(key("wake", 3), restore, None);
    assert_eq!(ledger.projected_memory_bytes(&key("new", 1), &start, Resource::Memory), 1024 + 4096 + 4096 + 2048);
    ledger.release(&claim).unwrap();
}

/// xorshift64*: the tests' own deterministic randomness.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }

    fn pick<T: Clone>(&mut self, items: &[T]) -> T {
        items[self.below(items.len())].clone()
    }
}

fn recount(claims: &[Claim], forecasts: &HashMap<Owner, Option<Cost>>, resource: Resource, extra: &[(Owner, Option<u64>)]) -> u64 {
    let mut peaks: HashMap<Owner, u64> = HashMap::new();
    let forecast = forecasts.iter().filter_map(|(owner, cost)| cost.as_ref().map(|cost| (owner.clone(), cost.get(resource))));
    let claimed = claims.iter().map(|claim| (claim.owner.clone(), claim.cost.get(resource)));
    for (owner, value) in forecast.chain(claimed).chain(extra.iter().cloned()) {
        if let Some(value) = value {
            let peak = peaks.entry(owner).or_insert(0);
            *peak = (*peak).max(value);
        }
    }
    peaks.values().sum()
}

#[test]
fn running_growth_totals_match_a_full_recount() {
    let mut rng = Rng(7);
    let mut ledger = Ledger::new();
    let owners: Vec<Owner> = (0..6).map(|index| key("s", index)).collect();
    let values = [None, Some(0), Some(512), Some(1024), Some(4096)];
    let random_cost = |rng: &mut Rng| {
        let mut cost = Cost::new(rng.pick(&Kind::ALL), rng.pick(&values));
        cost.ram_backing_bytes = rng.pick(&values);
        cost
    };
    let (mut claims, mut forecasts) = (Vec::<Claim>::new(), HashMap::<Owner, Option<Cost>>::new());
    for _ in 0..400 {
        let owner = rng.pick(&owners);
        let step = rng.below(10);
        if step < 4 {
            let cost = if rng.below(2) == 0 { None } else { Some(random_cost(&mut rng)) };
            ledger.set_growth_forecast(&owner, cost.clone());
            forecasts.insert(owner, cost);
        } else if step < 7 || claims.is_empty() {
            let cost = random_cost(&mut rng);
            claims.push(ledger.claim(owner, cost, None));
        } else {
            let claim = claims.remove(rng.below(claims.len()));
            ledger.release(&claim).unwrap();
        }
        for resource in Resource::ALL {
            assert_eq!(ledger.total(resource, &[]), recount(&claims, &forecasts, resource, &[]));
        }
    }
}

#[test]
fn unknown_cost_and_exact_token_lifetime() {
    let mut ledger = Ledger::new();
    let cost = Cost::new(Kind::Publication, None);
    let claim = ledger.claim(key("same", 1), cost.clone(), None);
    let peer = ledger.claim(key("same", 1), cost, None);
    let publication = |ledger: &Ledger| ledger.snapshot().into_iter().find(|(kind, _)| *kind == Kind::Publication).unwrap().1;
    assert_eq!(publication(&ledger).memory_bytes, None);
    let forged = Claim { owner: key("wrong", 1), ..claim.clone() };
    assert_eq!(ledger.release(&forged), Err(NotOwned));
    assert_eq!(ledger.active_count(Kind::Publication), 2);
    ledger.release(&claim).unwrap();
    assert_eq!(ledger.release(&claim), Err(NotOwned));
    assert_eq!(ledger.active_count(Kind::Publication), 1);
    ledger.release(&peer).unwrap();
    assert_eq!(publication(&ledger).memory_bytes, Some(0));
}

#[test]
fn duplicate_owner_forecast_is_shared_but_tokens_are_independent() {
    let mut ledger = Ledger::new();
    let cost = Cost::new(Kind::Restore, Some(1024));
    let first = ledger.claim(key("same", 1), cost.clone(), None);
    assert_eq!(ledger.projected_memory_bytes(&key("same", 1), &cost, Resource::Memory), 1024);
    let second = ledger.claim(key("same", 1), cost, None);
    assert_eq!(ledger.known_memory_bytes(), 1024);
    ledger.release(&first).unwrap();
    assert_eq!(ledger.known_memory_bytes(), 1024);
    ledger.release(&second).unwrap();
    assert_eq!(ledger.known_memory_bytes(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_fifo_head_grants_next_without_revoking_active_owner() {
    let capacity = FairCapacity::<String>::new(4).unwrap();
    assert!(capacity.try_acquire(2, Some("active".into())).unwrap());
    let head = {
        let capacity = capacity.clone();
        tokio::spawn(async move { capacity.acquire(4, Some("deleted".into()), Some(Duration::from_secs(3))).await.unwrap() })
    };
    wait_queued(&capacity, 1).await;
    let tail = {
        let capacity = capacity.clone();
        tokio::spawn(async move { capacity.acquire(2, Some("tail".into()), Some(Duration::from_secs(3))).await.unwrap() })
    };
    wait_queued(&capacity, 2).await;
    assert_eq!(capacity.cancel_waiters(Some(&"active".to_string())), 0);
    assert_eq!(capacity.cancel_waiters(Some(&"deleted".to_string())), 1);
    assert!(!tokio::time::timeout(Duration::from_secs(1), head).await.unwrap().unwrap());
    assert!(tokio::time::timeout(Duration::from_secs(1), tail).await.unwrap().unwrap());
    capacity.release(2).unwrap();
    capacity.release(2).unwrap();
    assert!(capacity.try_acquire(4, None).unwrap());
    capacity.release(4).unwrap();
}

// ---------------------------------------------------------------------------
// tests/test_managed_growth.py:319-427, at the forecast level: the intent a
// service call commits, a sampler pass, then what warm_park_demand bills.

struct GrowthCase {
    observed: Observed,
    ledger: Ledger,
    growth: Growth,
    now: f64,
}

impl GrowthCase {
    fn new(mode: MemoryMode) -> GrowthCase {
        GrowthCase { observed: Observed::new(mode), ledger: Ledger::new(), growth: Growth::new([]), now: 1000.0 }
    }

    fn intent(&mut self, key: &Owner, phase: &str, request_id: &str) {
        let intent = intent(key, "job", 4 * GIB, phase, request_id);
        self.growth.record(key, Some(intent), &mut self.ledger, &self.observed, self.now);
    }

    /// What a sampler pass publishes: the sample, then refreshed forecasts.
    fn observe(&mut self, key: &Owner, sample: ResidentSample) {
        self.observed.samples.insert(key.clone(), sample);
        self.growth.refresh_forecasts(&mut self.ledger, &self.observed, self.now);
    }

    fn sample(&self, current: u64, peak: u64, shared: Option<u64>, swap: u64, age: f64) -> ResidentSample {
        sample(current, peak, shared.unwrap_or(current), swap, self.now - age)
    }

    fn demand(&mut self) -> (u64, u64) {
        self.growth.current_forecasts(&mut self.ledger, &self.observed, self.now);
        (self.ledger.next_memory_demands(LIMITS, Resource::Memory), self.ledger.next_memory_demands(LIMITS, Resource::RamBacking))
    }
}

#[test]
fn residual_forecast_credits_only_fresh_incarnation_footprint() {
    let mut case = GrowthCase::new(MemoryMode::Ram);
    let one = key("one", 7);
    case.intent(&one, "active", "");
    let fresh = case.sample(3 * GIB, 0, Some(2 * GIB), 0, 0.0);
    case.observe(&one, fresh.clone());
    assert_eq!(case.demand().0, 2 * GIB);
    case.observe(&one, ResidentSample { sampled_at: 0.0, ..fresh.clone() });
    assert_eq!(case.demand().0, 4 * GIB);
    case.observe(&key("one", 8), fresh);
    assert_eq!(case.demand().0, 4 * GIB);
}

#[test]
fn a_swapped_continuation_owes_its_prefetch_not_its_whole_swap() {
    let mut case = GrowthCase::new(MemoryMode::Ram);
    let one = key("one", 7);
    case.intent(&one, "active", "wait-1");
    // A 3.5 GiB peak, 3 GiB of it reclaimed to swap by a pause.
    let swapped = case.sample(1 << 29, 7 << 29, None, 3 * GIB, 0.0);
    case.observe(&one, swapped);
    // The thaw's bounded prefetch now; swapped tmpfs blocks stay allocated.
    assert_eq!(case.demand(), (PREFETCH_MAX_BYTES, 0));
    // Without swap the same runtime owes its way back to the peak.
    let resident = case.sample(1 << 29, 7 << 29, None, 0, 0.0);
    case.observe(&one, resident);
    assert_eq!(case.demand().0, 3 * GIB);
}

#[test]
fn continuation_forecasts_physical_growth_to_its_demonstrated_peak() {
    let mut case = GrowthCase::new(MemoryMode::Ram);
    let (one, two) = (key("one", 7), key("two", 7));
    case.intent(&one, "active", "wait-1");
    let sample = case.sample(GIB, 3 << 29, None, 0, 0.0);
    case.observe(&one, sample.clone());
    // Both projections expect a return to the 1.5 GiB peak, not the 4 GiB bound.
    assert_eq!(case.demand(), (1 << 29, 1 << 29));
    // A launch has no safe wait yet and keeps its whole bound (less its pages).
    case.intent(&two, "active", "");
    case.observe(&two, sample);
    assert_eq!(case.demand(), ((1 << 29) + 3 * GIB, (1 << 29) + 3 * GIB));
}

#[test]
fn growth_credit_survives_a_slow_refresh_pass() {
    let mut case = GrowthCase::new(MemoryMode::File);
    let one = key("one", 7);
    case.intent(&one, "active", "");
    let sample = case.sample(3 * GIB, 0, None, 0, 10.0);
    case.observe(&one, sample);
    assert_eq!(case.demand().0, GIB);
    let sample = case.sample(3 * GIB, 0, None, 0, 40.0);
    case.observe(&one, sample);
    assert_eq!(case.demand().0, 4 * GIB);
    // Cached forecasts drop a credited sample as it ages out, pass or not.
    let sample = case.sample(3 * GIB, 0, None, 0, 29.9);
    case.observe(&one, sample);
    assert_eq!(case.demand().0, GIB);
    case.now += 0.2;
    assert_eq!(case.demand().0, 4 * GIB);
}

#[test]
fn terminal_releases_only_the_terminal_primary() {
    let mut case = GrowthCase::new(MemoryMode::File);
    let one = key("one", 7);
    case.intent(&one, "active", "");
    assert_eq!(case.demand().0, 4 * GIB);
    case.intent(&one, "terminal", "");
    assert_eq!(case.demand().0, 0);
    // Delete retires the key altogether.
    case.growth.forget(&one, &mut case.ledger, &case.observed, case.now);
    assert!(case.growth.intent(&one).is_none() && case.ledger.growth_forecast(&one).is_none());
}

#[test]
fn file_growth_does_not_reappear_when_reclaimable_pages_are_evicted() {
    let mut case = GrowthCase::new(MemoryMode::File);
    let one = key("one", 7);
    case.intent(&one, "active", "file-wait");
    for resident in [3 * GIB, 64 * MIB, 0] {
        let sample = case.sample(resident, 0, Some(0), 0, 0.0);
        case.observe(&one, sample);
        assert_eq!(case.demand(), (0, 0));
        assert_eq!(case.ledger.demand_snapshot(LIMITS).unknown_transition_memory_costs, 1);
    }
}

#[test]
fn covered_requests_and_provisional_claims() {
    let mut case = GrowthCase::new(MemoryMode::Ram);
    let one = key("one", 7);
    assert!(case.growth.covered(&one, false, "r").is_none());
    case.intent(&one, "safe", "r");
    // Its own safe wait is not covered; another request's continuation is.
    assert!(case.growth.covered(&one, false, "r").is_none());
    assert!(case.growth.covered(&one, false, "other").is_some());
    assert!(case.growth.covered(&one, true, "other").is_none());
    // An admitted continuation stays charged while its commit is in flight.
    let cost = transition_memory_cost(Kind::Restore, MemoryMode::Ram, Some(GIB), None, None, "primary-process-growth-forecast");
    case.growth.charge_provisional(&one, cost, &mut case.ledger, &case.observed, case.now);
    assert_eq!(case.demand(), (GIB, GIB));
    case.intent(&one, "active", "other");
    case.growth.clear_provisional(&one, &mut case.ledger, &case.observed, case.now);
    assert_eq!(case.demand(), (4 * GIB, 4 * GIB));
    assert!(case.growth.covered(&one, true, "").is_some());
    case.growth.retain(|_| false, &mut case.ledger, &case.observed, case.now);
    assert_eq!(case.demand(), (0, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn growth_turns_order_one_key_and_never_leak() {
    let turns = Arc::new(super::growth::Turns::default());
    let (one, two) = (key("one", 7), key("two", 7));
    let held = turns.take(&one).await;
    // Another key is independent.
    drop(tokio::time::timeout(Duration::from_secs(1), turns.take(&two)).await.unwrap());
    // The same key waits; a cancelled wait leaves no entry behind.
    assert!(tokio::time::timeout(Duration::from_millis(50), turns.take(&one)).await.is_err());
    assert_eq!(turns.active(), 1);
    let waiter = {
        let (turns, one) = (turns.clone(), one.clone());
        tokio::spawn(async move {
            let _turn = turns.take(&one).await;
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());
    drop(held);
    tokio::time::timeout(Duration::from_secs(1), waiter).await.unwrap().unwrap();
    assert_eq!(turns.active(), 0);
    let batch = turns.take_all([&two, &one, &two]).await;
    assert_eq!((batch.len(), turns.active()), (2, 2));
    drop(batch);
    assert_eq!(turns.active(), 0);
}

// ---------------------------------------------------------------------------
// tests/test_admission_properties.py invariants, on random Rust traces

#[test]
fn fair_capacity_random_traces_keep_fifo_and_conserve_weight() {
    for seed in 1..=40u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let total = 1 + rng.below(6);
        let capacity = FairCapacity::<String>::new(total).unwrap();
        let owners = [None, Some("a".to_string()), Some("b".to_string())];
        let (mut held, mut waiters, mut seq) = (Vec::<usize>::new(), Vec::<(u64, Waiter<String>)>::new(), 0u64);
        for _ in 0..60 {
            match rng.below(5) {
                0 | 1 => {
                    let (weight, owner) = (1 + rng.below(total), rng.pick(&owners));
                    let (queue_was_empty, available) = (capacity.waiting() == 0, capacity.available());
                    match capacity.enter(weight, owner, rng.below(4) != 0).unwrap() {
                        Entry::Granted => {
                            // No barging past a queue.
                            assert!(queue_was_empty && available >= weight);
                            held.push(weight);
                        }
                        Entry::Refused => assert!(!queue_was_empty || available < weight),
                        Entry::Queued(waiter) => {
                            seq += 1;
                            waiters.push((seq, waiter));
                        }
                    }
                }
                2 if !held.is_empty() => {
                    let weight = held.remove(rng.below(held.len()));
                    capacity.release(weight).unwrap();
                }
                3 => {
                    let owner = rng.pick(&owners);
                    let granted: Vec<u64> = waiters.iter().filter(|(_, w)| w.ticket().granted()).map(|(s, _)| *s).collect();
                    capacity.cancel_waiters(owner.as_ref());
                    // Cancel never revokes a grant.
                    assert!(granted.iter().all(|s| waiters.iter().any(|(t, w)| t == s && w.ticket().granted())));
                }
                _ if !waiters.is_empty() => {
                    let (_, waiter) = waiters.remove(rng.below(waiters.len()));
                    let (granted, weight) = (waiter.ticket().granted(), waiter.ticket().weight);
                    if rng.below(4) == 0 {
                        drop(waiter);
                    } else {
                        // A grant racing the timeout is never lost.
                        assert_eq!(waiter.finish(), granted);
                        if granted {
                            held.push(weight);
                        }
                    }
                }
                _ => {}
            }
            let pending_granted: usize = waiters.iter().filter(|(_, w)| w.ticket().granted()).map(|(_, w)| w.ticket().weight).sum();
            assert_eq!(capacity.available() + held.iter().sum::<usize>() + pending_granted, total, "seed {seed}");
            let queue = capacity.queue();
            assert!(queue.first().is_none_or(|head| head.weight > capacity.available()), "seed {seed}");
            let queued: Vec<u64> = queue
                .iter()
                .map(|ticket| waiters.iter().find(|(_, w)| Arc::ptr_eq(w.ticket(), ticket)).unwrap().0)
                .collect();
            assert!(queued.windows(2).all(|pair| pair[0] < pair[1]), "seed {seed}");
            for (seq, waiter) in &waiters {
                let ticket = waiter.ticket();
                assert!(!(ticket.granted() && ticket.cancelled()));
                if ticket.granted() {
                    // Nothing still queued was enqueued before a grant.
                    assert!(queued.iter().all(|queued| queued > seq), "seed {seed}");
                }
            }
        }
    }
}

#[test]
fn ledger_random_traces_match_a_recount() {
    for seed in 1..=30u64 {
        let mut rng = Rng(seed.wrapping_mul(0x2545_f491_4f6c_dd1d));
        let owners: Vec<Owner> = (0..5).map(|index| key("s", index)).collect();
        let values = [None, Some(0), Some(512), Some(1024), Some(2 * GIB)];
        let random_cost = |rng: &mut Rng| {
            let mut cost = Cost::new(rng.pick(&Kind::ALL), rng.pick(&values));
            cost.ram_backing_bytes = rng.pick(&values);
            cost
        };
        let mut ledger = Ledger::new();
        let (mut claims, mut forecasts, mut waits) = (Vec::<Claim>::new(), HashMap::new(), BTreeMap::<u64, (Owner, Cost)>::new());
        for _ in 0..80 {
            let owner = rng.pick(&owners);
            match rng.below(6) {
                0 => {
                    let cost = random_cost(&mut rng);
                    waits.insert(ledger.wait(owner.clone(), cost.clone()), (owner, cost));
                }
                1 if !waits.is_empty() => {
                    let token = *waits.keys().nth(rng.below(waits.len())).unwrap();
                    waits.remove(&token);
                    ledger.unwait(token);
                }
                2 => claims.push(ledger.claim(owner, random_cost(&mut rng), None)),
                3 if !claims.is_empty() => ledger.release(&claims.remove(rng.below(claims.len()))).unwrap(),
                _ => {
                    let cost = if rng.below(3) == 0 { None } else { Some(random_cost(&mut rng)) };
                    ledger.set_growth_forecast(&owner, cost.clone());
                    forecasts.insert(owner, cost);
                }
            }
            // Pending restores, first wait per owner not already restoring.
            let restoring: Vec<&Owner> = claims.iter().filter(|c| c.cost.kind == Kind::Restore).map(|c| &c.owner).collect();
            let mut restores: Vec<(Owner, Cost)> = Vec::new();
            for (who, cost) in waits.values() {
                if cost.kind == Kind::Restore && !restoring.contains(&who) && !restores.iter().any(|(seen, _)| seen == who) {
                    restores.push((who.clone(), cost.clone()));
                }
            }
            for resource in Resource::ALL {
                assert_eq!(ledger.total(resource, &[]), recount(&claims, &forecasts, resource, &[]), "seed {seed}");
                // Per owner a max, never a sum; a startup leaves every queued restore its headroom.
                let start = Cost { ram_backing_bytes: Some(512), ..Cost::new(Kind::Startup, Some(1024)) };
                let mut extra = vec![(key("new", 1), start.get(resource))];
                extra.extend(restores.iter().map(|(who, cost)| (who.clone(), cost.get(resource))));
                assert_eq!(
                    ledger.projected_memory_bytes(&key("new", 1), &start, resource),
                    recount(&claims, &forecasts, resource, &extra),
                    "seed {seed}"
                );
                // Reclaim bills admitted work plus the next eligible restore.
                let limits: &Limits = &[(Kind::Restore, 1)];
                let expected = match restores.first() {
                    Some((who, cost)) if claims.iter().all(|c| c.cost.kind != Kind::Restore) => {
                        recount(&claims, &forecasts, resource, &[(who.clone(), cost.get(resource))])
                    }
                    _ => recount(&claims, &forecasts, resource, &[]),
                };
                assert_eq!(ledger.next_memory_demands(limits, resource), expected, "seed {seed}");
            }
        }
    }
}
