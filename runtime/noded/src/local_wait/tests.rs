//! Local waits: Python goldens for the ruleset, the NFLOG requests and the
//! parsers (testdata/goldens.py); the scheduler ported from
//! tests/test_local_wait.py `SchedulerTests` with fake actions; the pause tier
//! actions against a fake runsc; and, as root only, the kernel pieces.

use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use serde_json::Value;

use super::scheduler::{Actions, Outcome, Recheck, Scheduler, Spawn, Stats};
use super::tier::{TierActions, try_lock};
use super::*;
use crate::exec_fence::{ExecFence, Fenced};
use crate::journal::JournalStore;
use crate::pause::tests::{TempDir, eventually};
use crate::pause::{PauseConfig, PauseTier};
use crate::warden::{Sandbox, WardenConfig};

type Key = (String, u64);

const GUEST: Ipv4Addr = Ipv4Addr::new(100, 96, 0, 3);
const RELAY: Ipv4Addr = Ipv4Addr::new(10, 42, 0, 2);

fn hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap()).collect()
}

fn goldens() -> Value {
    serde_json::from_str(include_str!("testdata/python_goldens.json")).unwrap()
}

/// An IPv4/TCP header as the rule snaps it (tests/test_local_wait.py `tcp`).
fn tcp(source: Ipv4Addr, destination: Ipv4Addr, payload: u16, flags: u8, sport: u16, dport: u16) -> Vec<u8> {
    let mut bytes = vec![0x45, 0];
    bytes.extend_from_slice(&(40 + payload).to_be_bytes());
    bytes.extend_from_slice(&[0, 0, 0, 0, 64, 6, 0, 0]);
    bytes.extend_from_slice(&source.octets());
    bytes.extend_from_slice(&destination.octets());
    bytes.extend_from_slice(&sport.to_be_bytes());
    bytes.extend_from_slice(&dport.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[5 << 4, flags, 0xFF, 0xFF, 0, 0, 0, 0]);
    bytes
}

fn request(payload: u16) -> RelayPacket {
    parse_packet(&tcp(GUEST, RELAY, payload, 0x18, 40000, 8092), NETWORK_CIDR).unwrap()
}

fn answer(payload: u16) -> RelayPacket {
    parse_packet(&tcp(RELAY, GUEST, payload, 0x10, 8092, 40000), NETWORK_CIDR).unwrap()
}

// ---------------------------------------------------------------------------
// Python goldens

#[test]
fn relay_endpoints_match_python() {
    let golden = goldens();
    for case in golden["relay_endpoints"].as_array().unwrap() {
        let relays: Vec<(String, u16)> = case["relays"]
            .as_array()
            .unwrap()
            .iter()
            .map(|relay| (relay[0].as_str().unwrap().to_string(), relay[1].as_u64().unwrap() as u16))
            .collect();
        let got: Vec<Value> = relay_endpoints(relays.iter().map(|(host, port)| (host.as_str(), *port)))
            .into_iter()
            .map(|(host, port)| serde_json::json!([host.to_string(), port]))
            .collect();
        assert_eq!(Value::Array(got), case["endpoints"], "relays {relays:?}");
    }
}

#[test]
fn the_ruleset_is_pythons_text() {
    let golden = goldens();
    assert_eq!((golden["table"].as_str(), golden["group"].as_u64(), golden["snaplen"].as_u64()),
        (Some(TABLE), Some(u64::from(NFLOG_GROUP)), Some(SNAPLEN as u64)));
    let scripts = golden["scripts"].as_array().unwrap();
    assert!(scripts.len() >= 6);
    for case in scripts {
        let endpoints: Vec<(Ipv4Addr, u16)> = case["endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| (e[0].as_str().unwrap().parse().unwrap(), e[1].as_u64().unwrap() as u16))
            .collect();
        let network = Ipv4Net::parse(case["network"].as_str().unwrap()).unwrap();
        assert_eq!(nft_script(&endpoints, network, NFLOG_GROUP), case["script"].as_str().unwrap());
        assert_eq!(NftRules::new(&endpoints, network).script(), case["script"].as_str().unwrap());
    }
}

#[test]
fn the_nflog_requests_are_pythons_bytes() {
    let golden = goldens();
    let want: Vec<Vec<u8>> = golden["configs"].as_array().unwrap().iter().map(|c| hex(c.as_str().unwrap())).collect();
    assert_eq!(bind_messages(NFLOG_GROUP).to_vec(), want);
}

#[test]
fn packets_parse_as_python_parses_them() {
    let golden = goldens();
    for case in golden["packets"].as_array().unwrap() {
        let raw = hex(case["raw"].as_str().unwrap());
        let got = parse_packet(&raw, NETWORK_CIDR).map(|p| {
            serde_json::json!([p.guest.to_string(), p.outbound, p.payload, p.flags, p.wakes()])
        });
        assert_eq!(got.unwrap_or(Value::Null), case["parsed"], "packet {}", case["raw"]);
    }
    assert_eq!(parse_packet(&[0x60; 40], NETWORK_CIDR), None);
}

#[test]
fn netlink_reads_yield_each_logged_header_as_python() {
    let golden = goldens();
    for case in golden["messages"].as_array().unwrap() {
        let data = hex(case["data"].as_str().unwrap());
        let mut got = Vec::new();
        parse_messages(&data, |packet| got.push(Value::from(packet.iter().map(|b| format!("{b:02x}")).collect::<String>())));
        assert_eq!(Value::Array(got), case["packets"], "read {}", case["data"]);
    }
}

#[test]
fn networks_are_strict_and_private_matches_python() {
    assert_eq!(Ipv4Net::parse("100.96.0.0/16"), Some(NETWORK_CIDR));
    assert_eq!(Ipv4Net::parse("100.96.0.1/16"), None);
    assert_eq!(Ipv4Net::parse("100.96.0.0/33"), None);
    assert!(NETWORK_CIDR.contains(Ipv4Addr::new(100, 96, 255, 255)) && !NETWORK_CIDR.contains(Ipv4Addr::new(100, 97, 0, 0)));
    assert!(Ipv4Net::parse("0.0.0.0/0").unwrap().contains(Ipv4Addr::new(8, 8, 8, 8)));
    assert!(is_private(RELAY) && !is_private(Ipv4Addr::new(100, 64, 0, 1)) && !is_private(Ipv4Addr::new(8, 8, 8, 8)));
}

// ---------------------------------------------------------------------------
// The flow and the scheduler (tests/test_local_wait.py)

#[test]
fn cpu_window_needs_its_full_span() {
    let mut flow = Flow::default();
    flow.sample(0.0, 10);
    flow.sample(0.03, 10);
    assert!(!flow.idle(0.03));
    flow.sample(0.06, 10);
    assert!(flow.idle(0.06));
    flow.sample(0.07, 5000);
    assert!(!flow.idle(0.07));
}

#[test]
fn cpu_stat_usage_is_read_like_python() {
    let dir = TempDir::new("lw-cpu");
    let path = dir.0.join("cpu.stat");
    std::fs::write(&path, "usage_usec 1234\nuser_usec 1000\n").unwrap();
    assert_eq!(cpu_usage_usec(&path).unwrap(), 1234);
    std::fs::write(&path, "user_usec 1000\n").unwrap();
    assert!(cpu_usage_usec(&path).is_err());
    assert!(cpu_usage_usec(&dir.0.join("missing")).is_err());
}

/// Runs an action's future at once; fake actions never wait.
fn immediate() -> Spawn {
    Arc::new(|mut future| {
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(())), "a fake action waited");
    })
}

#[derive(Default)]
struct Recorder {
    growth: Mutex<Vec<GrowthEvent>>,
    wakes: Mutex<Vec<(String, u64, String)>>,
}

impl LocalWaitEvents for Recorder {
    fn growth(&self, event: GrowthEvent) {
        self.growth.lock().unwrap().push(event);
    }

    fn wake(&self, sandbox_id: String, generation: u64, operation_id: String) -> BoxFuture<Result<(), String>> {
        self.wakes.lock().unwrap().push((sandbox_id, generation, operation_id));
        Box::pin(std::future::ready(Ok(())))
    }
}

impl Recorder {
    fn actions(&self) -> Vec<(&'static str, String)> {
        self.growth.lock().unwrap().iter().map(|e| (e.action.as_str(), e.request_id.clone())).collect()
    }
}

struct Fixed {
    candidates: Mutex<Vec<WaitCandidate>>,
    current: AtomicBool,
}

impl CandidateSource for Fixed {
    fn candidates(&self) -> Result<Vec<WaitCandidate>, String> {
        Ok(self.candidates.lock().unwrap().clone())
    }

    fn current(&self, _: &str, _: u64) -> bool {
        self.current.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct FakeActions {
    paused: Mutex<HashSet<Key>>,
    calls: Mutex<Vec<(&'static str, Key)>>,
    during_pause: Mutex<Vec<RelayPacket>>,
    scheduler: OnceLock<Weak<Scheduler>>,
    now: Mutex<f64>,
    refuse_thaws: AtomicUsize,
}

impl Actions for FakeActions {
    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool {
        self.paused.lock().unwrap().contains(&(sandbox_id.to_string(), generation))
    }

    fn pause(&self, wait: WaitCandidate, _: Recheck) -> BoxFuture<Outcome> {
        self.calls.lock().unwrap().push(("pause", wait.key()));
        self.paused.lock().unwrap().insert(wait.key());
        let packets = self.during_pause.lock().unwrap().clone();
        if let Some(scheduler) = self.scheduler.get().and_then(Weak::upgrade) {
            let now = *self.now.lock().unwrap();
            for packet in packets {
                scheduler.observe(&packet, now);
            }
        }
        Box::pin(std::future::ready(Outcome::Acted))
    }

    fn thaw(&self, wait: WaitCandidate) -> BoxFuture<Outcome> {
        if self.refuse_thaws.load(Ordering::SeqCst) > 0 {
            self.refuse_thaws.fetch_add(1, Ordering::SeqCst);
            return Box::pin(std::future::ready(Outcome::Failed("busy: a status read holds the request lock".into())));
        }
        self.calls.lock().unwrap().push(("thaw", wait.key()));
        self.paused.lock().unwrap().remove(&wait.key());
        Box::pin(std::future::ready(Outcome::Acted))
    }
}

fn sandbox(id: &str, generation: u64) -> Sandbox {
    Sandbox {
        sandbox_id: id.into(),
        generation,
        container_id: format!("c-{id}"),
        bundle: PathBuf::from("/nonexistent"),
        memory_directory: format!("mem-{id}"),
        spec_sha256: "a".repeat(64),
    }
}

struct Harness {
    _dir: TempDir,
    cpu: PathBuf,
    now: f64,
    actions: Arc<FakeActions>,
    events: Arc<Recorder>,
    candidates: Arc<Fixed>,
    scheduler: Arc<Scheduler>,
}

fn key() -> Key {
    ("s1".to_string(), 1)
}

impl Harness {
    fn new() -> Harness {
        let dir = TempDir::new("lw-sched");
        let cpu = dir.0.join("cpu.stat");
        let actions = Arc::new(FakeActions::default());
        let events = Arc::new(Recorder::default());
        let wait = WaitCandidate { sandbox: sandbox("s1", 1), guest: GUEST, cpu_stat: cpu.clone() };
        let candidates = Arc::new(Fixed { candidates: Mutex::new(vec![wait]), current: AtomicBool::new(true) });
        let scheduler = Scheduler::new(actions.clone(), candidates.clone(), events.clone(), immediate(), Arc::new(|| 0.0), 8, 1.0);
        actions.scheduler.set(Arc::downgrade(&scheduler)).unwrap();
        let mut harness = Harness { _dir: dir, cpu, now: 100.0, actions, events, candidates, scheduler };
        harness.usage(0);
        harness.set_now(100.0);
        harness
    }

    fn set_now(&mut self, now: f64) {
        self.now = now;
        *self.actions.now.lock().unwrap() = now;
    }

    fn usage(&self, usec: u64) {
        std::fs::write(&self.cpu, format!("usage_usec {usec}\nuser_usec 0\n")).unwrap();
    }

    fn run_for(&mut self, seconds: f64) {
        let end = self.now + seconds;
        while self.now < end - 1e-9 {
            self.scheduler.tick(self.now);
            self.set_now(((self.now + 0.01) * 1e6).round() / 1e6);
        }
    }

    fn send(&self) {
        self.scheduler.observe(&request(300), self.now);
    }

    fn answer(&self) {
        self.scheduler.observe(&answer(900), self.now);
    }

    fn calls(&self) -> Vec<(&'static str, Key)> {
        self.actions.calls.lock().unwrap().clone()
    }

    fn paused(&self) -> bool {
        self.actions.is_paused("s1", 1)
    }

    fn set_paused(&self, paused: bool) {
        let mut set = self.actions.paused.lock().unwrap();
        if paused { set.insert(key()) } else { set.remove(&key()) };
    }
}

#[test]
fn an_idle_outstanding_call_pauses_once_and_its_answer_thaws() {
    let mut h = Harness::new();
    h.run_for(0.02);
    h.send();
    h.run_for(0.04);
    assert_eq!(h.calls(), []); // Not yet settled, and not yet a full idle window.
    h.run_for(0.04);
    assert_eq!(h.calls(), [("pause", key())]);
    h.run_for(1.0);
    assert_eq!(h.calls().len(), 1);
    h.scheduler.observe(&answer(0), h.now);
    assert_eq!(h.calls().len(), 1); // A bare ACK does not thaw.
    h.answer();
    assert_eq!(h.calls().last(), Some(&("thaw", key())));
    h.run_for(0.5);
    assert_eq!(h.calls().len(), 2); // Answered: nothing outstanding.
    let stats = h.scheduler.snapshot();
    assert_eq!((&stats["pauses"], &stats["thaws"]), (&Value::from(1), &Value::from(1)));
}

#[test]
fn a_busy_sandbox_is_not_paused() {
    let mut h = Harness::new();
    h.run_for(0.01);
    h.send();
    for _ in 0..40 {
        h.usage((h.now * 1e6) as u64); // A background job keeps running while it waits.
        h.run_for(0.01);
    }
    assert_eq!(h.calls(), []);
    h.run_for(0.2);
    assert_eq!(h.calls(), [("pause", key())]); // Once it settles.
}

#[test]
fn an_answer_before_the_decision_or_during_the_pause_leaves_it_running() {
    let mut h = Harness::new();
    h.send();
    h.run_for(0.03);
    h.answer();
    h.run_for(0.5);
    assert_eq!(h.calls(), []);
    h.send();
    *h.actions.during_pause.lock().unwrap() = vec![answer(50)];
    h.run_for(0.2);
    assert_eq!(h.calls(), [("pause", key()), ("thaw", key())]);
    assert!(!h.paused());
    // The wait is recorded, and its answer (which raced it) activates it.
    let growth = h.events.actions();
    assert_eq!(growth.iter().map(|(action, _)| *action).collect::<Vec<_>>(), ["wait", "activate"]);
    assert_eq!(growth[0].1, growth[1].1);
}

#[test]
fn a_pause_landing_on_an_answered_call_is_undone() {
    // A status read thawed the paused sandbox (keep_paused), the answer
    // arrived meanwhile, then the read paused it again with the answer inside.
    let mut h = Harness::new();
    h.send();
    h.run_for(0.2);
    h.set_paused(false); // The read's thaw.
    h.answer();
    assert_eq!(h.calls(), [("pause", key())]);
    h.set_paused(true); // The read's re-pause.
    h.run_for(0.02);
    assert_eq!(h.calls().last(), Some(&("thaw", key())));
    h.send(); // The next request consumes the watch.
    h.run_for(0.02);
    h.set_paused(true);
    let before = h.calls().len();
    h.run_for(0.03);
    assert_eq!(h.calls().len(), before); // Nothing new until the policy pauses again.
}

#[test]
fn a_failed_thaw_of_an_answered_call_is_retried() {
    let mut h = Harness::new();
    h.send();
    h.run_for(0.2);
    h.actions.refuse_thaws.store(1, Ordering::SeqCst);
    h.answer();
    h.run_for(0.03);
    assert_eq!(h.actions.refuse_thaws.load(Ordering::SeqCst) - 1, 4); // The packet's attempt, then every tick.
    assert_eq!(h.scheduler.snapshot()["failures"], Value::from(4));
    h.actions.refuse_thaws.store(0, Ordering::SeqCst);
    h.run_for(0.01);
    assert_eq!(h.calls().last(), Some(&("thaw", key())));
    assert!(!h.paused());
}

#[test]
fn an_answered_call_reports_itself_to_escalation() {
    let mut h = Harness::new();
    h.send();
    h.run_for(0.2);
    assert!(!h.scheduler.answered("s1", 1, h.now));
    h.answer();
    assert!(h.scheduler.answered("s1", 1, h.now));
    assert!(!h.scheduler.answered("s1", 2, h.now));
    assert!(!h.scheduler.answered("s1", 1, h.now + ANSWERED_WATCH_SECONDS));
}

#[test]
fn flows_follow_the_candidates() {
    let mut h = Harness::new();
    h.send();
    h.candidates.candidates.lock().unwrap().clear();
    h.run_for(0.2);
    assert_eq!(h.calls(), []);
    assert_eq!(h.scheduler.counts().0, 0);
}

#[test]
fn a_wait_records_growth_and_its_answer_activates_however_it_was_thawed() {
    let mut h = Harness::new();
    h.send();
    h.run_for(0.2);
    let growth = h.events.growth.lock().unwrap().clone();
    assert_eq!(growth.len(), 1);
    assert_eq!((growth[0].action, growth[0].sandbox_id.as_str(), growth[0].generation), (GrowthAction::Wait, "s1", 1));
    assert!(growth[0].request_id.starts_with("local-wait-") && growth[0].request_id.len() == "local-wait-".len() + 32);
    assert_eq!(growth[0].to_json(), serde_json::json!({"action": "wait", "sandbox_id": "s1", "generation": 1, "request_id": growth[0].request_id}));
    // An exec thawed it (spec §9 #1): the answer still activates, once.
    h.set_paused(false);
    h.answer();
    assert_eq!(h.calls(), [("pause", key())]);
    h.answer();
    let actions = h.events.actions();
    assert_eq!(actions.iter().map(|(action, _)| *action).collect::<Vec<_>>(), ["wait", "activate"]);
    assert_eq!(actions[0].1, actions[1].1);
    // A wait whose sandbox stops being a candidate is forgotten.
    h.run_for(0.01);
    h.send();
    h.run_for(0.2);
    assert_eq!(h.events.actions().len(), 3);
    h.candidates.candidates.lock().unwrap().clear();
    h.run_for(1.1);
    h.candidates.candidates.lock().unwrap().push(WaitCandidate { sandbox: sandbox("s1", 1), guest: GUEST, cpu_stat: h.cpu.clone() });
    h.run_for(1.1);
    h.answer();
    assert_eq!(h.events.actions().len(), 3);
}

#[test]
fn at_most_one_action_per_sandbox_at_a_time() {
    let actions = Arc::new(FakeActions::default());
    let events = Arc::new(Recorder::default());
    let candidates = Arc::new(Fixed { candidates: Mutex::new(vec![]), current: AtomicBool::new(true) });
    let parked: Arc<Mutex<Vec<BoxFuture<()>>>> = Arc::default();
    let spawn: Spawn = {
        let parked = parked.clone();
        Arc::new(move |future| parked.lock().unwrap().push(future))
    };
    let scheduler = Scheduler::new(actions.clone(), candidates, events, spawn, Arc::new(|| 0.0), 8, 1.0);
    let wait = WaitCandidate { sandbox: sandbox("s1", 1), guest: GUEST, cpu_stat: PathBuf::from("/nonexistent") };
    assert!(scheduler.submit(wait.clone(), super::scheduler::Kind::Pause));
    assert!(!scheduler.submit(wait.clone(), super::scheduler::Kind::Thaw));
    let mut future = parked.lock().unwrap().pop().unwrap();
    assert!(future.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_ready());
    assert!(scheduler.submit(wait, super::scheduler::Kind::Thaw));
}

// ---------------------------------------------------------------------------
// The running scheduler: rules, reader and policy threads

struct ChannelSource {
    packets: std::sync::mpsc::Receiver<Vec<u8>>,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl PacketSource for ChannelSource {
    fn read(&mut self, _: &Stats, each: &mut dyn FnMut(&[u8])) -> std::io::Result<()> {
        {
            let mut log = self.log.lock().unwrap();
            if log.last() != Some(&"read") {
                log.push("read");
            }
        }
        if let Ok(packet) = self.packets.recv_timeout(Duration::from_millis(20)) {
            // One netlink read carrying the packet, as the kernel sends it.
            let mut attribute = (4 + packet.len() as u16).to_ne_bytes().to_vec();
            attribute.extend_from_slice(&9u16.to_ne_bytes());
            attribute.extend_from_slice(&packet);
            attribute.resize((attribute.len() + 3) & !3, 0);
            let mut message = ((20 + attribute.len()) as u32).to_ne_bytes().to_vec();
            message.extend_from_slice(&(4u16 << 8).to_ne_bytes());
            message.extend_from_slice(&[0; 10]);
            message.extend_from_slice(&[2, 0, 0x10, 0x6f]);
            message.extend_from_slice(&attribute);
            parse_messages(&message, |raw| each(raw));
        }
        Ok(())
    }
}

struct LoggedRules(Arc<Mutex<Vec<&'static str>>>, bool);

impl RuleSet for LoggedRules {
    fn install(&self) -> Result<(), String> {
        self.0.lock().unwrap().push("install");
        if self.1 { Ok(()) } else { Err("nft refused".into()) }
    }

    fn remove(&self) {
        self.0.lock().unwrap().push("remove");
    }
}

#[test]
fn the_running_scheduler_installs_reads_pauses_thaws_and_removes() {
    let dir = TempDir::new("lw-run");
    let cpu = dir.0.join("cpu.stat");
    std::fs::write(&cpu, "usage_usec 5\n").unwrap();
    let wait = WaitCandidate { sandbox: sandbox("s1", 1), guest: GUEST, cpu_stat: cpu };
    let candidates = Arc::new(Fixed { candidates: Mutex::new(vec![wait]), current: AtomicBool::new(true) });
    let actions = Arc::new(FakeActions::default());
    let events = Arc::new(Recorder::default());
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let (send, packets) = std::sync::mpsc::channel();
    let mut config = LocalWaitConfig::new(vec![("10.42.0.2".into(), 8092)], candidates.clone());
    config.refresh = Duration::from_millis(5);
    // A refused table starts nothing.
    let refused = LocalWaits::start_with(
        config.clone(),
        vec![(RELAY, 8092)],
        Box::new(ChannelSource { packets: std::sync::mpsc::channel().1, log: log.clone() }),
        Box::new(LoggedRules(log.clone(), false)),
        actions.clone(),
        events.clone(),
        immediate(),
        Arc::new(monotonic),
    );
    assert!(matches!(refused, Err(StartError::Rules(_))));
    assert_eq!(*log.lock().unwrap(), ["install"]);
    log.lock().unwrap().clear();
    let waits = LocalWaits::start_with(
        config,
        vec![(RELAY, 8092)],
        Box::new(ChannelSource { packets, log: log.clone() }),
        Box::new(LoggedRules(log.clone(), true)),
        actions.clone(),
        events,
        immediate(),
        Arc::new(monotonic),
    )
    .unwrap();
    assert_eq!(waits.endpoints(), [(RELAY, 8092)]);
    send.send(tcp(GUEST, RELAY, 300, 0x18, 40000, 8092)).unwrap();
    assert!(eventually(|| actions.calls.lock().unwrap().len() == 1));
    assert!(!waits.answered("s1", 1));
    send.send(tcp(RELAY, GUEST, 900, 0x18, 8092, 40000)).unwrap();
    assert!(eventually(|| actions.calls.lock().unwrap().len() == 2));
    assert_eq!(*actions.calls.lock().unwrap(), [("pause", key()), ("thaw", key())]);
    assert!(waits.answered("s1", 1));
    let stats = waits.stats();
    assert_eq!((&stats["packets"], &stats["pauses"], &stats["thaws"], &stats["candidates"]),
        (&Value::from(2), &Value::from(1), &Value::from(1), &Value::from(1)));
    waits.stop();
    waits.stop();
    assert_eq!(*log.lock().unwrap(), ["install", "read", "remove"]);
    drop(waits);
    assert_eq!(log.lock().unwrap().len(), 3);
}

// ---------------------------------------------------------------------------
// The pause tier under the exec fence, against a fake runsc

const RUNSC: &str = r#"#!/bin/sh
fake='@FAKE@'
verb=''
for arg in "$@"; do
  case "$arg" in --*) ;; *) verb="$arg"; break ;; esac
done
case "$verb" in
state)
  printf '{"id":"%s","pid":@PID@,"status":"%s"}\n' "$3" "$(cat "$fake/status")" ;;
pause|resume)
  echo "$verb" >> "$fake/commands"
  want=paused
  [ "$verb" = resume ] && want=running
  if [ "$(cat "$fake/status")" = "$want" ]; then echo "container is already $want" >&2; exit 1; fi
  echo "$want" > "$fake/status" ;;
esac
"#;
const PID: u32 = 4243;
const TICKS: u64 = 777_778;

struct TierFake {
    dir: TempDir,
    wait: WaitCandidate,
    tier: PauseTier,
    fence: WaitFence,
    locks: PathBuf,
}

impl TierFake {
    fn new() -> TierFake {
        let dir = TempDir::new("lw-tier");
        let root = dir.0.clone();
        let make = |path: &Path| std::fs::DirBuilder::new().recursive(true).mode(0o700).create(path).unwrap();
        for sub in ["bin", "fake", "runtime/warden-locks", "journals", "bundles/b-1", "memory", "proc/sys/kernel/random"] {
            make(&root.join(sub));
        }
        let mut sandbox = sandbox("sb-1", 1);
        sandbox.bundle = root.join("bundles/b-1");
        let runsc = root.join("bin/runsc");
        std::fs::write(&runsc, RUNSC.replace("@FAKE@", &root.join("fake").display().to_string()).replace("@PID@", &PID.to_string())).unwrap();
        std::fs::set_permissions(&runsc, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::write(root.join("fake/status"), "running\n").unwrap();
        let process = root.join("proc").join(PID.to_string());
        make(&process);
        let fields: Vec<String> = (0..19).map(|i| if i == 0 { "S".to_string() } else { "0".to_string() }).collect();
        std::fs::write(process.join("stat"), format!("{PID} (runsc-sandbox) {} {TICKS} 0 0\n", fields.join(" "))).unwrap();
        let runtime_root = root.join("runtime");
        let cmdline = format!("runsc-sandbox\0--root={}\0--bundle={}\0boot\0{}\0", runtime_root.display(), sandbox.bundle.display(), sandbox.container_id);
        std::fs::write(process.join("cmdline"), cmdline).unwrap();
        std::os::unix::fs::symlink(&runsc, process.join("exe")).unwrap();
        std::fs::write(process.join("cgroup"), format!("0::/ucloud-sandboxes/{}\n", sandbox.container_id)).unwrap();
        std::fs::write(root.join("proc/sys/kernel/random/boot_id"), "0123456789abcdef0123456789abcdef\n").unwrap();
        make(&root.join("cgroup/ucloud-sandboxes").join(&sandbox.container_id));
        std::fs::write(root.join("zswap_enabled"), "N\n").unwrap();
        JournalStore::new(root.join("journals"))
            .initialize_running(&sandbox.sandbox_id, sandbox.generation, &sandbox.spec_sha256, "create-1", PID as u64, TICKS)
            .unwrap();
        let warden = WardenConfig {
            runsc: runsc.clone(),
            runtime_root,
            bundle_root: root.join("bundles"),
            journal_root: root.join("journals"),
            memory_root: root.join("memory"),
            application_memory_root: None,
            network: "none".into(),
            reflink_memory_restore: false,
            proc_root: root.join("proc"),
            command_timeout: Duration::from_secs(10),
            stop_timeout: Duration::from_secs(1),
        };
        let mut config = PauseConfig::new(warden, true);
        config.cgroup_root = root.join("cgroup");
        config.zswap_enabled = root.join("zswap_enabled");
        // Exec the fresh script once (ETXTBSY while a forked sibling holds it).
        for _ in 0..200 {
            match std::process::Command::new(&runsc).arg("--root=/nonexistent").arg("warm-up").output() {
                Err(error) if error.raw_os_error() == Some(libc::ETXTBSY) => std::thread::sleep(Duration::from_millis(5)),
                result => {
                    result.unwrap();
                    break;
                }
            }
        }
        let locks = root.join("runtime/warden-locks");
        TierFake {
            wait: WaitCandidate { sandbox, guest: GUEST, cpu_stat: root.join("cpu.stat") },
            tier: PauseTier::new(config, None),
            fence: WaitFence::new(&locks),
            locks,
            dir,
        }
    }

    fn actions(&self, current: bool) -> (Arc<TierActions>, Arc<Recorder>) {
        let events = Arc::new(Recorder::default());
        let candidates = Arc::new(Fixed { candidates: Mutex::new(vec![]), current: AtomicBool::new(current) });
        let actions = Arc::new(TierActions { tier: self.tier.clone(), fence: self.fence.clone(), candidates, events: events.clone() });
        (actions, events)
    }

    fn status(&self) -> String {
        std::fs::read_to_string(self.dir.0.join("fake/status")).unwrap().trim().to_string()
    }

    fn paused(&self) -> bool {
        self.tier.is_paused("sb-1", 1)
    }

    fn set_journal_state(&self, state: &str) {
        let path = self.dir.0.join("journals/sb-1.sandbox-1.json");
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::write(&path, text.replace(r#""state":"running""#, &format!(r#""state":"{state}""#))).unwrap();
    }
}

fn yes() -> Recheck {
    Box::new(|| true)
}

#[tokio::test]
async fn a_pause_takes_the_fence_exclusively_and_skips_when_busy() {
    let fake = TierFake::new();
    let (actions, _) = fake.actions(true);
    // An exec holds A shared.
    let Fenced::Held(exec) = ExecFence::new(&fake.locks).acquire("sb-1").unwrap() else { panic!("the fence is free") };
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Busy);
    drop(exec);
    // A transition holds T exclusively.
    let transition = try_lock(&fake.locks.join(".sb-1.transition"), libc::LOCK_EX).unwrap().unwrap();
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Busy);
    drop(transition);
    assert!(!fake.paused() && fake.status() == "running");
    // The answer came in meanwhile: nothing to pause.
    assert_eq!(actions.pause(fake.wait.clone(), Box::new(|| false)).await, Outcome::Done);
    assert!(!fake.paused());
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Acted);
    assert!(fake.paused() && fake.status() == "paused");
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Done); // Already paused.
    // While paused, an exec can hold its fence and a pause cannot.
    let held = fake.fence.try_exclusive("sb-1").unwrap();
    assert!(held.is_some());
    assert!(matches!(ExecFence::new(&fake.locks).acquire("sb-1").unwrap(), Fenced::Busy));
    drop(held);
    assert_eq!(fake.tier.stats().get(crate::pause::Counter::Pauses), 1);
}

#[tokio::test]
async fn a_pause_skips_a_replaced_registration_or_a_runtime_that_is_not_running() {
    let fake = TierFake::new();
    let (stale, _) = fake.actions(false);
    assert_eq!(stale.pause(fake.wait.clone(), yes()).await, Outcome::Done);
    let (actions, _) = fake.actions(true);
    let mut dead = fake.wait.clone();
    dead.sandbox.generation = 2; // No journal for it.
    assert_eq!(actions.pause(dead, yes()).await, Outcome::Done);
    fake.set_journal_state("parked");
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Done);
    assert!(!fake.paused() && fake.status() == "running");
}

#[tokio::test]
async fn a_thaw_resumes_under_an_execs_fence_and_marks_activity() {
    let fake = TierFake::new();
    let (actions, events) = fake.actions(true);
    assert_eq!(actions.thaw(fake.wait.clone()).await, Outcome::Done); // Not paused.
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Acted);
    let activity = fake.locks.join(".sb-1.activity");
    let old = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);
    std::fs::File::options().write(true).open(&activity).unwrap().set_modified(old).unwrap();
    // A Python-served op holds A shared: a thaw shares it.
    let Fenced::Held(exec) = ExecFence::new(&fake.locks).acquire("sb-1").unwrap() else { panic!("the fence is free") };
    assert_eq!(actions.thaw(fake.wait.clone()).await, Outcome::Acted);
    drop(exec);
    assert!(!fake.paused() && fake.status() == "running");
    assert!(std::fs::metadata(&activity).unwrap().modified().unwrap() > old);
    assert!(events.wakes.lock().unwrap().is_empty());
    assert_eq!(fake.tier.stats().get(crate::pause::Counter::Thaws), 1);
}

#[tokio::test]
async fn a_thaw_behind_a_transition_or_off_a_live_runtime_goes_to_the_agent() {
    let fake = TierFake::new();
    let (actions, events) = fake.actions(true);
    assert_eq!(actions.pause(fake.wait.clone(), yes()).await, Outcome::Acted);
    let transition = try_lock(&fake.locks.join(".sb-1.transition"), libc::LOCK_EX).unwrap().unwrap();
    assert_eq!(actions.thaw(fake.wait.clone()).await, Outcome::Delegated);
    drop(transition);
    assert!(fake.paused());
    fake.set_journal_state("parked");
    assert_eq!(actions.thaw(fake.wait.clone()).await, Outcome::Delegated);
    assert!(fake.paused());
    let wakes = events.wakes.lock().unwrap().clone();
    assert_eq!(wakes.len(), 2);
    assert!(wakes.iter().all(|(id, generation, operation)| id == "sb-1" && *generation == 1 && operation.starts_with("local-wake-")));
    assert_ne!(wakes[0].2, wakes[1].2);
}

#[test]
fn the_fence_retakes_a_lock_on_an_unlinked_inode() {
    let dir = TempDir::new("lw-fence");
    let fence = WaitFence::new(&dir.0);
    let old = fence.try_exclusive("box-1").unwrap().unwrap();
    assert!(fence.try_exclusive("box-1").unwrap().is_none());
    // Delete unlinks A, then T, while an old holder keeps the orphans.
    std::fs::remove_file(dir.0.join(".box-1.activity")).unwrap();
    std::fs::remove_file(dir.0.join(".box-1.transition")).unwrap();
    assert!(fence.try_exclusive("box-1").unwrap().is_some());
    drop(old);
    assert!(fence.try_exclusive("../x").unwrap().is_none());
}

// ---------------------------------------------------------------------------
// Candidates

#[test]
fn leases_and_cgroups_come_from_the_node_files() {
    let dir = TempDir::new("lw-cand");
    let state = dir.0.join("network-slots.json");
    assert!(read_leases(&state, NETWORK_CIDR).unwrap().is_empty());
    std::fs::write(&state, r#"{"leases":{"agent\u00001":1,"other\u00002":7},"pool":[],"version":1}"#).unwrap();
    let leases = read_leases(&state, NETWORK_CIDR).unwrap();
    assert_eq!(leases.get(&("agent".to_string(), 1)), Some(&GUEST));
    assert_eq!(leases.get(&("other".to_string(), 2)), Some(&Ipv4Addr::new(100, 96, 0, 15)));
    std::fs::write(&state, r#"{"leases":{"agent\u00001":0}}"#).unwrap();
    assert!(read_leases(&state, NETWORK_CIDR).is_err());
    let bundle = dir.0.join("bundle");
    std::fs::create_dir(&bundle).unwrap();
    assert_eq!(cpu_stat_path(&bundle, Path::new("/sys/fs/cgroup")), None);
    std::fs::write(bundle.join("config.json"), r#"{"linux":{"cgroupsPath":"/ucloud-sandboxes/abc"}}"#).unwrap();
    assert_eq!(cpu_stat_path(&bundle, Path::new("/sys/fs/cgroup")), Some(PathBuf::from("/sys/fs/cgroup/ucloud-sandboxes/abc/cpu.stat")));
    std::fs::write(bundle.join("config.json"), r#"{"linux":{"cgroupsPath":"/a/../../etc"}}"#).unwrap();
    assert_eq!(cpu_stat_path(&bundle, Path::new("/sys/fs/cgroup")), None);
}

// ---------------------------------------------------------------------------
// start: refusals, and the kernel pieces as root only

fn tier_config(enabled: bool) -> PauseConfig {
    let warden = WardenConfig {
        runsc: "/nonexistent/runsc".into(),
        runtime_root: "/nonexistent/runtime".into(),
        bundle_root: "/nonexistent/bundles".into(),
        journal_root: "/nonexistent/journals".into(),
        memory_root: "/nonexistent/memory".into(),
        application_memory_root: None,
        network: "none".into(),
        reflink_memory_restore: false,
        proc_root: "/proc".into(),
        command_timeout: Duration::from_secs(1),
        stop_timeout: Duration::from_secs(1),
    };
    PauseConfig::new(warden, enabled)
}

fn start(enabled: bool, relays: &[&str]) -> Result<LocalWaits, StartError> {
    let candidates = Arc::new(Fixed { candidates: Mutex::new(vec![]), current: AtomicBool::new(true) });
    let relays = relays.iter().map(|relay| (relay.to_string(), 8092)).collect();
    LocalWaits::start(
        LocalWaitConfig::new(relays, candidates),
        PauseTier::new(tier_config(enabled), None),
        WaitFence::new("/nonexistent/warden-locks"),
        Arc::new(Recorder::default()),
    )
}

#[test]
fn start_refuses_without_the_pause_tier_a_private_relay_or_a_runtime() {
    assert!(matches!(start(false, &["10.42.0.2"]), Err(StartError::PauseTierDisabled)));
    assert!(matches!(start(true, &["relay.example.org", "77.42.92.27"]), Err(StartError::NoEndpoints)));
    assert!(matches!(start(true, &["10.42.0.2"]), Err(StartError::NoRuntime)));
}

fn root() -> bool {
    let root = crate::fsutil::euid() == 0;
    if !root {
        eprintln!("skipped: needs root (netlink and nftables)");
    }
    root
}

#[tokio::test]
async fn without_root_start_fails_closed_and_leaves_no_table() {
    if crate::fsutil::euid() == 0 {
        return; // Covered by the root tests.
    }
    assert!(matches!(start(true, &["10.42.0.2"]), Err(StartError::Nflog(_))));
}

#[test]
fn as_root_a_group_has_one_binder() {
    if !root() {
        return;
    }
    let group = 4299; // Not the node's group.
    let first = NflogSocket::bind(group).unwrap();
    let second = NflogSocket::bind(group).unwrap_err();
    assert_eq!(second.raw_os_error(), Some(libc::EPERM), "{second}");
    drop(first);
    // A sibling test's fork can hold the closed socket until its exec.
    let again: Mutex<Option<NflogSocket>> = Mutex::new(None);
    assert!(eventually(|| {
        let mut again = again.lock().unwrap();
        *again = NflogSocket::bind(group).ok();
        again.is_some()
    }));
    let again = again.into_inner().unwrap().unwrap();
    let mut buffer = vec![0u8; 4096];
    let started = std::time::Instant::now();
    let error = again.recv(&mut buffer).unwrap_err();
    assert_eq!(error.raw_os_error(), Some(libc::EAGAIN));
    assert!(started.elapsed() >= Duration::from_millis(150));
}

#[test]
fn as_root_the_table_installs_and_is_removed() {
    if !root() {
        return;
    }
    let listed = || std::process::Command::new("nft").args(["list", "table", "inet", TABLE]).output().map(|o| o.status.success());
    match listed() {
        Ok(false) => {}
        Ok(true) => return eprintln!("skipped: {TABLE} exists on this host"),
        Err(_) => return eprintln!("skipped: no nft"),
    }
    let rules = NftRules::new(&[(RELAY, 8092)], NETWORK_CIDR);
    rules.install().unwrap();
    rules.install().unwrap(); // Idempotent.
    let text = std::process::Command::new("nft").args(["list", "table", "inet", TABLE]).output().unwrap();
    let text = String::from_utf8_lossy(&text.stdout).to_string();
    rules.remove();
    assert!(!listed().unwrap());
    assert_eq!(text.matches("log group 4207 snaplen 64 queue-threshold 1").count(), 2, "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn as_root_start_binds_installs_and_stop_removes() {
    if !root() {
        return;
    }
    let listed = || std::process::Command::new("nft").args(["list", "table", "inet", TABLE]).output().is_ok_and(|o| o.status.success());
    if listed() {
        return eprintln!("skipped: {TABLE} exists on this host");
    }
    let waits = start(true, &["10.42.0.2", "relay.example.org"]).unwrap();
    assert_eq!(waits.endpoints(), [(RELAY, 8092)]);
    assert!(listed());
    // A second binder fails closed and leaves the first one's table alone.
    let second = start(true, &["10.42.0.2"]);
    assert!(matches!(&second, Err(StartError::Nflog(error)) if error.raw_os_error() == Some(libc::EPERM)), "{second:?}", second = second.as_ref().err());
    assert!(listed());
    tokio::task::spawn_blocking(move || waits.stop()).await.unwrap();
    assert!(!listed());
}
