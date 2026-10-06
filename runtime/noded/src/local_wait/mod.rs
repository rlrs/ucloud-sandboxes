//! Node-local model waits (pause-reclaim item 6; docs/node-local-model-waits.md;
//! phase-3 spec §1.5, §3): a port of `ucloud_sandboxes/local_wait.py`.
//!
//! The node sees a sandbox's plaintext HTTP/1.1 calls to a private relay
//! endpoint (IPv4 literals) at the TCP level. A call is outstanding when the
//! last relay payload went out; its answer is the next inbound payload. The
//! node pauses a sandbox whose call is outstanding and whose cgroup has been
//! idle, and thaws it on the answer's first packet, which a paused gVisor
//! network stack holds until then. Nothing here reads payloads: nftables logs
//! only the IP and TCP headers of relay flows to one NFLOG group.
//!
//! Pauses and thaws go through the pause tier ([`crate::pause`]) under the
//! exec fence (`tier`). Growth bookkeeping stays with the agent in 3a: a
//! pause emits a `wait` event, and the answer an `activate`
//! ([`LocalWaitEvents`]).
//!
//! The NFLOG group has one binder. `start` binds it before it touches the nft
//! table and fails closed: no bind, no local waits, and the table is left
//! alone (it may be another process's).

mod flow;
mod netlink;
mod nft;
mod packet;
mod scheduler;
mod tier;
#[cfg(test)]
mod tests;

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use serde_json::{Map, Value};

pub use flow::{ANSWERED_WATCH_SECONDS, EPSILON, Flow, IDLE_SECONDS, IDLE_USEC, SETTLE_SECONDS, cpu_usage_usec};
pub use netlink::{NflogSocket, attribute, bind_messages, config_message, parse_messages};
pub use nft::{NftRules, RuleSet, is_private, nft_script, relay_endpoints};
pub use packet::{Ipv4Net, NETWORK_CIDR, RelayPacket, parse_packet};
pub use scheduler::{BoxFuture, CandidateSource, GrowthAction, GrowthEvent, LocalWaitEvents, WaitCandidate};
pub use tier::{Exclusive, RegistryCandidates, WaitFence, cpu_stat_path, read_leases};

use crate::pause::PauseTier;
use scheduler::{Actions, Clock, Scheduler, Spawn, Stats};

pub const TABLE: &str = "ucloud_local_wait";
pub const NFLOG_GROUP: u16 = 4207;
/// IPv4 and the first 14 bytes of TCP, with IP options.
pub const SNAPLEN: usize = 64;
/// Errors of the packet reader are logged at most this often.
const READ_ERROR_LOG: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct LocalWaitConfig {
    /// The node's relays, `(host, port)` (`sandbox.network_relays`). Only
    /// private IPv4 literals take the local path.
    pub relays: Vec<(String, u16)>,
    /// The sandboxes' network (`NETWORK_CIDR`).
    pub network: Ipv4Net,
    pub candidates: Arc<dyn CandidateSource>,
    /// The policy's period (10 ms).
    pub tick: Duration,
    /// How often the candidates are listed again (1 s).
    pub refresh: Duration,
    /// Pauses and thaws in flight at once (Python's 8 executor threads).
    pub workers: usize,
}

impl LocalWaitConfig {
    pub fn new(relays: Vec<(String, u16)>, candidates: Arc<dyn CandidateSource>) -> LocalWaitConfig {
        LocalWaitConfig {
            relays,
            network: NETWORK_CIDR,
            candidates,
            tick: Duration::from_millis(10),
            refresh: Duration::from_secs(1),
            workers: 8,
        }
    }
}

#[derive(Debug)]
pub enum StartError {
    /// `sandbox.direct_pause_tier` is off: local waits pause through it.
    PauseTierDisabled,
    /// No relay is a private IPv4 literal: nothing to watch.
    NoEndpoints,
    /// Not inside a tokio runtime (pauses and thaws run on it).
    NoRuntime,
    /// The NFLOG group could not be bound (EPERM: another process holds it, or
    /// no CAP_NET_ADMIN).
    Nflog(std::io::Error),
    Rules(String),
}

impl std::fmt::Display for StartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartError::PauseTierDisabled => write!(f, "local model waits need the pause tier"),
            StartError::NoEndpoints => write!(f, "no relay is a private IPv4 literal"),
            StartError::NoRuntime => write!(f, "local model waits need a tokio runtime"),
            StartError::Nflog(error) => write!(f, "cannot bind NFLOG group {NFLOG_GROUP}: {error}"),
            StartError::Rules(error) => write!(f, "cannot install the {TABLE} table: {error}"),
        }
    }
}

impl std::error::Error for StartError {}

/// `time.monotonic()`.
pub(crate) fn monotonic() -> f64 {
    let mut now = libc::timespec { tv_sec: 0, tv_nsec: 0 };
    // SAFETY: `now` is a valid timespec.
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) };
    now.tv_sec as f64 + now.tv_nsec as f64 * 1e-9
}

/// Where logged packets come from (the NFLOG socket in production).
pub(crate) trait PacketSource: Send {
    /// One read: call `each` with every logged packet. A timeout reads none.
    fn read(&mut self, stats: &Stats, each: &mut dyn FnMut(&[u8])) -> std::io::Result<()>;
}

struct NflogSource {
    socket: NflogSocket,
    buffer: Vec<u8>,
}

impl PacketSource for NflogSource {
    fn read(&mut self, stats: &Stats, each: &mut dyn FnMut(&[u8])) -> std::io::Result<()> {
        match self.socket.recv(&mut self.buffer) {
            Ok(read) => {
                parse_messages(&self.buffer[..read], each);
                Ok(())
            }
            Err(error) => match error.raw_os_error() {
                Some(libc::EAGAIN | libc::EINTR) => Ok(()),
                // The kernel dropped packets for a full buffer; the socket goes on.
                Some(libc::ENOBUFS) => {
                    Stats::add(&stats.nflog_drops);
                    Ok(())
                }
                _ => Err(error),
            },
        }
    }
}

struct Running {
    stop: Arc<AtomicBool>,
    policy_stop: mpsc::Sender<()>,
    threads: Vec<JoinHandle<()>>,
    rules: Box<dyn RuleSet>,
}

/// The node's local waits, running until `stop` (or drop).
pub struct LocalWaits {
    scheduler: Arc<Scheduler>,
    endpoints: Vec<(Ipv4Addr, u16)>,
    running: Mutex<Option<Running>>,
}

impl LocalWaits {
    /// Bind the NFLOG group (fail closed), install the nft table, then start
    /// the packet reader and the 10 ms policy. Call inside the daemon's tokio
    /// runtime: pauses and thaws run on it. `fence`: the node's warden lock
    /// directory (`WaitFence::new(exec.warden_locks_dir)`).
    pub fn start(
        config: LocalWaitConfig,
        pause_tier: PauseTier,
        fence: WaitFence,
        events: Arc<dyn LocalWaitEvents>,
    ) -> Result<LocalWaits, StartError> {
        if !pause_tier.config().enabled {
            return Err(StartError::PauseTierDisabled);
        }
        let endpoints = relay_endpoints(config.relays.iter().map(|(host, port)| (host.as_str(), *port)));
        if endpoints.is_empty() {
            return Err(StartError::NoEndpoints);
        }
        let handle = tokio::runtime::Handle::try_current().map_err(|_| StartError::NoRuntime)?;
        let socket = NflogSocket::bind(NFLOG_GROUP).map_err(StartError::Nflog)?;
        let actions = Arc::new(tier::TierActions { tier: pause_tier, fence, candidates: config.candidates.clone(), events: events.clone() });
        let spawn: Spawn = Arc::new(move |future| {
            handle.spawn(future);
        });
        let rules = Box::new(NftRules::new(&endpoints, config.network));
        let source = Box::new(NflogSource { socket, buffer: vec![0u8; 1 << 20] });
        Self::start_with(config, endpoints, source, rules, Arc::new(actions), events, spawn, Arc::new(monotonic))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn start_with(
        config: LocalWaitConfig,
        endpoints: Vec<(Ipv4Addr, u16)>,
        mut source: Box<dyn PacketSource>,
        rules: Box<dyn RuleSet>,
        actions: Arc<dyn Actions>,
        events: Arc<dyn LocalWaitEvents>,
        spawn: Spawn,
        clock: Clock,
    ) -> Result<LocalWaits, StartError> {
        // Before the reader runs: no packet arrives without the table.
        rules.install().map_err(StartError::Rules)?;
        let scheduler = Scheduler::new(
            actions,
            config.candidates.clone(),
            events,
            spawn,
            clock.clone(),
            config.workers,
            config.refresh.as_secs_f64(),
        );
        let stop = Arc::new(AtomicBool::new(false));
        let (policy_stop, policy_stopped) = mpsc::channel::<()>();
        let network = config.network;
        let reader = {
            let (scheduler, stop, clock) = (scheduler.clone(), stop.clone(), clock.clone());
            std::thread::Builder::new().name("ucloud-local-wait-packets".into()).spawn(move || {
                let mut logged: Option<std::time::Instant> = None;
                while !stop.load(Ordering::Acquire) {
                    let result = source.read(&scheduler.stats, &mut |raw| {
                        if let Some(packet) = parse_packet(raw, network) {
                            Stats::add(&scheduler.stats.packets);
                            scheduler.observe(&packet, clock());
                        }
                    });
                    if let Err(error) = result {
                        Stats::add(&scheduler.stats.read_errors);
                        if logged.is_none_or(|at| at.elapsed() >= READ_ERROR_LOG) {
                            logged = Some(std::time::Instant::now());
                            eprintln!("ucloud-noded: local wait packet read failed: {error}");
                        }
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
            })
        };
        let reader = match reader {
            Ok(reader) => reader,
            Err(error) => {
                rules.remove();
                return Err(StartError::Rules(format!("cannot start the packet reader: {error}")));
            }
        };
        let tick = config.tick;
        let policy = std::thread::Builder::new().name("ucloud-local-wait".into()).spawn({
            let (scheduler, clock) = (scheduler.clone(), clock.clone());
            move || loop {
                scheduler.tick(clock());
                match policy_stopped.recv_timeout(tick) {
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    _ => return,
                }
            }
        });
        let policy = match policy {
            Ok(policy) => policy,
            Err(error) => {
                stop.store(true, Ordering::Release);
                let _ = reader.join();
                rules.remove();
                return Err(StartError::Rules(format!("cannot start the policy: {error}")));
            }
        };
        Ok(LocalWaits {
            scheduler,
            endpoints,
            running: Mutex::new(Some(Running { stop, policy_stop, threads: vec![reader, policy], rules })),
        })
    }

    /// The private relay endpoints the table logs.
    pub fn endpoints(&self) -> &[(Ipv4Addr, u16)] {
        &self.endpoints
    }

    /// Its call is answered (within the 30 s watch): escalation must never
    /// hibernate it, which would strand the answer it holds.
    pub fn answered(&self, sandbox_id: &str, generation: u64) -> bool {
        self.scheduler.answered(sandbox_id, generation, self.scheduler.now())
    }

    /// Counters of this process's local waits. `pauses` and `thaws` count
    /// local-wait actions only; the pause tier's own `pause_stats` (the
    /// heartbeat's numbers) already include them.
    pub fn stats(&self) -> Map<String, Value> {
        self.scheduler.snapshot()
    }

    /// Stop the reader and the policy, unbind the group and remove the nft
    /// table. Blocks up to one read timeout (200 ms). Pauses and thaws in
    /// flight finish on their own. Idempotent.
    pub fn stop(&self) {
        let running = self.running.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take();
        let Some(running) = running else { return };
        running.stop.store(true, Ordering::Release);
        drop(running.policy_stop);
        for thread in running.threads {
            let _ = thread.join();
        }
        running.rules.remove();
    }
}

impl Drop for LocalWaits {
    fn drop(&mut self) {
        self.stop();
    }
}
