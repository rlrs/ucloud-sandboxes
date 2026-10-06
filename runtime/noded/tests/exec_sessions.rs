//! The exec session manager with real processes (`/bin/sh -c ...` in place of
//! `runsc exec`; no root needed): sequencing, the cumulative-ack cursor and
//! replay, backpressure, completion, stdin, signals, capacity and eviction,
//! and the route answers for unknown sessions.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use ucloud_noded::exec::{ActivityHook, ExecError, ExecLimits, ExecManager, ExecRequest, Reply, StartTimings};

const SESSION_KEYS: [&str; 9] =
    ["id", "spec", "argv", "status", "exit_code", "stdin_open", "created_at", "updated_at", "final_sequence"];
const EVENT_KEYS: [&str; 5] = ["sequence", "stream", "data", "exit_code", "created_at"];

/// Dropped exactly once, when the session releases its guard.
struct Fence(Arc<AtomicBool>);

impl Drop for Fence {
    fn drop(&mut self) {
        assert!(!self.0.swap(true, Ordering::SeqCst), "guard dropped twice");
    }
}

fn fence() -> (Box<Fence>, Arc<AtomicBool>) {
    let released = Arc::new(AtomicBool::new(false));
    (Box::new(Fence(released.clone())), released)
}

fn request(script: &str, stdin: bool, initial_wait: Option<f64>) -> ExecRequest {
    let body = json!({"command": ["sh", "-c", script], "env": {}, "working_dir": null, "stdin": stdin, "tty": false});
    let query = initial_wait.map(|wait| format!("initial_wait_seconds={wait}")).unwrap_or_default();
    let request = ExecRequest::parse("sbx", body.to_string().as_bytes(), &query, None).unwrap();
    request.check_direct().unwrap();
    request
}

fn argv(script: &str) -> Vec<String> {
    vec!["/bin/sh".into(), "-c".into(), script.into()]
}

async fn start(manager: &ExecManager, script: &str, stdin: bool, initial_wait: Option<f64>) -> (String, Value, Arc<AtomicBool>) {
    let (guard, released) = fence();
    let reply = manager
        .start(request(script, stdin, initial_wait), argv(script), guard, None, StartTimings::now())
        .await
        .unwrap();
    assert_eq!(reply.status, 201);
    let id = reply.body["session"]["id"].as_str().unwrap().to_owned();
    (id, reply.body, released)
}

async fn events(manager: &ExecManager, id: &str, after: i64, wait: f64) -> Reply {
    manager.events(id, &format!("after={after}&limit=100&wait_seconds={wait}")).await
}

/// The SDK's read loop: page after the last sequence until the final event
/// (or an empty read of a terminal session without one). Checks contiguity.
async fn drain(manager: &ExecManager, id: &str, mut after: i64) -> (Vec<Value>, Value) {
    let mut seen = Vec::new();
    loop {
        let reply = events(manager, id, after, 5.0).await;
        assert_eq!(reply.status, 200, "{:?}", reply.body);
        let page = reply.body["events"].as_array().unwrap().clone();
        for event in &page {
            assert_eq!(event["sequence"].as_i64().unwrap(), after + 1, "gap in {page:?}");
            after += 1;
        }
        let session = reply.body["session"].clone();
        seen.extend(page.iter().cloned());
        let terminal = matches!(session["status"].as_str(), Some("exited" | "failed"));
        match session["final_sequence"].as_i64() {
            Some(last) if after >= last => return (seen, session),
            None if terminal && page.is_empty() => return (seen, session),
            _ => {}
        }
    }
}

fn stream_text(events: &[Value], stream: &str) -> String {
    events.iter().filter(|event| event["stream"] == stream).map(|event| event["data"].as_str().unwrap()).collect()
}

fn keys(value: &Value) -> Vec<&str> {
    value.as_object().unwrap().keys().map(String::as_str).collect()
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn manager() -> Arc<ExecManager> {
    ExecManager::new(ExecLimits::default(), None)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_command_reports_its_output_and_exit_in_order() {
    let manager = manager();
    let (id, body, released) =
        start(&manager, "printf 'hello\\n'; printf 'oops' >&2; exit 3", false, Some(0.05)).await;
    assert!(id.starts_with("exec-") && id.len() == 37, "{id}");
    assert_eq!(keys(&body), ["session", "events", "timings"]);
    assert_eq!(keys(&body["timings"]), ["manager", "start_ms", "session_start"]);
    assert_eq!(keys(&body["session"]), SESSION_KEYS);
    assert_eq!(body["session"]["argv"], json!(argv("printf 'hello\\n'; printf 'oops' >&2; exit 3")));
    let first = &body["events"][0];
    assert_eq!(keys(first), EVENT_KEYS);
    assert_eq!((first["sequence"].as_i64(), first["stream"].as_str(), first["data"].as_str()), (Some(1), Some("status"), Some("started")));
    let (events, session) = drain(&manager, &id, 0).await;
    assert_eq!(events[0]["stream"], "status");
    assert_eq!(stream_text(&events, "stdout"), "hello\n");
    assert_eq!(stream_text(&events, "stderr"), "oops");
    let last = events.last().unwrap();
    assert_eq!((last["stream"].as_str(), last["data"].as_str(), last["exit_code"].as_i64()), (Some("exit"), Some(""), Some(3)));
    assert_eq!(session["status"], "failed");
    assert_eq!(session["exit_code"], 3);
    assert_eq!(session["final_sequence"], last["sequence"]);
    assert_eq!(session["stdin_open"], false);
    assert!(session["created_at"].as_str().unwrap().ends_with("+00:00"));
    assert!(released.load(Ordering::SeqCst));
    // R2 answers the same session; without an initial wait R1 has no events.
    assert_eq!(manager.get(&id).body["session"]["id"], id.as_str());
    let (_, body, _) = start(&manager, "true", false, None).await;
    assert_eq!(keys(&body), ["session", "timings"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_cursor_replays_until_acknowledged() {
    let manager = manager();
    let (id, _, _) = start(&manager, "for i in 1 2 3; do echo $i; sleep 0.05; done", false, None).await;
    let (events, _) = drain(&manager, &id, 0).await;
    // Re-polling the same cursor replays: nothing was pruned (the buffer never filled).
    let again = events_page(&manager, &id, 0).await;
    assert_eq!(again, events);
    let tail = events_page(&manager, &id, 2).await;
    assert_eq!(tail, events[2..]);
    assert!(events_page(&manager, &id, 1_000).await.is_empty());
    assert_eq!(stream_text(&events, "stdout"), "1\n2\n3\n");
    // A limit of zero or less returns nothing at once.
    let reply = manager.events(&id, "after=0&limit=-1&wait_seconds=5").await;
    assert_eq!(reply.body["events"], json!([]));
}

async fn events_page(manager: &ExecManager, id: &str, after: i64) -> Vec<Value> {
    events(manager, id, after, 0.0).await.body["events"].as_array().unwrap().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledgement_releases_a_full_buffer_without_gaps() {
    let limits = ExecLimits { max_events_per_session: 4, ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (id, _, _) = start(&manager, "i=0; while [ $i -lt 20 ]; do echo $i; i=$((i+1)); sleep 0.01; done", false, None).await;
    // Unacknowledged, the producer stops at four held events.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let held = events_page(&manager, &id, 0).await;
    assert_eq!(held.len(), 4);
    assert_eq!(manager.get(&id).body["session"]["status"], "running");
    let (events, session) = drain(&manager, &id, 0).await;
    let lines: String = (0..20).map(|i| format!("{i}\n")).collect();
    assert_eq!(stream_text(&events, "stdout"), lines);
    assert_eq!(session["exit_code"], 0);
    // Acknowledged events were pruned to make room: a replay from 0 starts later.
    let replay = events_page(&manager, &id, 0).await;
    assert!(replay[0]["sequence"].as_i64().unwrap() > 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_reader_aborts_the_command() {
    let limits = ExecLimits {
        max_events_per_session: 3,
        output_idle_timeout: Duration::from_millis(300),
        ..ExecLimits::default()
    };
    let manager = ExecManager::new(limits, None);
    let (id, _, released) = start(&manager, "while true; do echo x; sleep 0.01; done", false, None).await;
    eventually("the abort", || released.load(Ordering::SeqCst)).await;
    let page = events_page(&manager, &id, 0).await;
    let streams: Vec<&str> = page.iter().map(|event| event["stream"].as_str().unwrap()).collect();
    assert_eq!(streams, ["status", "stdout", "stdout", "error", "exit"]);
    assert_eq!(page[3]["data"], "exec output consumer stopped advancing; output backpressure timed out");
    assert_eq!(page[4]["exit_code"], 1);
    let session = &manager.get(&id).body["session"];
    assert_eq!((session["status"].as_str(), session["exit_code"].as_i64()), (Some("failed"), Some(1)));
    assert_eq!(session["final_sequence"], 5);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stdin_is_written_closed_and_refused_like_python() {
    let manager = manager();
    let (id, body, _) = start(&manager, "cat; echo done", true, Some(0.05)).await;
    // No initial wait for stdin sessions: only the start event.
    assert_eq!(body["events"].as_array().unwrap().len(), 1);
    assert_eq!(body["session"]["stdin_open"], true);
    let reply = manager.stdin(&id, br#"{"data":"abc"}"#).await;
    assert_eq!((reply.status, &reply.body["session"]["stdin_open"]), (200, &json!(true)));
    let reply = manager.stdin(&id, r#"{"data":"é","eof":true}"#.as_bytes()).await;
    assert_eq!((reply.status, &reply.body["session"]["stdin_open"]), (200, &json!(false)));
    let (events, session) = drain(&manager, &id, 0).await;
    assert_eq!(stream_text(&events, "stdout"), "abcédone\n");
    assert_eq!(session["status"], "exited");
    // Closed: writes fail, closing again is fine.
    let reply = manager.stdin(&id, br#"{"data":"x"}"#).await;
    assert_eq!((reply.status, reply.body.clone()), (400, json!({"error": "stdin is closed for this exec session."})));
    assert_eq!(manager.close_stdin(&id).await.status, 200);
    // A session without stdin reads as closed.
    let (other, _, _) = start(&manager, "sleep 0.1", false, None).await;
    let reply = manager.stdin(&other, br#"{"data":"x"}"#).await;
    assert_eq!(reply.body["error"], "stdin is closed for this exec session.");
    let reply = manager.stdin(&other, b"[]").await;
    assert_eq!((reply.status, reply.body.clone()), (400, json!({"error": "stdin payload must be a JSON object"})));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_stdin_ends_the_input() {
    let manager = manager();
    let (id, _, _) = start(&manager, "wc -c", true, None).await;
    assert_eq!(manager.stdin(&id, br#"{"data":"12345"}"#).await.status, 200);
    let reply = manager.close_stdin(&id).await;
    assert_eq!((reply.status, &reply.body["session"]["stdin_open"]), (200, &json!(false)));
    let (events, session) = drain(&manager, &id, 0).await;
    assert_eq!(stream_text(&events, "stdout").trim(), "5");
    assert_eq!(session["exit_code"], 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signalled_process_exits_128_plus_the_signal() {
    let manager = manager();
    let (id, _, released) = start(&manager, "exec sleep 30", false, None).await;
    let reply = manager.signal(&id, br#"{"signal":15}"#).await;
    assert_eq!(reply.status, 200);
    let (events, session) = drain(&manager, &id, 0).await;
    assert_eq!(events.last().unwrap()["exit_code"], 143);
    assert_eq!((session["status"].as_str(), session["exit_code"].as_i64()), (Some("failed"), Some(143)));
    assert!(released.load(Ordering::SeqCst));
    // Terminal: signals are a no-op; bodies are still checked first.
    assert_eq!(manager.signal(&id, br#"{"signal":9}"#).await.status, 200);
    let reply = manager.signal(&id, br#"{"signal":0}"#).await;
    assert_eq!((reply.status, reply.body.clone()), (400, json!({"error": "signal must be an integer in [1, 64]"})));
    let reply = manager.signal(&id, br#"{"signal":9,"x":1}"#).await;
    assert_eq!(reply.body["error"], "signal payload must contain exactly signal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_sessions_are_404_on_reads_and_400_on_writes() {
    let manager = manager();
    assert!(!manager.owns("nope"));
    let reply = manager.get("nope");
    assert_eq!((reply.status, reply.body), (404, json!({"error": "exec session not found"})));
    let reply = manager.events("nope", "").await;
    assert_eq!((reply.status, reply.body), (404, json!({"error": "exec session not found: nope"})));
    for reply in [
        manager.stdin("nope", br#"{"data":"x"}"#).await,
        manager.close_stdin("nope").await,
        manager.signal("nope", br#"{"signal":9}"#).await,
    ] {
        assert_eq!((reply.status, reply.body), (400, json!({"error": "exec session not found: nope"})));
        assert!(reply.headers.is_empty());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spawn_failure_is_a_failed_session_not_an_http_error() {
    let manager = manager();
    let (guard, released) = fence();
    let reply = manager
        .start(request("true", false, Some(0.05)), vec!["/nonexistent/runsc".into(), "exec".into()], guard, None, StartTimings::now())
        .await
        .unwrap();
    assert_eq!(reply.status, 201);
    let events = reply.body["events"].as_array().unwrap();
    let summary: Vec<(&str, &str)> =
        events.iter().map(|event| (event["stream"].as_str().unwrap(), event["data"].as_str().unwrap())).collect();
    assert_eq!(summary, [
        ("status", "started"),
        ("error", "[Errno 2] No such file or directory: '/nonexistent/runsc'"),
        ("exit", "")
    ]);
    assert_eq!(events[2]["exit_code"], 1);
    // Python never marks the output closed on this path.
    assert_eq!(reply.body["session"]["final_sequence"], Value::Null);
    assert_eq!(reply.body["session"]["status"], "failed");
    assert!(released.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_evicts_delivered_results_and_never_running_sessions() {
    let limits = ExecLimits { max_sessions: 2, ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (first, _, _) = start(&manager, "exec sleep 30", false, None).await;
    let (second, _, _) = start(&manager, "exec sleep 30", false, None).await;
    let (guard, released) = fence();
    let refused = manager.start(request("true", false, None), argv("true"), guard, None, StartTimings::now()).await;
    assert_eq!(refused, Err(ExecError::ExecDeferred("exec session capacity reached".into())));
    let reply = refused.unwrap_err().reply();
    assert_eq!(reply.status, 503);
    assert_eq!(
        reply.body,
        json!({"error": "exec session capacity reached", "error_code": "node_active_exec_deferred", "retryable": true})
    );
    assert_eq!(reply.headers, [("Retry-After", "1")]);
    assert!(released.load(Ordering::SeqCst), "a refused start drops its guard");
    // Terminal but undelivered and recent: kept, still refused.
    manager.signal(&first, br#"{"signal":9}"#).await;
    manager.signal(&second, br#"{"signal":9}"#).await;
    eventually("both terminal", || manager.running_count() == 0).await;
    let (guard, _) = fence();
    assert!(manager.start(request("true", false, None), argv("true"), guard, None, StartTimings::now()).await.is_err());
    // Delivering the first's final event makes it evictable after the grace.
    drain(&manager, &first, 0).await;
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let (third, _, _) = start(&manager, "true", false, None).await;
    assert!(!manager.owns(&first));
    assert!(manager.owns(&second) && manager.owns(&third));
    assert_eq!(manager.get(&first).status, 404);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_initial_snapshot_result_is_evictable_at_once() {
    let limits = ExecLimits { max_sessions: 1, ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (first, body, _) = start(&manager, "echo hi", false, Some(0.05)).await;
    if body["session"]["final_sequence"].is_null() {
        // A slow machine missed the 50 ms window: deliver the final event instead.
        drain(&manager, &first, 0).await;
        tokio::time::sleep(Duration::from_millis(2_100)).await;
    }
    let (second, _, _) = start(&manager, "true", false, None).await;
    assert!(!manager.owns(&first) && manager.owns(&second));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn utf8_is_decoded_across_reads_with_replacement() {
    let manager = manager();
    let (id, _, _) = start(&manager, r"printf '\342\202'; sleep 0.1; printf '\254 \377\n'", false, None).await;
    let (events, _) = drain(&manager, &id, 0).await;
    assert_eq!(stream_text(&events, "stdout"), "€ \u{FFFD}\n");
    assert!(events.iter().filter(|event| event["stream"] == "stdout").count() >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_descendant_holding_output_leaves_no_final_sequence() {
    let limits = ExecLimits { output_quiet_grace: Duration::from_millis(200), ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (id, _, released) = start(&manager, "sleep 2 & echo done", false, None).await;
    eventually("completion", || released.load(Ordering::SeqCst)).await;
    let session = &manager.get(&id).body["session"];
    assert_eq!((session["status"].as_str(), session["exit_code"].as_i64()), (Some("exited"), Some(0)));
    assert_eq!(session["final_sequence"], Value::Null);
    let (events, _) = drain(&manager, &id, 0).await;
    assert_eq!(events.last().unwrap()["stream"], "exit");
    assert_eq!(stream_text(&events, "stdout"), "done\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_long_poll_wakes_on_output_and_pages_are_capped_by_bytes() {
    let limits = ExecLimits { page_bytes: 2_000, ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (id, _, _) = start(&manager, "sleep 0.2; head -c 1000 /dev/zero | tr '\\0' a; sleep 0.1; head -c 1000 /dev/zero | tr '\\0' b; sleep 0.1; echo", false, None).await;
    let started = Instant::now();
    let page = events_page(&manager, &id, 1).await;
    assert!(page.is_empty());
    let reply = events(&manager, &id, 1, 10.0).await;
    assert!(started.elapsed() < Duration::from_secs(5));
    assert!(!reply.body["events"].as_array().unwrap().is_empty());
    let (_, session) = drain(&manager, &id, 1).await;
    assert_eq!(session["status"], "exited");
    // Both 1000-byte events fit the count limit but not the byte cap.
    let page = events_page(&manager, &id, 1).await;
    assert_eq!(page.len(), 1);
    assert_eq!(page[0]["data"].as_str().unwrap().len(), 1000);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_activity_hook_runs_at_start_and_at_completion_with_the_guard() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let hook: ActivityHook = Arc::new(move |sandbox_id, guard| {
        assert_eq!(sandbox_id, "sbx");
        let fence = guard.downcast_ref::<Fence>().expect("the session's guard");
        assert!(!fence.0.load(Ordering::SeqCst), "the hook runs before the guard drops");
        seen.fetch_add(1, Ordering::SeqCst);
    });
    let manager = ExecManager::new(ExecLimits::default(), Some(hook));
    let (_, _, released) = start(&manager, "true", false, None).await;
    eventually("completion", || released.load(Ordering::SeqCst)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gateway_prefix_names_the_session() {
    let manager = manager();
    let prefix = format!("xr1.{}.{}", "p".repeat(40), "m".repeat(22));
    let body = json!({"command": ["true"], "env": {"B": "1"}, "working_dir": "/", "stdin": false, "tty": false});
    let request = ExecRequest::parse("sbx", body.to_string().as_bytes(), "", Some(&prefix)).unwrap();
    let (guard, _) = fence();
    let reply = manager.start(request, argv("true"), guard, None, StartTimings::now()).await.unwrap();
    let id = reply.body["session"]["id"].as_str().unwrap();
    assert_eq!(id.len(), prefix.len() + 33);
    assert!(id.starts_with(&format!("{prefix}.")));
    assert_eq!(reply.body["session"]["spec"], json!({
        "sandbox_id": "sbx", "command": ["true"], "env": {"B": "1"}, "working_dir": "/", "stdin": false, "tty": false
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capacity_evicts_the_oldest_retained_result_first() {
    let limits = ExecLimits { max_sessions: 2, completed_retention: Duration::from_millis(200), ..ExecLimits::default() };
    let manager = ExecManager::new(limits, None);
    let (first, _, first_done) = start(&manager, "true", false, None).await;
    eventually("the first exit", || first_done.load(Ordering::SeqCst)).await;
    let (second, _, second_done) = start(&manager, "true", false, None).await;
    eventually("the second exit", || second_done.load(Ordering::SeqCst)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (third, _, _) = start(&manager, "exec sleep 30", false, None).await;
    assert!(!manager.owns(&first), "least recently updated goes first");
    assert!(manager.owns(&second) && manager.owns(&third));
    manager.signal(&third, br#"{"signal":9}"#).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_spawn_fence_is_released_after_the_spawn_and_the_guard_after_the_process() {
    let manager = manager();
    let (guard, released) = fence();
    let (spawn_fence, spawn_released) = fence();
    let reply = manager
        .start(request("exec sleep 30", false, Some(0.05)), argv("exec sleep 30"), guard, Some(spawn_fence), StartTimings::now())
        .await
        .unwrap();
    assert!(spawn_released.load(Ordering::SeqCst));
    assert!(!released.load(Ordering::SeqCst));
    let id = reply.body["session"]["id"].as_str().unwrap();
    manager.signal(id, br#"{"signal":9}"#).await;
    eventually("completion", || released.load(Ordering::SeqCst)).await;
}
