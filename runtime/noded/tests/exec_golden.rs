//! The exec routes' parsing, argv, decoding and encoding against the Python
//! node agent (the oracle), case by case: R1 bodies and queries through
//! `NodeAgentHandler._start_exec` down to the real `DirectRunscWarden.exec_lease`
//! argv, R3 queries, R4 and R6 bodies, CPython's incremental UTF-8 decoder,
//! `json.dumps`, `isoformat` and the session/event key order.
//!
//! GOLDEN is the output of GENERATOR (below) run with the repository's Python;
//! `cargo test --test exec_golden -- --ignored` re-runs it and compares.

use std::path::Path;
use std::time::{Duration, UNIX_EPOCH};

use serde_json::Value;
use ucloud_noded::exec::{
    EventsQuery, ExecError, ExecRequest, StdinRequest, Utf8Decoder, isoformat, json, parse_signal, runsc_exec_argv,
};

fn check(golden: &Value) {
    let (runsc, root, container) = (
        golden["runsc"].as_str().unwrap(),
        golden["runtime_root"].as_str().unwrap(),
        golden["container_id"].as_str().unwrap(),
    );
    let start = golden["start"].as_array().unwrap();
    assert!(start.len() > 40);
    for case in start {
        let name = case["name"].as_str().unwrap();
        let parsed = ExecRequest::parse(
            case["sandbox_id"].as_str().unwrap(),
            case["body"].as_str().unwrap().as_bytes(),
            case["query"].as_str().unwrap(),
            None,
        )
        .and_then(|request| request.check_direct().map(|()| request));
        match (parsed, case.get("argv")) {
            (Ok(request), Some(argv)) => {
                let argv: Vec<String> = serde_json::from_value(argv.clone()).unwrap();
                assert_eq!(runsc_exec_argv(runsc, root, container, &request), argv, "{name}");
                assert_eq!(json::dumps(&request.spec_json()), case["spec"].as_str().unwrap(), "{name}");
            }
            (Err(error), None) => {
                assert_eq!(error.to_string(), case["error"].as_str().unwrap(), "{name}");
                assert!(matches!(error, ExecError::BadRequest(_)), "{name}: {error:?}");
                assert_eq!(u64::from(error.reply().status), case["status"].as_u64().unwrap(), "{name}");
            }
            (parsed, _) => panic!("{name}: {parsed:?} against {case}"),
        }
    }
    for case in golden["events"].as_array().unwrap() {
        let query = EventsQuery::parse(case["query"].as_str().unwrap());
        if case.get("drops_connection").is_some() {
            // Python drops the connection (spec §7.1); this port reads the default.
            assert_eq!((query.after, query.limit, query.wait), (0, 100, Duration::ZERO), "{case}");
            continue;
        }
        assert_eq!(query.after, case["after"].as_i64().unwrap(), "{case}");
        assert_eq!(query.limit, case["limit"].as_i64().unwrap(), "{case}");
        assert_eq!(query.wait.as_secs_f64(), case["wait"].as_f64().unwrap(), "{case}");
    }
    for case in golden["stdin"].as_array().unwrap() {
        let parsed = StdinRequest::parse(case["body"].as_str().unwrap().as_bytes());
        match case.get("error") {
            Some(error) => assert_eq!(parsed.unwrap_err().to_string(), error.as_str().unwrap(), "{case}"),
            None => {
                let parsed = parsed.unwrap();
                assert_eq!(parsed.data, case["data"].as_str().unwrap(), "{case}");
                assert_eq!(parsed.eof, case["eof"].as_bool().unwrap(), "{case}");
            }
        }
    }
    for case in golden["signal"].as_array().unwrap() {
        let parsed = parse_signal(case["body"].as_str().unwrap().as_bytes());
        match case.get("error") {
            Some(error) => {
                let parsed = parsed.unwrap_err();
                assert_eq!(parsed.to_string(), error.as_str().unwrap(), "{case}");
                assert_eq!(parsed.reply().status, 400);
            }
            None => assert_eq!(i64::from(parsed.unwrap()), case["signal"].as_i64().unwrap(), "{case}"),
        }
    }
    let decode = golden["decode"].as_array().unwrap();
    assert!(decode.len() > 80);
    for case in decode {
        let mut decoder = Utf8Decoder::new();
        let mut outputs: Vec<String> = case["chunks"]
            .as_array()
            .unwrap()
            .iter()
            .map(|chunk| decoder.decode(&hex(chunk.as_str().unwrap())))
            .collect();
        outputs.push(decoder.finish());
        assert_eq!(outputs, serde_json::from_value::<Vec<String>>(case["outputs"].clone()).unwrap(), "{case}");
    }
    for text in golden["dumps"].as_array().unwrap() {
        let text = text.as_str().unwrap();
        assert_eq!(json::dumps(&serde_json::from_str(text).unwrap()), text);
    }
    for case in golden["isoformat"].as_array().unwrap() {
        let at = UNIX_EPOCH + Duration::from_micros(case["micros"].as_u64().unwrap());
        assert_eq!(isoformat(at), case["text"].as_str().unwrap());
    }
    let keys = &golden["keys"];
    assert_eq!(keys["session"], serde_json::json!(SESSION_KEYS));
    assert_eq!(keys["spec"], serde_json::json!(SPEC_KEYS));
    assert_eq!(keys["event"], serde_json::json!(EVENT_KEYS));
}

/// `ExecSession.to_dict()`, `SandboxExecSpec.to_dict()`, `ExecEvent.to_dict()`;
/// tests/exec_sessions.rs checks the manager's JSON against the same lists.
const SESSION_KEYS: [&str; 9] =
    ["id", "spec", "argv", "status", "exit_code", "stdin_open", "created_at", "updated_at", "final_sequence"];
const SPEC_KEYS: [&str; 6] = ["sandbox_id", "command", "env", "working_dir", "stdin", "tty"];
const EVENT_KEYS: [&str; 5] = ["sequence", "stream", "data", "exit_code", "created_at"];

fn hex(text: &str) -> Vec<u8> {
    (0..text.len()).step_by(2).map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap()).collect()
}

#[test]
fn exec_routes_match_the_python_agent() {
    check(&serde_json::from_str(GOLDEN).unwrap());
}

/// Regenerate with the repository's Python and compare (needs `.venv`).
#[test]
#[ignore]
fn golden_is_what_python_generates_now() {
    let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let nanos = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let script = std::env::temp_dir().join(format!("noded-exec-golden-{}-{nanos}.py", std::process::id()));
    std::fs::write(&script, GENERATOR).unwrap();
    let output = std::process::Command::new(repository.join(".venv/bin/python"))
        .arg(&script)
        .current_dir(&repository)
        .output()
        .unwrap();
    std::fs::remove_file(&script).unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let fresh: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(fresh, serde_json::from_str::<Value>(GOLDEN).unwrap());
    check(&fresh);
}

const GOLDEN: &str = r##"{"runsc": "/usr/local/bin/runsc", "runtime_root": "/run/ucloud/runsc root", "container_id": "sbx-1.sandbox-3", "start": [{"name": "plain", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "env sorted, cwd", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"ZED\": \"1\", \"A_B\": \"x=y\", \"_u\": \"\", \"a\": \"\\u00e9 \\u2603\"}, \"working_dir\": \"/work dir\", \"stdin\": false, \"tty\": false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "--cwd=/work dir", "--env=A_B=x=y", "--env=ZED=1", "--env=_u=", "--env=a=\u00e9 \u2603", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{\"ZED\":\"1\",\"A_B\":\"x=y\",\"_u\":\"\",\"a\":\"\\u00e9 \\u2603\"},\"working_dir\":\"/work dir\",\"stdin\":false,\"tty\":false}"}, {"name": "stdin", "sandbox_id": "sbx-1", "body": "{\"command\": [\"cat\"], \"env\": {}, \"working_dir\": null, \"stdin\": true, \"tty\": false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "cat"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"cat\"],\"env\":{},\"working_dir\":null,\"stdin\":true,\"tty\":false}"}, {"name": "unicode command", "sandbox_id": "sbx-1", "body": "{\"command\": [\"printf\", \"%s\", \"na\\u00efve \\ud83d\\ude00\", \"\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "printf", "%s", "na\u00efve \ud83d\ude00", ""], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"printf\",\"%s\",\"na\\u00efve \\ud83d\\ude00\",\"\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "env order kept in spec", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"B\": \"2\", \"A\": \"1\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "--env=A=1", "--env=B=2", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{\"B\":\"2\",\"A\":\"1\"},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "duplicate env key", "sandbox_id": "sbx-1", "body": "{\"command\":[\"true\"],\"env\":{\"A\":\"1\",\"B\":\"2\",\"A\":\"3\"},\"working_dir\":null,\"stdin\":false,\"tty\":false}", "query": "", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "--env=A=3", "--env=B=2", "sbx-1.sandbox-3", "true"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"true\"],\"env\":{\"A\":\"3\",\"B\":\"2\"},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=0.05", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait zero", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=0", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait forms", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=+.01", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait exponent", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=1e-2", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait percent", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=%30.01", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait underscore", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=0.0_1", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait whitespace", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=%200.02%09", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait blank first", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=&initial_wait_seconds=0.01", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "wait other keys", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "a=1&initial_wait_seconds=0.03&initial_wait_seconds=9", "argv": ["/usr/local/bin/runsc", "--root=/run/ucloud/runsc root", "exec", "sbx-1.sandbox-3", "sh", "-c", "echo hi"], "spec": "{\"sandbox_id\":\"sbx-1\",\"command\":[\"sh\",\"-c\",\"echo hi\"],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}"}, {"name": "not an object", "sandbox_id": "sbx-1", "body": "[]", "query": "", "status": 400, "error": "exec payload must be a JSON object"}, {"name": "string", "sandbox_id": "sbx-1", "body": "\"x\"", "query": "", "status": 400, "error": "exec payload must be a JSON object"}, {"name": "missing key", "sandbox_id": "sbx-1", "body": "{\"command\":[\"a\"],\"env\":{},\"working_dir\":null,\"stdin\":false}", "query": "", "status": 400, "error": "exec payload has an invalid schema"}, {"name": "extra key", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false, \"extra\": 1}", "query": "", "status": 400, "error": "exec payload has an invalid schema"}, {"name": "command not a list", "sandbox_id": "sbx-1", "body": "{\"command\": \"ls\", \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec command must be a JSON string array"}, {"name": "command item", "sandbox_id": "sbx-1", "body": "{\"command\":[\"a\",1],\"env\":{},\"working_dir\":null,\"stdin\":false,\"tty\":false}", "query": "", "status": 400, "error": "exec command must be a JSON string array"}, {"name": "env not a map", "sandbox_id": "sbx-1", "body": "{\"command\":[\"a\"],\"env\":[],\"working_dir\":null,\"stdin\":false,\"tty\":false}", "query": "", "status": 400, "error": "exec env must be a JSON string map"}, {"name": "env value", "sandbox_id": "sbx-1", "body": "{\"command\":[\"a\"],\"env\":{\"A\":1},\"working_dir\":null,\"stdin\":false,\"tty\":false}", "query": "", "status": 400, "error": "exec env must be a JSON string map"}, {"name": "working_dir type", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": 1, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec working_dir must be a string or null"}, {"name": "stdin type", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": 1, \"tty\": false}", "query": "", "status": 400, "error": "exec stdin and tty must be booleans"}, {"name": "tty type", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": \"true\"}", "query": "", "status": 400, "error": "exec stdin and tty must be booleans"}, {"name": "wait word", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=x", "status": 400, "error": "could not convert string to float: 'x'"}, {"name": "wait quote", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=x'y", "status": 400, "error": "could not convert string to float: \"x'y\""}, {"name": "wait both quotes", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=a%22b'c", "status": 400, "error": "could not convert string to float: 'a\"b\\'c'"}, {"name": "wait control", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=%01%5C%7F", "status": 400, "error": "could not convert string to float: '\\x01\\\\\\x7f'"}, {"name": "wait blank", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=+", "status": 400, "error": "could not convert string to float: ' '"}, {"name": "wait underscores", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=0__1", "status": 400, "error": "could not convert string to float: '0__1'"}, {"name": "wait big", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=0.06", "status": 400, "error": "initial_wait_seconds must be between 0 and 0.05"}, {"name": "wait negative", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=-0.01", "status": 400, "error": "initial_wait_seconds must be between 0 and 0.05"}, {"name": "wait nan", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=nan", "status": 400, "error": "initial_wait_seconds must be between 0 and 0.05"}, {"name": "wait inf", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=inf", "status": 400, "error": "initial_wait_seconds must be between 0 and 0.05"}, {"name": "wait before validate", "sandbox_id": "sbx-1", "body": "{\"command\": [], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "initial_wait_seconds=1", "status": 400, "error": "initial_wait_seconds must be between 0 and 0.05"}, {"name": "empty command", "sandbox_id": "sbx-1", "body": "{\"command\": [], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec command cannot be empty."}, {"name": "nul command", "sandbox_id": "sbx-1", "body": "{\"command\": [\"a\\u0000\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec command cannot contain NUL bytes."}, {"name": "nul env key", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"A\\u0000\": \"1\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec environment cannot contain NUL bytes."}, {"name": "nul env value", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"A\": \"\\u0000\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec environment cannot contain NUL bytes."}, {"name": "nul working_dir", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": \"/a\\u0000\", \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec working_dir cannot contain NUL bytes."}, {"name": "empty sandbox id", "sandbox_id": "", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "sandbox id is required."}, {"name": "tty", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": null, \"stdin\": false, \"tty\": true}", "query": "", "status": 400, "error": "direct runtime TTY exec is not yet qualified"}, {"name": "tty before cwd", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": \"rel\", \"stdin\": false, \"tty\": true}", "query": "", "status": 400, "error": "direct runtime TTY exec is not yet qualified"}, {"name": "relative cwd", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": \"rel\", \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec working directory must be absolute"}, {"name": "empty cwd", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {}, \"working_dir\": \"\", \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec working directory must be absolute"}, {"name": "cwd before env", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"1\": \"x\"}, \"working_dir\": \"rel\", \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "exec working directory must be absolute"}, {"name": "env digit first", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"1A\": \"x\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "direct exec environment is invalid"}, {"name": "env dash", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"A-B\": \"x\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "direct exec environment is invalid"}, {"name": "env empty name", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"\": \"x\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "direct exec environment is invalid"}, {"name": "env unicode name", "sandbox_id": "sbx-1", "body": "{\"command\": [\"sh\", \"-c\", \"echo hi\"], \"env\": {\"\\u00c9\": \"x\"}, \"working_dir\": null, \"stdin\": false, \"tty\": false}", "query": "", "status": 400, "error": "direct exec environment is invalid"}], "events": [{"query": "", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=3&limit=7&wait_seconds=1.5", "after": 3, "limit": 7, "wait": 1.5}, {"query": "after=007&limit=+5", "after": 7, "limit": 5, "wait": 0.0}, {"query": "after=%2012%09&limit=-5", "after": 12, "limit": -5, "wait": 0.0}, {"query": "after=1_000", "after": 1000, "limit": 100, "wait": 0.0}, {"query": "after=_1", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=1_", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=1__0", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=--5", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=1.0", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=1+2", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=%31", "after": 1, "limit": 100, "wait": 0.0}, {"query": "after=", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=x&limit=y", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", "after": 9223372036854775807, "limit": 100, "wait": 0.0}, {"query": "after=99999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999", "after": 0, "limit": 100, "wait": 0.0}, {"query": "after=99999999999999999999", "after": 9223372036854775807, "limit": 100, "wait": 0.0}, {"query": "after=-99999999999999999999", "after": -9223372036854775808, "limit": 100, "wait": 0.0}, {"query": "after=5&after=6", "after": 5, "limit": 100, "wait": 0.0}, {"query": "wait_seconds=0", "after": 0, "limit": 100, "wait": 0.0}, {"query": "wait_seconds=31", "after": 0, "limit": 100, "wait": 30.0}, {"query": "wait_seconds=-1", "after": 0, "limit": 100, "wait": 0.0}, {"query": "wait_seconds=nan", "after": 0, "limit": 100, "wait": 0.0}, {"query": "wait_seconds=-inf", "after": 0, "limit": 100, "wait": 0.0}, {"query": "wait_seconds=Infinity", "after": 0, "limit": 100, "wait": 30.0}, {"query": "wait_seconds=1_0", "after": 0, "limit": 100, "wait": 10.0}, {"query": "wait_seconds=1e1", "after": 0, "limit": 100, "wait": 10.0}, {"query": "wait_seconds=.5", "after": 0, "limit": 100, "wait": 0.5}, {"query": "wait_seconds=5.", "after": 0, "limit": 100, "wait": 5.0}, {"query": "wait_seconds=+2.5e-1", "after": 0, "limit": 100, "wait": 0.25}, {"query": "wait_seconds=1e400", "after": 0, "limit": 100, "wait": 30.0}, {"query": "wait_seconds=%202%20", "after": 0, "limit": 100, "wait": 2.0}, {"query": "wait_seconds=x", "drops_connection": "could not convert string to float: 'x'"}, {"query": "wait_seconds=0x10", "drops_connection": "could not convert string to float: '0x10'"}, {"query": "wait_seconds=1_e1", "drops_connection": "could not convert string to float: '1_e1'"}, {"query": "wait_seconds=1._5", "drops_connection": "could not convert string to float: '1._5'"}, {"query": "wait_seconds=e1", "drops_connection": "could not convert string to float: 'e1'"}, {"query": "limit=0&wait_seconds=29.999", "after": 0, "limit": 0, "wait": 29.999}], "stdin": [{"body": "{\"data\":\"x\"}", "data": "x", "eof": false}, {"body": "{\"data\":\"x\",\"eof\":true}", "data": "x", "eof": true}, {"body": "{\"data\":true}", "data": "True", "eof": false}, {"body": "{\"data\":false}", "data": "", "eof": false}, {"body": "{\"data\":0}", "data": "", "eof": false}, {"body": "{\"data\":12}", "data": "12", "eof": false}, {"body": "{\"data\":-3}", "data": "-3", "eof": false}, {"body": "{\"data\":1.5}", "data": "1.5", "eof": false}, {"body": "{\"data\":1e20}", "data": "1e+20", "eof": false}, {"body": "{\"data\":1e16}", "data": "1e+16", "eof": false}, {"body": "{\"data\":0.0001}", "data": "0.0001", "eof": false}, {"body": "{\"data\":-0.0}", "data": "", "eof": false}, {"body": "{\"data\":null}", "data": "", "eof": false}, {"body": "{}", "data": "", "eof": false}, {"body": "{\"data\":\"\",\"eof\":1}", "data": "", "eof": true}, {"body": "{\"eof\":\"x\"}", "data": "", "eof": true}, {"body": "{\"eof\":0}", "data": "", "eof": false}, {"body": "{\"eof\":[]}", "data": "", "eof": false}, {"body": "{\"eof\":{}}", "data": "", "eof": false}, {"body": "{\"eof\":[0]}", "data": "", "eof": true}, {"body": "{\"eof\":0.0}", "data": "", "eof": false}, {"body": "{\"other\":1,\"data\":\"\u00e9\"}", "data": "\u00e9", "eof": false}, {"body": "[]", "status": 400, "error": "stdin payload must be a JSON object"}, {"body": "\"x\"", "status": 400, "error": "stdin payload must be a JSON object"}, {"body": "null", "status": 400, "error": "stdin payload must be a JSON object"}, {"body": "{\"data\":[]}", "data": "", "eof": false}, {"body": "{\"data\":{}}", "data": "", "eof": false}, {"body": "{\"data\":123456789012345678901234}", "data": "123456789012345678901234", "eof": false}], "signal": [{"body": "{\"signal\":15}", "signal": 15}, {"body": "{\"signal\":1}", "signal": 1}, {"body": "{\"signal\":64}", "signal": 64}, {"body": "{\"signal\":0}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":65}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":-9}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":true}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":9.0}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":\"9\"}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":null}", "status": 400, "error": "signal must be an integer in [1, 64]"}, {"body": "{\"signal\":9,\"x\":1}", "status": 400, "error": "signal payload must contain exactly signal"}, {"body": "{}", "status": 400, "error": "signal payload must contain exactly signal"}, {"body": "[]", "status": 400, "error": "signal payload must contain exactly signal"}, {"body": "{\"Signal\":9}", "status": 400, "error": "signal payload must contain exactly signal"}, {"body": "{\"signal\":99999999999999999999}", "status": 400, "error": "signal must be an integer in [1, 64]"}], "decode": [{"chunks": ["827788", "0080e2a9e20031a99fac987d"], "outputs": ["\ufffdw\ufffd", "\u0000\ufffd\ufffd\ufffd\u00001\ufffd\ufffd\ufffd\ufffd}", ""]}, {"chunks": ["", "e9", "7f3b"], "outputs": ["", "", "\ufffd\u007f;", ""]}, {"chunks": ["80", "a9c8", "c3", "e2a92180", "9d"], "outputs": ["\ufffd", "\ufffd", "\ufffd", "\ufffd\ufffd!\ufffd", "\ufffd", ""]}, {"chunks": ["7f9fc3c3ca7f", "9ff4cac3eadbfb98ac", "ec61907ce15b0c"], "outputs": ["\u007f\ufffd\ufffd\ufffd\ufffd\u007f", "\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd", "\ufffda\ufffd|\ufffd[\f", ""]}, {"chunks": ["90e2", "ed9f00d3614ae4e2e2", "ed0a", ""], "outputs": ["\ufffd", "\ufffd\ufffd\u0000\ufffdaJ\ufffd\ufffd", "\ufffd\ufffd\n", "", ""]}, {"chunks": ["", "00e5e2", "4c00aec2f0989f98e6", "7fee040ae200c8e89ff5b861"], "outputs": ["", "\u0000\ufffd", "\ufffdL\u0000\ufffd\ufffd\ud821\udfd8", "\ufffd\u007f\ufffd\u0004\n\ufffd\u0000\ufffd\ufffd\ufffd\ufffda", ""]}, {"chunks": ["", "4e0085ed", "cacb009f980a", "61c40a0a800698aff5f882a3c8d5"], "outputs": ["", "N\u0000\ufffd", "\ufffd\ufffd\ufffd\u0000\ufffd\ufffd\n", "a\ufffd\n\n\ufffd\u0006\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["", "03"], "outputs": ["", "\u0003", ""]}, {"chunks": ["b4a482ac", "c39f"], "outputs": ["\ufffd\ufffd\ufffd\ufffd", "\u00df", ""]}, {"chunks": ["b1", "c7ac7fe30a82"], "outputs": ["\ufffd", "\u01ec\u007f\ufffd\n\ufffd", ""]}, {"chunks": ["", "9fc1", ""], "outputs": ["", "\ufffd\ufffd", "", ""]}, {"chunks": ["00ac9f", "1f727fa3e400efdfaac3e2acf4", "dec9e0cc82", "82ac00"], "outputs": ["\u0000\ufffd\ufffd", "\u001fr\u007f\ufffd\ufffd\u0000\ufffd\u07ea\ufffd\ufffd", "\ufffd\ufffd\ufffd\ufffd\u0302", "\ufffd\ufffd\u0000", ""]}, {"chunks": ["00", "f0a9", "9f", "f0"], "outputs": ["\u0000", "", "", "\ufffd", "\ufffd"]}, {"chunks": ["e6", "95cde12a140d", "00", "80", "a93bba"], "outputs": ["", "\ufffd\ufffd\ufffd*\u0014\r", "\u0000", "\ufffd", "\ufffd;\ufffd", ""]}, {"chunks": ["c39baf057fea483622", "c3", "fa", "82", "eee5f36a"], "outputs": ["\u00db\ufffd\u0005\u007f\ufffdH6\"", "", "\ufffd\ufffd", "\ufffd", "\ufffd\ufffd\ufffdj", ""]}, {"chunks": ["a9", "0ae2d4", ""], "outputs": ["\ufffd", "\n\ufffd", "", "\ufffd"]}, {"chunks": ["", "7f", "9f80", "00dc61"], "outputs": ["", "\u007f", "\ufffd\ufffd", "\u0000\ufffda", ""]}, {"chunks": ["", "b1dff0", ""], "outputs": ["", "\ufffd\ufffd", "", "\ufffd"]}, {"chunks": ["d2ea99ceace3e298820a1300f9"], "outputs": ["\ufffd\ufffd\u03ac\ufffd\u2602\n\u0013\u0000\ufffd", ""]}, {"chunks": ["7f9fd78b04f0e2c334f0"], "outputs": ["\u007f\ufffd\u05cb\u0004\ufffd\ufffd\ufffd4", "\ufffd"]}, {"chunks": ["828077c29fc480e24c0ac30af0f0b6e2f0"], "outputs": ["\ufffd\ufffdw\u009f\u0100\ufffdL\n\ufffd\n\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["a1c3a9d2d4ac9f98e2f1c07f2a80e2850ad3a94480f061d3"], "outputs": ["\ufffd\u00e9\ufffd\u052c\ufffd\ufffd\ufffd\ufffd\ufffd\u007f*\ufffd\ufffd\n\u04e9D\ufffd\ufffda", "\ufffd"]}, {"chunks": ["98ac", "9ff180e2acf0c3e69f", "0ad882"], "outputs": ["\ufffd\ufffd", "\ufffd\ufffd\ufffd\ufffd\ufffd", "\ufffd\n\u0602", ""]}, {"chunks": ["", "e0", ""], "outputs": ["", "", "", "\ufffd"]}, {"chunks": ["9b61", "9fabedf4ad86d6619882a9"], "outputs": ["\ufffda", "\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffda\ufffd\ufffd\ufffd", ""]}, {"chunks": ["da", "55c3e20ae7ac87d6c89f"], "outputs": ["", "\ufffdU\ufffd\ufffd\n\u7b07\ufffd\u021f", ""]}, {"chunks": ["cfc35b88069fcc9f91b5ace97f98", "9f6d7f"], "outputs": ["\ufffd\ufffd[\ufffd\u0006\ufffd\u031f\ufffd\ufffd\ufffd\ufffd\u007f\ufffd", "\ufffdm\u007f", ""]}, {"chunks": [""], "outputs": ["", ""]}, {"chunks": ["", "9f7fbc7fe0d47daae2", "a9"], "outputs": ["", "\ufffd\u007f\ufffd\u007f\ufffd\ufffd}\ufffd", "", "\ufffd"]}, {"chunks": ["f0c89f98cf98f30aa9f0ed9f75820aac"], "outputs": ["\ufffd\u021f\ufffd\u03d8\ufffd\n\ufffd\ufffd\ufffdu\ufffd\n\ufffd", ""]}, {"chunks": ["e0", "6082f000", "f4e28000", "f3a98080a6f4", "c3f5acc37f"], "outputs": ["", "\ufffd`\ufffd\ufffd\u0000", "\ufffd\ufffd\u0000", "\udb64\udc00\ufffd", "\ufffd\ufffd\ufffd\ufffd\ufffd\u007f", ""]}, {"chunks": ["c3", "52800ae2", "e5", "98e2e282d382", "a9"], "outputs": ["", "\ufffdR\ufffd\n", "\ufffd", "\ufffd\ufffd\ufffd\u04c2", "\ufffd", ""]}, {"chunks": ["", "61", "00", "bb", ""], "outputs": ["", "a", "\u0000", "\ufffd", "", ""]}, {"chunks": ["839395d17fc3e60c", "9882c8980a827faaf1c6f2"], "outputs": ["\ufffd\ufffd\ufffd\ufffd\u007f\ufffd\ufffd\f", "\ufffd\ufffd\u0218\n\ufffd\u007f\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["30", "e6", "61be80007ff2"], "outputs": ["0", "", "\ufffda\ufffd\ufffd\u0000\u007f", "\ufffd"]}, {"chunks": ["d70a80", "00f080e27fa936", "0a3e"], "outputs": ["\ufffd\n\ufffd", "\u0000\ufffd\ufffd\ufffd\u007f\ufffd6", "\n>", ""]}, {"chunks": ["d9", "7f", "c4ac98ab800ad880f0", "aa9f80c3", ""], "outputs": ["", "\ufffd\u007f", "\u012c\ufffd\ufffd\ufffd\n\u0600", "\ud869\udfc0", "", "\ufffd"]}, {"chunks": ["c198"], "outputs": ["\ufffd\ufffd", ""]}, {"chunks": ["82", "0aa5", "db"], "outputs": ["\ufffd", "\n\ufffd", "", "\ufffd"]}, {"chunks": ["9fc1a9e3370af0e9bc", "33d0c3f1", "00d8ef6180", "cb"], "outputs": ["\ufffd\ufffd\ufffd\ufffd7\n\ufffd", "\ufffd3\ufffd\ufffd", "\ufffd\u0000\ufffd\ufffda\ufffd", "", "\ufffd"]}, {"chunks": ["f160", "828282"], "outputs": ["\ufffd`", "\ufffd\ufffd\ufffd", ""]}, {"chunks": ["a1610cab80", "a9ac0a"], "outputs": ["\ufffda\f\ufffd\ufffd", "\ufffd\ufffd\n", ""]}, {"chunks": ["ec"], "outputs": ["", "\ufffd"]}, {"chunks": ["e89f", "80e4c3", "edcad098c00a", "c375acc3"], "outputs": ["", "\u87c0\ufffd", "\ufffd\ufffd\ufffd\u0418\ufffd\n", "\ufffdu\ufffd", "\ufffd"]}, {"chunks": ["", "98d9dd98", "f87f", ""], "outputs": ["", "\ufffd\ufffd\u0758", "\ufffd\u007f", "", ""]}, {"chunks": ["82e2399800dc8053"], "outputs": ["\ufffd\ufffd9\ufffd\u0000\u0700S", ""]}, {"chunks": ["9fa909e8f940ace6ed989fe1eef87f", "c4d66182", "0a00e382"], "outputs": ["\ufffd\ufffd\t\ufffd\ufffd@\ufffd\ufffd\ud61f\ufffd\ufffd\ufffd\u007f", "\ufffd\ufffda\ufffd", "\n\u0000", "\ufffd"]}, {"chunks": ["ab98", "e2d4", ""], "outputs": ["\ufffd\ufffd", "\ufffd", "", "\ufffd"]}, {"chunks": ["00", "37", "a9e282f1a998", "acf06180e864f2ca"], "outputs": ["\u0000", "7", "\ufffd\ufffd", "\ud965\ude2c\ufffda\ufffd\ufffdd\ufffd", "\ufffd"]}, {"chunks": ["1cb5cf", "e282", "c780cdf6", "a9e23356b5d3d1f0d7"], "outputs": ["\u001c\ufffd", "\ufffd", "\ufffd\u01c0\ufffd\ufffd", "\ufffd\ufffd3V\ufffd\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["ebc3", "e261", "f0de85e20ad4cdb682f000c39fc3eb80", "5f9f", ""], "outputs": ["\ufffd", "\ufffd\ufffda", "\ufffd\u0785\ufffd\n\ufffd\u0376\ufffd\ufffd\u0000\u00df\ufffd", "\ufffd_\ufffd", "", ""]}, {"chunks": ["c373179ff200829f71a998d89e"], "outputs": ["\ufffds\u0017\ufffd\ufffd\u0000\ufffd\ufffdq\ufffd\ufffd\u061e", ""]}, {"chunks": ["d4fd0a82f3f4dd8dee8098a9", "98", "d100f0", "9a7f"], "outputs": ["\ufffd\ufffd\n\ufffd\ufffd\ufffd\u074d\ue018\ufffd", "\ufffd", "\ufffd\u0000", "\ufffd\u007f", ""]}, {"chunks": ["", "ddccc061", "f4f000c382ca"], "outputs": ["", "\ufffd\ufffd\ufffda", "\ufffd\ufffd\u0000\u00c2", "\ufffd"]}, {"chunks": ["b782d2f080a79f7fd65d00d1cf98e8df0acfac", "0adc82"], "outputs": ["\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\u007f\ufffd]\u0000\ufffd\u03d8\ufffd\ufffd\n\u03ec", "\n\u0702", ""]}, {"chunks": ["", "184f61", "dc9b8b92", "4a0ac3c2df", "e2"], "outputs": ["", "\u0018Oa", "\u071b\ufffd\ufffd", "J\n\ufffd\ufffd", "\ufffd", "\ufffd"]}, {"chunks": ["80a3", "00f0f7d782dc", "df82e8e2c97f3b57a961"], "outputs": ["\ufffd\ufffd", "\u0000\ufffd\ufffd\u05c2", "\ufffd\u07c2\ufffd\ufffd\ufffd\u007f;W\ufffda", ""]}, {"chunks": ["", "aeea23c3827f9f2cb6", "c4da"], "outputs": ["", "\ufffd\ufffd#\u00c2\u007f\ufffd,\ufffd", "\ufffd", "\ufffd"]}, {"chunks": ["f0f3", "d9ac0af079", "f5d4f0bf"], "outputs": ["\ufffd", "\ufffd\u066c\n\ufffdy", "\ufffd\ufffd", "\ufffd"]}, {"chunks": ["c3ef", "fd", "61f0e5617f6a0ae19f", ""], "outputs": ["\ufffd", "\ufffd\ufffd", "a\ufffd\ufffda\u007fj\n", "", "\ufffd"]}, {"chunks": ["", "a9d57fdb38cf23", "0a0ae22c6161de", ""], "outputs": ["", "\ufffd\ufffd\u007f\ufffd8\ufffd#", "\n\n\ufffd,aa", "", "\ufffd"]}, {"chunks": ["0aac", "82c1a9edbaacd07fe991"], "outputs": ["\n\ufffd", "\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd\u007f", "\ufffd"]}, {"chunks": ["0aac2c0af0a97f00", "0a", "cca5d7", "809fcb9f98f0e382f498e7"], "outputs": ["\n\ufffd,\n\ufffd\u007f\u0000", "\n", "\u0325", "\u05c0\ufffd\u02df\ufffd\ufffd\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["0da9b8c380c3fd", "b50ddd59", "7f61", ""], "outputs": ["\r\ufffd\ufffd\u00c0\ufffd\ufffd", "\ufffd\r\ufffdY", "\u007fa", "", ""]}, {"chunks": ["a0eea99f5ddff5", "9f", "e2", "f48082"], "outputs": ["\ufffd\uea5f]\ufffd\ufffd", "\ufffd", "", "\ufffd", "\ufffd"]}, {"chunks": ["", "d7"], "outputs": ["", "", "\ufffd"]}, {"chunks": ["f061980aedc0c2ad00f3deb961c65f", "f6"], "outputs": ["\ufffda\ufffd\n\ufffd\ufffd\u00ad\u0000\ufffd\u07b9a\ufffd_", "\ufffd", ""]}, {"chunks": ["d4", "ebc3", "739ecb"], "outputs": ["", "\ufffd\ufffd", "\ufffds\ufffd", "\ufffd"]}, {"chunks": ["", "c8", "bfac", "00", ""], "outputs": ["", "", "\u023f\ufffd", "\u0000", "", ""]}, {"chunks": ["", "9f", ""], "outputs": ["", "\ufffd", "", ""]}, {"chunks": ["800af380ac0ab7c7fba2e8f09882e2"], "outputs": ["\ufffd\n\ufffd\n\ufffd\ufffd\ufffd\ufffd\ufffd\ufffd", "\ufffd"]}, {"chunks": ["2f7f9fa9a15bac", "98", "a95ba1c3", "00", "989800"], "outputs": ["/\u007f\ufffd\ufffd\ufffd[\ufffd", "\ufffd", "\ufffd[\ufffd", "\ufffd\u0000", "\ufffd\ufffd\u0000", ""]}, {"chunks": ["a9", "f598dc", "86", "acc1e2d6a9ace2", "4d98c0e6"], "outputs": ["\ufffd", "\ufffd\ufffd", "\u0706", "\ufffd\ufffd\ufffd\u05a9\ufffd", "\ufffdM\ufffd\ufffd", "\ufffd"]}, {"chunks": ["807fcff0820ab1e2", "809f", "619fa988c30cc4"], "outputs": ["\ufffd\u007f\ufffd\ufffd\ufffd\n\ufffd", "\u201f", "a\ufffd\ufffd\ufffd\ufffd\f", "\ufffd"]}, {"chunks": ["", "98", "a8", "af", ""], "outputs": ["", "\ufffd", "\ufffd", "\ufffd", "", ""]}, {"chunks": ["c582e28961f0", "b6e280df0ae4d200c8f0", ""], "outputs": ["\u0142\ufffda", "\ufffd\ufffd\ufffd\n\ufffd\ufffd\u0000\ufffd", "", "\ufffd"]}, {"chunks": ["650010618000809f008a009882", "d0982ae761c3a90a61c8"], "outputs": ["e\u0000\u0010a\ufffd\u0000\ufffd\ufffd\u0000\ufffd\u0000\ufffd\ufffd", "\u0418*\ufffda\u00e9\na", "\ufffd"]}, {"chunks": ["", ""], "outputs": ["", "", ""]}, {"chunks": ["7fa946d4f3618614ce7ff013d3"], "outputs": ["\u007f\ufffdF\ufffd\ufffda\ufffd\u0014\ufffd\u007f\ufffd\u0013", "\ufffd"]}, {"chunks": ["98e51f9f986ea9", "6186dac79f00a1"], "outputs": ["\ufffd\ufffd\u001f\ufffd\ufffdn\ufffd", "a\ufffd\ufffd\u01df\u0000\ufffd", ""]}, {"chunks": ["", "eda0807a"], "outputs": ["", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["ed", "a0807a"], "outputs": ["", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["eda0", "807a"], "outputs": ["", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["eda080", "7a"], "outputs": ["\ufffd\ufffd\ufffd", "z", ""]}, {"chunks": ["", "f49080807a"], "outputs": ["", "\ufffd\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["f4", "9080807a"], "outputs": ["", "\ufffd\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["f490", "80807a"], "outputs": ["\ufffd\ufffd", "\ufffd\ufffdz", ""]}, {"chunks": ["f49080", "807a"], "outputs": ["\ufffd\ufffd\ufffd", "\ufffdz", ""]}, {"chunks": ["f4908080", "7a"], "outputs": ["\ufffd\ufffd\ufffd\ufffd", "z", ""]}, {"chunks": ["", "c0af7a"], "outputs": ["", "\ufffd\ufffdz", ""]}, {"chunks": ["c0", "af7a"], "outputs": ["\ufffd", "\ufffdz", ""]}, {"chunks": ["c0af", "7a"], "outputs": ["\ufffd\ufffd", "z", ""]}, {"chunks": ["", "e080af7a"], "outputs": ["", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["e0", "80af7a"], "outputs": ["", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["e080", "af7a"], "outputs": ["\ufffd\ufffd", "\ufffdz", ""]}, {"chunks": ["e080af", "7a"], "outputs": ["\ufffd\ufffd\ufffd", "z", ""]}, {"chunks": ["", "f09f987a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["f0", "9f987a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["f09f", "987a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["f09f98", "7a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["", "e2827a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["e2", "827a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["e282", "7a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["", "efbfbd7a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["ef", "bfbd7a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["efbf", "bd7a"], "outputs": ["", "\ufffdz", ""]}, {"chunks": ["efbfbd", "7a"], "outputs": ["\ufffd", "z", ""]}, {"chunks": ["", "f8888080807a"], "outputs": ["", "\ufffd\ufffd\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["f8", "888080807a"], "outputs": ["\ufffd", "\ufffd\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["f888", "8080807a"], "outputs": ["\ufffd\ufffd", "\ufffd\ufffd\ufffdz", ""]}, {"chunks": ["f88880", "80807a"], "outputs": ["\ufffd\ufffd\ufffd", "\ufffd\ufffdz", ""]}, {"chunks": ["f8888080", "807a"], "outputs": ["\ufffd\ufffd\ufffd\ufffd", "\ufffdz", ""]}, {"chunks": ["f888808080", "7a"], "outputs": ["\ufffd\ufffd\ufffd\ufffd\ufffd", "z", ""]}], "dumps": ["{\"session\":null,\"events\":[]}", "{\"error\":\"exec session not found: x\\\"\\\\\\n\\t\\u0001\\u007f \\u00e9 \\u2603 \\ud83d\\ude00\",\"error_code\":\"node_active_exec_deferred\",\"retryable\":true}", "{\"z\":1,\"a\":[1.5,0.1,1e+16,1e-05,-0.0,12345678901234567890,2.5e-07],\"m\":{\"b\":null,\"a\":false}}"], "isoformat": [{"micros": 0, "text": "1970-01-01T00:00:00+00:00"}, {"micros": 1, "text": "1970-01-01T00:00:00.000001+00:00"}, {"micros": 999999, "text": "1970-01-01T00:00:00.999999+00:00"}, {"micros": 1000000, "text": "1970-01-01T00:00:01+00:00"}, {"micros": 1791288001000250, "text": "2026-10-06T12:00:01.000250+00:00"}, {"micros": 951782400000000, "text": "2000-02-29T00:00:00+00:00"}, {"micros": 4102444799999999, "text": "2099-12-31T23:59:59.999999+00:00"}, {"micros": 1234567890123456, "text": "2009-02-13T23:31:30.123456+00:00"}], "keys": {"session": ["id", "spec", "argv", "status", "exit_code", "stdin_open", "created_at", "updated_at", "final_sequence"], "spec": ["sandbox_id", "command", "env", "working_dir", "stdin", "tty"], "event": ["sequence", "stream", "data", "exit_code", "created_at"]}}"##;

const GENERATOR: &str = r##""""Golden exec data from the Python node agent for runtime/noded/tests/exec_golden.rs.

Run from the repository root with its Python: `.venv/bin/python golden.py`.
Prints one JSON document, keys in insertion order. Every case goes through the real code:
- R1: `NodeAgentHandler._start_exec` with a real `ExecSessionManager` and a real
  `DirectExecRuntime`/`DirectRunscWarden.exec_lease`; only the lifecycle, the
  capacity lease and the warden's journal/flock are stubbed, and the runtime
  stops the start (a RuntimeError) once it has built the argv;
- R3: `NodeAgentHandler._exec_events` with a recording manager;
- R4: `NodeAgentHandler._write_exec_stdin` with a recording manager;
- R6: `NodeAgentHandler._signal_exec` with a real, empty `ExecSessionManager`;
- decode: `codecs.getincrementaldecoder("utf-8")(errors="replace")`;
- dumps: `json.dumps(payload, separators=(",", ":"))`;
- isoformat: `datetime.isoformat()` of aware UTC datetimes;
- keys: `ExecSession.to_dict()` and `ExecEvent.to_dict()` key order.
"""
import codecs
import json
import random
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Condition
from types import SimpleNamespace
from urllib.parse import urlparse

from ucloud_sandboxes.direct_warden import DirectRunscWarden
from ucloud_sandboxes.hibernation import HibernationAuthority
from ucloud_sandboxes.node_agent import NodeAgentHandler
from ucloud_sandboxes.node_runtime import DirectExecRuntime
from ucloud_sandboxes.sandbox_exec import ExecEvent, ExecSession, ExecSessionManager, SandboxExecSpec
from ucloud_sandboxes.telemetry import Telemetry

RUNSC = "/usr/local/bin/runsc"
ROOT = "/run/ucloud/runsc root"
CONTAINER = "sbx-1.sandbox-3"


class Built(RuntimeError):
    """Stops the start once the argv exists."""


def handler(**attributes):
    instance = NodeAgentHandler.__new__(NodeAgentHandler)
    instance.headers = {}
    instance.telemetry = Telemetry.disabled("golden")
    written = []
    instance._write_json = lambda payload, status=200, headers=None: written.append(
        {"status": int(status), "body": payload, "headers": headers or {}})
    for key, value in attributes.items():
        setattr(instance, key, value)
    return instance, written


def direct_runtime(sandbox_id):
    sandbox = SimpleNamespace(sandbox_id=sandbox_id, container_id=CONTAINER)
    warden = DirectRunscWarden.__new__(DirectRunscWarden)
    warden.config = SimpleNamespace(runsc=Path(RUNSC), runtime_root=Path(ROOT), pause_tier=False)
    warden._locked = lambda _sandbox: nullcontext()
    warden._require_state = lambda _sandbox, _state: SimpleNamespace(authority=HibernationAuthority.LIVE)
    warden._thaw_locked = lambda _sandbox, prefetch: None
    owner = SimpleNamespace(
        _acquire_exec_start=lambda _id: object(),
        service=SimpleNamespace(
            _require_registration=lambda _id: SimpleNamespace(to_direct_sandbox=lambda: sandbox),
            warden=warden,
        ),
        _record_exec_start_timing=lambda *_args: None,
        _attach_exec_lease=lambda _id, _lease: None,
        _release_exec_start_lock=lambda _id, _lock: None,
    )
    return DirectExecRuntime(owner)


def start_case(name, body, query="", sandbox_id="sbx-1"):
    captured = {}
    runtime = direct_runtime(sandbox_id)

    class Runtime:
        def exec_command(self, *args, **kwargs):
            captured["argv"] = list(runtime.exec_command(*args, **kwargs))
            raise Built("built")

        def exec_start_failed(self, _id):
            pass

    sandbox_manager = SimpleNamespace(
        lifecycle=SimpleNamespace(acquire_shared=lambda _id: None, release_shared=lambda _id: None),
        acquire_exec_capacity=lambda _id: object(),
        release_exec_capacity=lambda _lease: None,
        runtime=Runtime(),
    )
    manager = ExecSessionManager(sandbox_manager)
    real_start = manager.start

    def start(spec, session_prefix=None):
        captured["spec"] = spec.to_dict()
        return real_start(spec, session_prefix=session_prefix)

    manager.start = start
    instance, written = handler(exec_manager=manager, _read_json_body=lambda: json.loads(body))
    instance._start_exec(f"/v1/sandboxes/{sandbox_id}/exec", query)
    [reply] = written
    case = {"name": name, "sandbox_id": sandbox_id, "body": body, "query": query}
    if reply["body"].get("error") == "built":
        case["argv"] = captured["argv"]
        case["spec"] = json.dumps(captured["spec"], separators=(",", ":"))
    else:
        case["status"] = reply["status"]
        case["error"] = reply["body"]["error"]
    return case


def body(command=("sh", "-c", "echo hi"), env=None, working_dir=None, stdin=False, tty=False, **extra):
    raw = {"command": list(command) if isinstance(command, tuple) else command, "env": env if env is not None else {},
           "working_dir": working_dir, "stdin": stdin, "tty": tty}
    raw.update(extra)
    return json.dumps(raw)


START_CASES = [
    ("plain", body()),
    ("env sorted, cwd", body(env={"ZED": "1", "A_B": "x=y", "_u": "", "a": "é ☃"}, working_dir="/work dir")),
    ("stdin", body(command=["cat"], stdin=True)),
    ("unicode command", body(command=["printf", "%s", "naïve 😀", ""])),
    ("env order kept in spec", body(env={"B": "2", "A": "1"})),
    ("duplicate env key", '{"command":["true"],"env":{"A":"1","B":"2","A":"3"},"working_dir":null,"stdin":false,"tty":false}'),
    ("wait", body(), "initial_wait_seconds=0.05"),
    ("wait zero", body(), "initial_wait_seconds=0"),
    ("wait forms", body(), "initial_wait_seconds=+.01"),
    ("wait exponent", body(), "initial_wait_seconds=1e-2"),
    ("wait percent", body(), "initial_wait_seconds=%30.01"),
    ("wait underscore", body(), "initial_wait_seconds=0.0_1"),
    ("wait whitespace", body(), "initial_wait_seconds=%200.02%09"),
    ("wait blank first", body(), "initial_wait_seconds=&initial_wait_seconds=0.01"),
    ("wait other keys", body(), "a=1&initial_wait_seconds=0.03&initial_wait_seconds=9"),
    ("not an object", "[]"),
    ("string", '"x"'),
    ("missing key", '{"command":["a"],"env":{},"working_dir":null,"stdin":false}'),
    ("extra key", body(extra=1)),
    ("command not a list", body(command="ls")),
    ("command item", '{"command":["a",1],"env":{},"working_dir":null,"stdin":false,"tty":false}'),
    ("env not a map", '{"command":["a"],"env":[],"working_dir":null,"stdin":false,"tty":false}'),
    ("env value", '{"command":["a"],"env":{"A":1},"working_dir":null,"stdin":false,"tty":false}'),
    ("working_dir type", body(working_dir=1)),
    ("stdin type", body(stdin=1)),
    ("tty type", body(tty="true")),
    ("wait word", body(), "initial_wait_seconds=x"),
    ("wait quote", body(), "initial_wait_seconds=x'y"),
    ("wait both quotes", body(), "initial_wait_seconds=a%22b'c"),
    ("wait control", body(), "initial_wait_seconds=%01%5C%7F"),
    ("wait blank", body(), "initial_wait_seconds=+"),
    ("wait underscores", body(), "initial_wait_seconds=0__1"),
    ("wait big", body(), "initial_wait_seconds=0.06"),
    ("wait negative", body(), "initial_wait_seconds=-0.01"),
    ("wait nan", body(), "initial_wait_seconds=nan"),
    ("wait inf", body(), "initial_wait_seconds=inf"),
    ("wait before validate", body(command=[]), "initial_wait_seconds=1"),
    ("empty command", body(command=[])),
    ("nul command", body(command=["a\0"])),
    ("nul env key", body(env={"A\0": "1"})),
    ("nul env value", body(env={"A": "\0"})),
    ("nul working_dir", body(working_dir="/a\0")),
    ("empty sandbox id", body(), "", ""),
    ("tty", body(tty=True)),
    ("tty before cwd", body(tty=True, working_dir="rel")),
    ("relative cwd", body(working_dir="rel")),
    ("empty cwd", body(working_dir="")),
    ("cwd before env", body(working_dir="rel", env={"1": "x"})),
    ("env digit first", body(env={"1A": "x"})),
    ("env dash", body(env={"A-B": "x"})),
    ("env empty name", body(env={"": "x"})),
    ("env unicode name", body(env={"É": "x"})),
]


def events_case(query):
    seen = {}

    class Manager:
        def events_after(self, session_id, *, after, limit, wait_seconds):
            seen.update(after=after, limit=limit, wait=wait_seconds)
            return []

        def get(self, _session_id):
            return None

    instance, written = handler(exec_manager=Manager())
    try:
        instance._exec_events(urlparse(f"/v1/exec/s/events?{query}"))
    except ValueError as error:
        return {"query": query, "drops_connection": str(error)}
    clamp = lambda value: max(-(2 ** 63), min(2 ** 63 - 1, value))
    return {"query": query, "after": clamp(seen["after"]), "limit": clamp(seen["limit"]), "wait": seen["wait"]}


EVENTS_QUERIES = [
    "", "after=3&limit=7&wait_seconds=1.5", "after=007&limit=+5", "after=%2012%09&limit=-5", "after=1_000",
    "after=_1", "after=1_", "after=1__0", "after=--5", "after=1.0", "after=1+2", "after=%31", "after=",
    "after=x&limit=y", "after=" + "9" * 4300, "after=" + "9" * 4301, "after=99999999999999999999",
    "after=-99999999999999999999", "after=5&after=6", "wait_seconds=0", "wait_seconds=31", "wait_seconds=-1",
    "wait_seconds=nan", "wait_seconds=-inf", "wait_seconds=Infinity", "wait_seconds=1_0", "wait_seconds=1e1",
    "wait_seconds=.5", "wait_seconds=5.", "wait_seconds=+2.5e-1", "wait_seconds=1e400", "wait_seconds=%202%20",
    "wait_seconds=x", "wait_seconds=0x10", "wait_seconds=1_e1", "wait_seconds=1._5", "wait_seconds=e1",
    "limit=0&wait_seconds=29.999",
]


def stdin_case(raw):
    calls = []
    session = SimpleNamespace(to_dict=lambda: {})

    class Manager:
        def write_stdin(self, _session_id, data):
            calls.append(["write", data])
            return session

        def close_stdin(self, _session_id):
            calls.append(["close"])
            return session

    instance, written = handler(exec_manager=Manager(), _read_json_body=lambda: json.loads(raw))
    instance._write_exec_stdin("/v1/exec/s/stdin")
    [reply] = written
    if reply["status"] != 200:
        return {"body": raw, "status": reply["status"], "error": reply["body"]["error"]}
    data = calls[0][1]
    return {"body": raw, "data": data, "eof": calls[1:] == [["close"]]}


STDIN_BODIES = [
    '{"data":"x"}', '{"data":"x","eof":true}', '{"data":true}', '{"data":false}', '{"data":0}', '{"data":12}',
    '{"data":-3}', '{"data":1.5}', '{"data":1e20}', '{"data":1e16}', '{"data":0.0001}', '{"data":-0.0}',
    '{"data":null}', '{}', '{"data":"","eof":1}', '{"eof":"x"}', '{"eof":0}', '{"eof":[]}', '{"eof":{}}',
    '{"eof":[0]}', '{"eof":0.0}', '{"other":1,"data":"é"}', '[]', '"x"', 'null', '{"data":[]}', '{"data":{}}',
    '{"data":123456789012345678901234}',
]


def signal_case(raw):
    instance, written = handler(exec_manager=ExecSessionManager(SimpleNamespace()),
                                _read_json_body=lambda: json.loads(raw))
    instance._signal_exec("/v1/exec/s/signal")
    [reply] = written
    error = reply["body"]["error"]
    if error == "exec session not found: s":
        return {"body": raw, "signal": json.loads(raw)["signal"]}
    return {"body": raw, "status": reply["status"], "error": error}


SIGNAL_BODIES = [
    '{"signal":15}', '{"signal":1}', '{"signal":64}', '{"signal":0}', '{"signal":65}', '{"signal":-9}',
    '{"signal":true}', '{"signal":9.0}', '{"signal":"9"}', '{"signal":null}', '{"signal":9,"x":1}', '{}',
    '[]', '{"Signal":9}', '{"signal":99999999999999999999}',
]


def decode_cases():
    generator = random.Random(20261006)
    text = "aé€😀\n\x00\x7f".encode("utf-8")
    cases = []
    for index in range(80):
        length = generator.randint(0, 24)
        data = bytes(
            generator.choice(text) if generator.random() < 0.5 else generator.choice(
                [generator.randint(0x80, 0xff), generator.randint(0xc0, 0xf7), generator.randint(0, 0xff)])
            for _ in range(length))
        cuts = sorted(generator.sample(range(length + 1), min(length + 1, generator.randint(0, 4))))
        chunks, start = [], 0
        for cut in cuts + [length]:
            chunks.append(data[start:cut])
            start = cut
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        outputs = [decoder.decode(chunk) for chunk in chunks] + [decoder.decode(b"", final=True)]
        cases.append({"chunks": [chunk.hex() for chunk in chunks], "outputs": outputs})
    for special in [b"\xed\xa0\x80", b"\xf4\x90\x80\x80", b"\xc0\xaf", b"\xe0\x80\xaf", b"\xf0\x9f\x98",
                    b"\xe2\x82", b"\xef\xbf\xbd", b"\xf8\x88\x80\x80\x80"]:
        for cut in range(len(special) + 1):
            decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
            chunks = [special[:cut], special[cut:] + b"z"]
            outputs = [decoder.decode(chunk) for chunk in chunks] + [decoder.decode(b"", final=True)]
            cases.append({"chunks": [chunk.hex() for chunk in chunks], "outputs": outputs})
    return cases


DUMPS = [
    {"session": None, "events": []},
    {"error": "exec session not found: x\"\\\n\t\x01\x7f é ☃ 😀", "error_code": "node_active_exec_deferred",
     "retryable": True},
    {"z": 1, "a": [1.5, 0.1, 1e16, 1e-05, -0.0, 12345678901234567890, 2.5e-7], "m": {"b": None, "a": False}},
]


def isoformat_cases():
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    micros = [0, 1, 999999, 1_000_000, 1_791_288_001_000_250, 951_782_400_000_000, 4_102_444_799_999_999,
              1_234_567_890_123_456]
    return [{"micros": value, "text": (epoch + timedelta(microseconds=value)).isoformat()} for value in micros]


def keys():
    spec = SandboxExecSpec(sandbox_id="s", command=("true",))
    now = datetime.now(timezone.utc)
    session = ExecSession(id="i", spec=spec, argv=("a",), status="running", created_at=now, updated_at=now,
                          condition=Condition())
    return {"session": list(session.to_dict()), "spec": list(spec.to_dict()),
            "event": list(ExecEvent(sequence=1, stream="status").to_dict())}


print(json.dumps({
    "runsc": RUNSC,
    "runtime_root": ROOT,
    "container_id": CONTAINER,
    "start": [start_case(*case) for case in START_CASES],
    "events": [events_case(query) for query in EVENTS_QUERIES],
    "stdin": [stdin_case(raw) for raw in STDIN_BODIES],
    "signal": [signal_case(raw) for raw in SIGNAL_BODIES],
    "decode": decode_cases(),
    "dumps": [json.dumps(value, separators=(",", ":")) for value in DUMPS],
    "isoformat": isoformat_cases(),
    "keys": keys(),
}))
"##;
