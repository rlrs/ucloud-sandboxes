//! Python's `json.dumps(value, sort_keys=True, separators=(",", ":"))` byte for
//! byte, including `ensure_ascii` escapes and float `repr`: the encoding behind
//! the spec fingerprint (`sandbox_spec_fingerprint`) and the registry rows that
//! Python re-encodes and compares on every read.

use serde_json::{Number, Value};
use sha2::{Digest, Sha256};

pub fn dumps(value: &Value) -> String {
    let mut out = String::new();
    write(value, &mut out);
    out
}

pub fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|b| format!("{b:02x}")).collect()
}

/// `sandbox_spec_fingerprint`: sha256 of the canonical spec dict.
pub fn fingerprint(spec: &Value) -> String {
    sha256_hex(&dumps(spec))
}

fn write(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(true) => out.push_str("true"),
        Value::Bool(false) => out.push_str("false"),
        Value::Number(number) => out.push_str(&number_repr(number)),
        Value::String(text) => write_string(text, out),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write(&map[key], out);
            }
            out.push('}');
        }
    }
}

fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 || (c as u32) > 0x7e => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Integers as written; floats as Python's `repr(float)`.
pub fn number_repr(number: &Number) -> String {
    if let Some(value) = number.as_i64() {
        return value.to_string();
    }
    if let Some(value) = number.as_u64() {
        return value.to_string();
    }
    float_repr(number.as_f64().unwrap_or(f64::NAN))
}

/// Python `repr(float)`: shortest round-trip digits; scientific notation when
/// the decimal exponent is below -4 or at least 16 (`1e-05`, `1e+16`), else
/// positional with at least one fractional digit (`2.0`, `0.0001`).
pub fn float_repr(value: f64) -> String {
    if value.is_nan() {
        return "NaN".into();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity".into() } else { "-Infinity".into() };
    }
    if value == 0.0 {
        return if value.is_sign_negative() { "-0.0".into() } else { "0.0".into() };
    }
    // Rust's `{:e}` is the shortest round-trip form: digits and exponent.
    let formatted = format!("{value:e}");
    let (mantissa, exponent) = formatted.split_once('e').expect("exponent present");
    let exponent: i32 = exponent.parse().expect("integer exponent");
    let negative = mantissa.starts_with('-');
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let sign = if negative { "-" } else { "" };
    if !(-4..16).contains(&exponent) {
        let (head, tail) = digits.split_at(1);
        let fraction = if tail.is_empty() { String::new() } else { format!(".{tail}") };
        let exp_sign = if exponent < 0 { '-' } else { '+' };
        return format!("{sign}{head}{fraction}e{exp_sign}{:02}", exponent.abs());
    }
    let point = exponent + 1; // digits before the decimal point
    let body = if point <= 0 {
        format!("0.{}{}", "0".repeat((-point) as usize), digits)
    } else if (point as usize) >= digits.len() {
        format!("{}{}.0", digits, "0".repeat(point as usize - digits.len()))
    } else {
        let (whole, fraction) = digits.split_at(point as usize);
        format!("{whole}.{fraction}")
    };
    format!("{sign}{body}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn floats_match_python_repr() {
        for (value, expected) in [
            (1.0, "1.0"), (2.5, "2.5"), (0.1, "0.1"), (1e-05, "1e-05"), (0.0001, "0.0001"),
            (1e16, "1e+16"), (1e15, "1000000000000000.0"), (123456789.125, "123456789.125"),
            (-0.5, "-0.5"), (1.5e-07, "1.5e-07"), (2.0e22, "2e+22"), (0.30000000000000004, "0.30000000000000004"),
            (0.0, "0.0"),
        ] {
            assert_eq!(float_repr(value), expected, "{value}");
        }
    }

    #[test]
    fn dumps_matches_python() {
        let value: Value = serde_json::from_str(r#"{"cpus":1.0,"b":[1,2.5,"\u00e9"],"a":null,"x":1e-05}"#).unwrap();
        assert_eq!(dumps(&value), r#"{"a":null,"b":[1,2.5,"\u00e9"],"cpus":1.0,"x":1e-05}"#);
        assert_eq!(dumps(&json!({"k": "\u{7f}"})), r#"{"k":"\u007f"}"#);
    }

    #[test]
    fn spec_fingerprint_matches_python() {
        // SandboxSpec(...).to_dict() and sandbox_spec_fingerprint from the repo's Python.
        let spec: Value = serde_json::from_str(r#"{"id": "rlbench-1", "image": "10.36.101.16:5000/ucloud-managed/x:latest@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "profile": "container", "command": [], "env": {"LANG": "C.UTF-8", "NOTE": "\u00e9"}, "working_dir": null, "memory_mb": 2048, "cpus": 1, "disk_mb": 5120, "network": "bridge", "ttl_seconds": null, "parkable": true, "managed_process": true, "ssh": {"enabled": false, "user": "root", "host": "127.0.0.1", "host_port": null, "container_port": 22, "authorized_keys": []}, "security": {"user": "1000:1000", "cap_drop": ["ALL"], "cap_add": [], "no_new_privileges": true, "pids_limit": 256, "read_only_rootfs": false, "init": true}, "filesystem": {"enforce_disk_quota": false, "workspace_path": "/workspace", "tmpfs_mb": 64, "run_tmpfs_mb": 16}, "linux_host": {"enable_cron": false, "enable_sshd": false, "keep_alive": true, "writable_paths": ["/run", "/run/lock", "/run/sshd", "/tmp", "/var/tmp", "/var/run", "/var/lock", "/var/spool/cron", "/var/spool/cron/crontabs", "/etc/cron.d", "/logs", "/logs/agent", "/logs/verifier", "/tests", "/task", "/oracle", "/workspace"]}, "labels": {}}"#).unwrap();
        assert_eq!(fingerprint(&spec), "168984eb3fd4f60469eea108b5bcf5728f3c174adead2910c9b782320d6c5b48");
    }
}
