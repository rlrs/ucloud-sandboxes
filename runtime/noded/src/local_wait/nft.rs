//! The nftables table that logs relay flows' headers (`local_wait.py`
//! `relay_endpoints`, `nft_script`, `install_rules`, `remove_rules`).

use std::io::Write;
use std::net::Ipv4Addr;
use std::process::{Command, Stdio};

use super::packet::Ipv4Net;
use super::{NFLOG_GROUP, SNAPLEN, TABLE};

/// Python 3.10's `IPv4Address.is_private` networks.
const PRIVATE: [(Ipv4Addr, u8); 14] = [
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 0, 0, 0), 29),
    (Ipv4Addr::new(192, 0, 0, 170), 31),
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(240, 0, 0, 0), 4),
    (Ipv4Addr::new(255, 255, 255, 255), 32),
];

pub fn is_private(address: Ipv4Addr) -> bool {
    PRIVATE.iter().any(|&(network, prefix)| Ipv4Net::new(network, prefix).expect("a network").contains(address))
}

/// (IPv4, port) of each relay with a literal private address: the plaintext
/// path. A DNS name is a TLS ingress; its waits keep the relay's park. Sorted
/// as Python sorts its (str, int) tuples, deduplicated.
pub fn relay_endpoints<'a>(relays: impl IntoIterator<Item = (&'a str, u16)>) -> Vec<(Ipv4Addr, u16)> {
    let mut endpoints: Vec<(String, u16, Ipv4Addr)> = relays
        .into_iter()
        .filter_map(|(host, port)| host.parse::<Ipv4Addr>().ok().filter(|address| is_private(*address)).map(|a| (a.to_string(), port, a)))
        .collect();
    endpoints.sort();
    endpoints.dedup();
    endpoints.into_iter().map(|(_, port, address)| (address, port)).collect()
}

/// Idempotent rules that log relay flows' headers, both directions, before any filter.
pub fn nft_script(endpoints: &[(Ipv4Addr, u16)], network: Ipv4Net, group: u16) -> String {
    let mut lines = vec![
        format!("add table inet {TABLE}"),
        format!("add chain inet {TABLE} forward {{ type filter hook forward priority -150; policy accept; }}"),
        format!("flush chain inet {TABLE} forward"),
    ];
    let log = format!("log group {group} snaplen {SNAPLEN} queue-threshold 1");
    for (host, port) in endpoints {
        lines.push(format!("add rule inet {TABLE} forward ip saddr {network} ip daddr {host} tcp dport {port} {log}"));
        lines.push(format!("add rule inet {TABLE} forward ip saddr {host} tcp sport {port} ip daddr {network} {log}"));
    }
    lines.join("\n") + "\n"
}

/// The node's rules: install before the reader runs, remove on stop.
pub trait RuleSet: Send + Sync {
    fn install(&self) -> Result<(), String>;
    fn remove(&self);
}

/// `nft -f -` with the script; `nft delete table inet ucloud_local_wait`.
pub struct NftRules {
    script: String,
}

impl NftRules {
    pub fn new(endpoints: &[(Ipv4Addr, u16)], network: Ipv4Net) -> NftRules {
        NftRules { script: nft_script(endpoints, network, NFLOG_GROUP) }
    }

    pub fn script(&self) -> &str {
        &self.script
    }
}

impl RuleSet for NftRules {
    fn install(&self) -> Result<(), String> {
        let mut child = Command::new("nft")
            .args(["-f", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("cannot run nft: {error}"))?;
        let written = child.stdin.take().expect("piped").write_all(self.script.as_bytes());
        let output = child.wait_with_output().map_err(|error| format!("nft failed: {error}"))?;
        written.map_err(|error| format!("cannot write the nft script: {error}"))?;
        if !output.status.success() {
            return Err(format!("nft -f - failed ({}): {}", output.status, String::from_utf8_lossy(&output.stderr).trim()));
        }
        Ok(())
    }

    fn remove(&self) {
        let _ = Command::new("nft").args(["delete", "table", "inet", TABLE]).stdin(Stdio::null()).output();
    }
}
