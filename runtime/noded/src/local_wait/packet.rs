//! What the node sees of a relay flow: IPv4 and TCP headers, never payloads
//! (`local_wait.py` `RelayPacket`, `parse_packet`).

use std::fmt;
use std::net::Ipv4Addr;

/// An IPv4 network (Python `ipaddress.IPv4Network`, strict: no host bits).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ipv4Net {
    address: Ipv4Addr,
    prefix: u8,
}

/// `direct_network.NETWORK_CIDR`: every sandbox's guest and host address.
pub const NETWORK_CIDR: Ipv4Net = Ipv4Net { address: Ipv4Addr::new(100, 96, 0, 0), prefix: 16 };

impl Ipv4Net {
    pub fn new(address: Ipv4Addr, prefix: u8) -> Option<Ipv4Net> {
        let net = Ipv4Net { address, prefix };
        (prefix <= 32 && u32::from(address) & !net.mask() == 0).then_some(net)
    }

    /// `a.b.c.d/p`.
    pub fn parse(text: &str) -> Option<Ipv4Net> {
        let (address, prefix) = text.split_once('/')?;
        if prefix.is_empty() || prefix.len() > 2 || !prefix.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        Ipv4Net::new(address.parse().ok()?, prefix.parse().ok()?)
    }

    pub fn address(&self) -> Ipv4Addr {
        self.address
    }

    fn mask(&self) -> u32 {
        if self.prefix == 0 { 0 } else { u32::MAX << (32 - self.prefix) }
    }

    pub fn contains(&self, address: Ipv4Addr) -> bool {
        u32::from(address) & self.mask() == u32::from(self.address)
    }
}

impl fmt::Display for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.address, self.prefix)
    }
}

/// One logged relay packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelayPacket {
    pub guest: Ipv4Addr,
    /// Sandbox to relay.
    pub outbound: bool,
    pub payload: usize,
    /// The TCP flags byte.
    pub flags: u8,
}

impl RelayPacket {
    /// An answer's data, FIN or RST: a bare ACK need not thaw anyone.
    pub fn wakes(&self) -> bool {
        !self.outbound && (self.payload > 0 || self.flags & 0x05 != 0)
    }
}

/// A logged IPv4/TCP header as a `RelayPacket`, or `None`.
pub fn parse_packet(packet: &[u8], network: Ipv4Net) -> Option<RelayPacket> {
    if packet.len() < 20 || packet[0] >> 4 != 4 || packet[9] != 6 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0F) * 4;
    if packet.len() < ihl + 14 {
        return None;
    }
    let total = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    let source = Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]);
    let destination = Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]);
    let payload = total.saturating_sub(ihl + usize::from(packet[ihl + 12] >> 4) * 4);
    let flags = packet[ihl + 13];
    if network.contains(source) {
        Some(RelayPacket { guest: source, outbound: true, payload, flags })
    } else if network.contains(destination) {
        Some(RelayPacket { guest: destination, outbound: false, payload, flags })
    } else {
        None
    }
}
