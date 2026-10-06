//! rtnetlink requests for the direct-egress pair, over a raw NETLINK_ROUTE
//! socket: what `ip link add … type veth`, `ip link set`, `ip address replace`
//! and `ip route replace` send, without an `ip` process per step
//! (linux/rtnetlink.h, linux/if_link.h, linux/veth.h).
//!
//! A socket belongs to the network namespace of the thread that opened it,
//! so one opened inside a sandbox namespace configures that namespace from
//! any thread afterwards.

use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::time::{Duration, Instant};

pub const IFF_UP: u32 = 1;

const RTM_NEWLINK: u16 = 16;
const RTM_DELLINK: u16 = 17;
const RTM_GETLINK: u16 = 18;
const RTM_SETLINK: u16 = 19;
const RTM_NEWADDR: u16 = 20;
const RTM_NEWROUTE: u16 = 24;
const NLMSG_ERROR: u16 = 2;

const NLM_F_REQUEST: u16 = 0x1;
const NLM_F_ACK: u16 = 0x4;
const NLM_F_REPLACE: u16 = 0x100;
const NLM_F_EXCL: u16 = 0x200;
const NLM_F_CREATE: u16 = 0x400;

const IFLA_IFNAME: u16 = 3;
const IFLA_MTU: u16 = 4;
const IFLA_LINKINFO: u16 = 18;
const IFLA_NET_NS_FD: u16 = 28;
const IFLA_NUM_TX_QUEUES: u16 = 31;
const IFLA_NUM_RX_QUEUES: u16 = 32;
const IFLA_INFO_KIND: u16 = 1;
const IFLA_INFO_DATA: u16 = 2;
const VETH_INFO_PEER: u16 = 1;

const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;

const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;
const RT_TABLE_MAIN: u8 = 254;
/// `ip route` without `proto`.
const RTPROT_BOOT: u8 = 3;
const RT_SCOPE_UNIVERSE: u8 = 0;
const RTN_UNICAST: u8 = 1;

const NLMSG_HEADER: usize = 16;
const IFINFOMSG: usize = 16;
const RECEIVE_BUFFER: usize = 64 * 1024;
/// rtnl can be held for a while by other work on a busy node; a request
/// that is not answered in this time fails rather than hanging a create.
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

fn align(length: usize) -> usize {
    (length + 3) & !3
}

/// One request: the netlink header, a family header, then attributes.
struct Builder {
    bytes: Vec<u8>,
}

impl Builder {
    fn new(kind: u16, flags: u16) -> Builder {
        let mut bytes = Vec::with_capacity(256);
        bytes.extend_from_slice(&0u32.to_ne_bytes()); // length, set by finish
        bytes.extend_from_slice(&kind.to_ne_bytes());
        bytes.extend_from_slice(&(flags | NLM_F_REQUEST).to_ne_bytes());
        bytes.extend_from_slice(&0u32.to_ne_bytes()); // sequence, set when sent
        bytes.extend_from_slice(&0u32.to_ne_bytes()); // port id: the kernel
        Builder { bytes }
    }

    /// struct ifinfomsg: family, pad, type, index, flags, change.
    fn ifinfomsg(mut self, index: i32, flags: u32, change: u32) -> Builder {
        push_ifinfomsg(&mut self.bytes, index, flags, change);
        self
    }

    fn attr(&mut self, kind: u16, value: &[u8]) {
        push_attr(&mut self.bytes, kind, value);
    }

    fn finish(mut self) -> Vec<u8> {
        let length = self.bytes.len() as u32;
        self.bytes[..4].copy_from_slice(&length.to_ne_bytes());
        self.bytes
    }
}

fn push_ifinfomsg(bytes: &mut Vec<u8>, index: i32, flags: u32, change: u32) {
    bytes.extend_from_slice(&[libc::AF_UNSPEC as u8, 0]);
    bytes.extend_from_slice(&0u16.to_ne_bytes());
    bytes.extend_from_slice(&index.to_ne_bytes());
    bytes.extend_from_slice(&flags.to_ne_bytes());
    bytes.extend_from_slice(&change.to_ne_bytes());
}

fn push_attr(bytes: &mut Vec<u8>, kind: u16, value: &[u8]) {
    bytes.extend_from_slice(&((4 + value.len()) as u16).to_ne_bytes());
    bytes.extend_from_slice(&kind.to_ne_bytes());
    bytes.extend_from_slice(value);
    bytes.resize(align(bytes.len()), 0);
}

/// A nested attribute around what `body` appends. Like iproute2's
/// `addattr_nest`, without NLA_F_NESTED (the kernel accepts both here).
fn push_nested(bytes: &mut Vec<u8>, kind: u16, body: impl FnOnce(&mut Vec<u8>)) {
    let start = bytes.len();
    bytes.extend_from_slice(&[0, 0]);
    bytes.extend_from_slice(&kind.to_ne_bytes());
    body(bytes);
    let length = (bytes.len() - start) as u16;
    bytes[start..start + 2].copy_from_slice(&length.to_ne_bytes());
}

fn name_value(name: &str) -> Vec<u8> {
    let mut value = name.as_bytes().to_vec();
    value.push(0);
    value
}

/// `ip link add HOST numtxqueues 1 numrxqueues 1 mtu MTU type veth peer name
/// PEER numtxqueues 1 numrxqueues 1 mtu MTU netns FD`: both ends down.
///
/// veth allocates a queue pair per possible CPU by default, each a sysfs
/// object with its own uevents (280 per sandbox on a 64-vCPU worker against
/// 31 with one); the sandbox's netstack reads eth0 through one channel.
pub fn new_veth(host: &str, peer: &str, mtu: u32, peer_namespace: RawFd) -> Vec<u8> {
    let link = |bytes: &mut Vec<u8>, name: &str| {
        push_attr(bytes, IFLA_IFNAME, &name_value(name));
        push_attr(bytes, IFLA_NUM_TX_QUEUES, &1u32.to_ne_bytes());
        push_attr(bytes, IFLA_NUM_RX_QUEUES, &1u32.to_ne_bytes());
        push_attr(bytes, IFLA_MTU, &mtu.to_ne_bytes());
    };
    let mut request = Builder::new(RTM_NEWLINK, NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL).ifinfomsg(0, 0, 0);
    link(&mut request.bytes, host);
    push_nested(&mut request.bytes, IFLA_LINKINFO, |bytes| {
        push_attr(bytes, IFLA_INFO_KIND, b"veth");
        push_nested(bytes, IFLA_INFO_DATA, |bytes| {
            push_nested(bytes, VETH_INFO_PEER, |bytes| {
                push_ifinfomsg(bytes, 0, 0, 0);
                link(bytes, peer);
                push_attr(bytes, IFLA_NET_NS_FD, &(peer_namespace as u32).to_ne_bytes());
            });
        });
    });
    request.finish()
}

/// `ip link set dev INDEX [mtu MTU] up`.
pub fn set_link_up(index: i32, mtu: Option<u32>) -> Vec<u8> {
    let mut request = Builder::new(RTM_SETLINK, NLM_F_ACK).ifinfomsg(index, IFF_UP, IFF_UP);
    if let Some(mtu) = mtu {
        request.attr(IFLA_MTU, &mtu.to_ne_bytes());
    }
    request.finish()
}

/// `ip link set dev NAME up`, the device named rather than indexed.
pub fn set_named_link_up(name: &str) -> Vec<u8> {
    let mut request = Builder::new(RTM_SETLINK, NLM_F_ACK).ifinfomsg(0, IFF_UP, IFF_UP);
    request.attr(IFLA_IFNAME, &name_value(name));
    request.finish()
}

/// `ip link show dev NAME`: answered with the link (RTM_NEWLINK), not an ack.
pub fn get_link(name: &str) -> Vec<u8> {
    let mut request = Builder::new(RTM_GETLINK, 0).ifinfomsg(0, 0, 0);
    request.attr(IFLA_IFNAME, &name_value(name));
    request.finish()
}

/// `ip link delete NAME`, by name: no index lookup, so no module autoload.
pub fn delete_link(name: &str) -> Vec<u8> {
    let mut request = Builder::new(RTM_DELLINK, NLM_F_ACK).ifinfomsg(0, 0, 0);
    request.attr(IFLA_IFNAME, &name_value(name));
    request.finish()
}

/// `ip address replace ADDRESS/PREFIX dev INDEX` (IPv4: local = address).
pub fn replace_address(index: i32, address: Ipv4Addr, prefix: u8) -> Vec<u8> {
    let mut request = Builder::new(RTM_NEWADDR, NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE);
    // struct ifaddrmsg: family, prefixlen, flags, scope, index.
    request.bytes.extend_from_slice(&[libc::AF_INET as u8, prefix, 0, RT_SCOPE_UNIVERSE]);
    request.bytes.extend_from_slice(&(index as u32).to_ne_bytes());
    request.attr(IFA_LOCAL, &address.octets());
    request.attr(IFA_ADDRESS, &address.octets());
    request.finish()
}

/// `ip route replace default via GATEWAY dev INDEX`.
pub fn replace_default_route(index: i32, gateway: Ipv4Addr) -> Vec<u8> {
    let mut request = Builder::new(RTM_NEWROUTE, NLM_F_ACK | NLM_F_CREATE | NLM_F_REPLACE);
    // struct rtmsg: family, dst_len, src_len, tos, table, protocol, scope, type, flags.
    request.bytes.extend_from_slice(&[
        libc::AF_INET as u8, 0, 0, 0, RT_TABLE_MAIN, RTPROT_BOOT, RT_SCOPE_UNIVERSE, RTN_UNICAST,
    ]);
    request.bytes.extend_from_slice(&0u32.to_ne_bytes());
    request.attr(RTA_GATEWAY, &gateway.octets());
    request.attr(RTA_OIF, &(index as u32).to_ne_bytes());
    request.finish()
}

/// The kernel's answer to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reply {
    /// NLMSG_ERROR: 0 acknowledges, else a positive errno.
    Ack(i32),
    /// RTM_NEWLINK answering RTM_GETLINK: the link's index.
    Link(i32),
}

fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_ne_bytes([data[offset], data[offset + 1]])
}

fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes(data[offset..offset + 4].try_into().expect("four bytes"))
}

/// The reply to request `seq` among the messages of one read, if any.
pub fn parse_reply(data: &[u8], seq: u32) -> Option<Reply> {
    let mut offset = 0;
    while offset + NLMSG_HEADER <= data.len() {
        let length = u32_at(data, offset) as usize;
        if length < NLMSG_HEADER || offset + length > data.len() {
            break;
        }
        if u32_at(data, offset + 8) == seq {
            match u16_at(data, offset + 4) {
                NLMSG_ERROR if length >= NLMSG_HEADER + 4 => {
                    let code = i32::from_ne_bytes(data[offset + NLMSG_HEADER..offset + NLMSG_HEADER + 4].try_into().expect("four"));
                    return Some(Reply::Ack(code.checked_neg().unwrap_or(libc::EINVAL)));
                }
                RTM_NEWLINK if length >= NLMSG_HEADER + IFINFOMSG => {
                    return Some(Reply::Link(u32_at(data, offset + NLMSG_HEADER + 4) as i32));
                }
                _ => {}
            }
        }
        offset += align(length);
    }
    None
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(result) }
}

/// A NETLINK_ROUTE socket in the opening thread's network namespace.
pub struct RouteSocket {
    fd: OwnedFd,
    seq: u32,
}

impl RouteSocket {
    pub fn open() -> io::Result<RouteSocket> {
        // SAFETY: plain socket creation; the descriptor is owned below.
        let raw = check(unsafe { libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_ROUTE) })?;
        // SAFETY: a fresh descriptor nobody else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: an all-zero sockaddr_nl is valid; the kernel assigns the port id.
        let mut address: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        address.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        let size = std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t;
        // SAFETY: `address` is a valid sockaddr_nl of `size` bytes.
        check(unsafe { libc::bind(fd.as_raw_fd(), (&raw const address).cast(), size) })?;
        // `as _`: musl deprecates naming time_t and suseconds_t.
        let timeout = libc::timeval { tv_sec: 1, tv_usec: 0 as _ };
        let length = std::mem::size_of::<libc::timeval>() as libc::socklen_t;
        // SAFETY: `timeout` is a valid timeval of `length` bytes.
        check(unsafe { libc::setsockopt(fd.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVTIMEO, (&raw const timeout).cast(), length) })?;
        Ok(RouteSocket { fd, seq: 0 })
    }

    /// Send one request and wait for its reply.
    pub fn request(&mut self, mut message: Vec<u8>) -> io::Result<Reply> {
        self.seq = self.seq.wrapping_add(1);
        let seq = self.seq;
        message[8..12].copy_from_slice(&seq.to_ne_bytes());
        // SAFETY: `message` is valid for its length.
        let sent = unsafe { libc::send(self.fd.as_raw_fd(), message.as_ptr().cast(), message.len(), 0) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        if sent as usize != message.len() {
            return Err(io::Error::other("short netlink send"));
        }
        let deadline = Instant::now() + REPLY_TIMEOUT;
        let mut buffer = vec![0u8; RECEIVE_BUFFER];
        loop {
            // SAFETY: `buffer` is writable for its length.
            let read = unsafe { libc::recv(self.fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len(), 0) };
            if read < 0 {
                let error = io::Error::last_os_error();
                if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR)) && Instant::now() < deadline {
                    continue;
                }
                return Err(error);
            }
            if let Some(reply) = parse_reply(&buffer[..read as usize], seq) {
                return Ok(reply);
            }
        }
    }

    /// A request answered by an acknowledgement.
    pub fn acknowledged(&mut self, message: Vec<u8>) -> io::Result<()> {
        match self.request(message)? {
            Reply::Ack(0) => Ok(()),
            Reply::Ack(code) => Err(io::Error::from_raw_os_error(code)),
            Reply::Link(_) => Err(io::Error::other("unexpected link message")),
        }
    }

    /// The index of the link called `name` (ENODEV when there is none).
    pub fn link_index(&mut self, name: &str) -> io::Result<i32> {
        match self.request(get_link(name))? {
            Reply::Link(index) => Ok(index),
            Reply::Ack(0) => Err(io::Error::other("link request was only acknowledged")),
            Reply::Ack(code) => Err(io::Error::from_raw_os_error(code)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(length: u32, kind: u16, flags: u16) -> Vec<u8> {
        let mut bytes = length.to_ne_bytes().to_vec();
        bytes.extend_from_slice(&kind.to_ne_bytes());
        bytes.extend_from_slice(&flags.to_ne_bytes());
        bytes.extend_from_slice(&[0; 8]);
        bytes
    }

    fn attr(kind: u16, value: &[u8]) -> Vec<u8> {
        let mut bytes = ((4 + value.len()) as u16).to_ne_bytes().to_vec();
        bytes.extend_from_slice(&kind.to_ne_bytes());
        bytes.extend_from_slice(value);
        while bytes.len() % 4 != 0 {
            bytes.push(0);
        }
        bytes
    }

    fn ifinfo(index: i32, flags: u32, change: u32) -> Vec<u8> {
        let mut bytes = vec![0u8, 0, 0, 0];
        bytes.extend_from_slice(&index.to_ne_bytes());
        bytes.extend_from_slice(&flags.to_ne_bytes());
        bytes.extend_from_slice(&change.to_ne_bytes());
        bytes
    }

    fn link_attrs(name: &[u8], mtu: u32) -> Vec<u8> {
        [attr(3, name), attr(31, &1u32.to_ne_bytes()), attr(32, &1u32.to_ne_bytes()), attr(4, &mtu.to_ne_bytes())].concat()
    }

    #[test]
    fn veth_request_nests_the_peer_in_its_namespace() {
        let message = new_veth("us7h", "eth0", 1420, 9);
        // Peer: ifinfomsg, then its own name, queues, MTU and namespace.
        let peer = [ifinfo(0, 0, 0), link_attrs(b"eth0\0", 1420), attr(28, &9u32.to_ne_bytes())].concat();
        let peer_attr = attr(1, &peer); // VETH_INFO_PEER
        let data = attr(2, &peer_attr); // IFLA_INFO_DATA
        let linkinfo = attr(18, &[attr(1, b"veth"), data].concat()); // IFLA_LINKINFO
        let body = [ifinfo(0, 0, 0), link_attrs(b"us7h\0", 1420), linkinfo].concat();
        // NLM_F_REQUEST | NLM_F_ACK | NLM_F_EXCL | NLM_F_CREATE.
        let expected = [header((16 + body.len()) as u32, 16, 0x605), body].concat();
        assert_eq!(message, expected);
        assert_eq!(message.len() % 4, 0);
    }

    #[test]
    fn link_address_and_route_requests_match_iproute2() {
        let up = set_link_up(12, Some(1420));
        let body = [ifinfo(12, 1, 1), attr(4, &1420u32.to_ne_bytes())].concat();
        assert_eq!(up, [header(16 + body.len() as u32, 19, 0x5), body].concat());

        let lo = set_named_link_up("lo");
        let body = [ifinfo(0, 1, 1), attr(3, b"lo\0")].concat();
        assert_eq!(lo, [header(16 + body.len() as u32, 19, 0x5), body].concat());

        let get = get_link("eth0");
        let body = [ifinfo(0, 0, 0), attr(3, b"eth0\0")].concat();
        assert_eq!(get, [header(16 + body.len() as u32, 18, 0x1), body].concat());

        let delete = delete_link("us7h");
        let body = [ifinfo(0, 0, 0), attr(3, b"us7h\0")].concat();
        assert_eq!(delete, [header(16 + body.len() as u32, 17, 0x5), body].concat());

        let address = replace_address(12, Ipv4Addr::new(100, 96, 0, 14), 31);
        let mut body = vec![2u8, 31, 0, 0];
        body.extend_from_slice(&12u32.to_ne_bytes());
        body.extend([attr(2, &[100, 96, 0, 14]), attr(1, &[100, 96, 0, 14])].concat());
        // NLM_F_REQUEST | NLM_F_ACK | NLM_F_REPLACE | NLM_F_CREATE.
        assert_eq!(address, [header(16 + body.len() as u32, 20, 0x505), body].concat());

        let route = replace_default_route(3, Ipv4Addr::new(100, 96, 0, 14));
        let mut body = vec![2u8, 0, 0, 0, 254, 3, 0, 1, 0, 0, 0, 0];
        body.extend([attr(5, &[100, 96, 0, 14]), attr(4, &3u32.to_ne_bytes())].concat());
        assert_eq!(route, [header(16 + body.len() as u32, 24, 0x505), body].concat());
    }

    #[test]
    fn replies_are_matched_by_sequence() {
        let error = |seq: u32, code: i32| {
            let mut bytes = header(36, NLMSG_ERROR, 0);
            bytes[8..12].copy_from_slice(&seq.to_ne_bytes());
            bytes.extend_from_slice(&code.to_ne_bytes());
            bytes.extend_from_slice(&[0; 16]); // the request's header
            bytes
        };
        let mut link = header(32, RTM_NEWLINK, 0);
        link[8..12].copy_from_slice(&5u32.to_ne_bytes());
        link.extend(ifinfo(42, 1, 0));
        let read = [error(3, 0), error(4, -libc::ENODEV), link].concat();
        assert_eq!(parse_reply(&read, 3), Some(Reply::Ack(0)));
        assert_eq!(parse_reply(&read, 4), Some(Reply::Ack(libc::ENODEV)));
        assert_eq!(parse_reply(&read, 5), Some(Reply::Link(42)));
        assert_eq!(parse_reply(&read, 6), None);
        // A truncated message ends the walk.
        assert_eq!(parse_reply(&read[..40], 4), None);
    }
}
