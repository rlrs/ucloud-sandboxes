//! One NFLOG group over a raw netlink socket (`local_wait.py` `NflogReader`,
//! `parse_messages`; linux/netfilter/nfnetlink_log.h). Every logged packet
//! arrives at once (queue threshold 1). A group has exactly one binder: the
//! kernel refuses another socket's BIND of a bound group with EPERM, and the
//! caller then runs no local waits at all.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::{Duration, Instant};

use super::SNAPLEN;

const NFNL_SUBSYS_ULOG: u16 = 4;
const NFULNL_MSG_PACKET: u16 = 0;
const NFULNL_MSG_CONFIG: u16 = 1;
const NFULA_CFG_CMD: u16 = 1;
const NFULA_CFG_MODE: u16 = 2;
const NFULA_CFG_QTHRESH: u16 = 5;
const NFULNL_CFG_CMD_BIND: u8 = 1;
const NFULNL_COPY_PACKET: u8 = 2;
const NFULA_PAYLOAD: u16 = 9;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;
const NLMSG_ERROR: u16 = 2;
const NLMSG_HEADER: usize = 16;
const NFGEN_HEADER: usize = 4;
const NLATTR_HEADER: usize = 4;
const RECEIVE_BUFFER: libc::c_int = 4 * 1024 * 1024;
/// A read returns at least this often, so a stop is seen.
pub const READ_TIMEOUT: Duration = Duration::from_millis(200);
const CONFIG_TIMEOUT: Duration = Duration::from_secs(2);

fn align(length: usize) -> usize {
    (length + 3) & !3
}

fn u16_at(data: &[u8], offset: usize) -> u16 {
    u16::from_ne_bytes([data[offset], data[offset + 1]])
}

fn u32_at(data: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes(data[offset..offset + 4].try_into().expect("four bytes"))
}

/// Python `_attribute`: an nlattr, padded to four bytes.
pub fn attribute(kind: u16, value: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(align(NLATTR_HEADER + value.len()));
    bytes.extend_from_slice(&((NLATTR_HEADER + value.len()) as u16).to_ne_bytes());
    bytes.extend_from_slice(&kind.to_ne_bytes());
    bytes.extend_from_slice(value);
    bytes.resize(align(bytes.len()), 0);
    bytes
}

/// Python `_config`: one NFULNL_MSG_CONFIG request for `group`, acked.
pub fn config_message(group: u16, attribute: &[u8], seq: u32) -> Vec<u8> {
    let length = NLMSG_HEADER + NFGEN_HEADER + attribute.len();
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&(length as u32).to_ne_bytes());
    bytes.extend_from_slice(&((NFNL_SUBSYS_ULOG << 8) | NFULNL_MSG_CONFIG).to_ne_bytes());
    bytes.extend_from_slice(&(NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
    bytes.extend_from_slice(&seq.to_ne_bytes());
    bytes.extend_from_slice(&0u32.to_ne_bytes());
    bytes.extend_from_slice(&[libc::AF_UNSPEC as u8, 0]);
    bytes.extend_from_slice(&group.to_be_bytes());
    bytes.extend_from_slice(attribute);
    bytes
}

/// The three requests in Python's order (sequence numbers 1-3): bind the
/// group, copy `SNAPLEN` bytes of each packet, deliver every packet at once.
pub fn bind_messages(group: u16) -> [Vec<u8>; 3] {
    let mut mode = (SNAPLEN as u32).to_be_bytes().to_vec();
    mode.extend_from_slice(&[NFULNL_COPY_PACKET, 0]);
    [
        config_message(group, &attribute(NFULA_CFG_CMD, &[NFULNL_CFG_CMD_BIND]), 1),
        config_message(group, &attribute(NFULA_CFG_MODE, &mode), 2),
        config_message(group, &attribute(NFULA_CFG_QTHRESH, &1u32.to_be_bytes()), 3),
    ]
}

/// The logged packets (NFULA_PAYLOAD) in one netlink read, in order.
pub fn parse_messages(data: &[u8], mut each: impl FnMut(&[u8])) {
    let mut offset = 0;
    while offset + NLMSG_HEADER <= data.len() {
        let length = u32_at(data, offset) as usize;
        if length < NLMSG_HEADER || offset + length > data.len() {
            break;
        }
        if u16_at(data, offset + 4) == (NFNL_SUBSYS_ULOG << 8) | NFULNL_MSG_PACKET {
            let (mut cursor, end) = (offset + NLMSG_HEADER + NFGEN_HEADER, offset + length);
            while cursor + NLATTR_HEADER <= end {
                let size = usize::from(u16_at(data, cursor));
                if size < NLATTR_HEADER {
                    break;
                }
                if u16_at(data, cursor + 2) & 0x7FFF == NFULA_PAYLOAD {
                    // Python's slice: clamped to the read, not to the message.
                    let stop = (cursor + size).min(data.len());
                    each(&data[(cursor + NLATTR_HEADER).min(stop)..stop]);
                }
                cursor += align(size);
            }
        }
        offset += align(length);
    }
}

/// The netlink errno of an NLMSG_ERROR acknowledging `seq` in `data`
/// (0 is success), if there is one.
fn acknowledgement(data: &[u8], seq: u32) -> Option<i32> {
    let mut offset = 0;
    while offset + NLMSG_HEADER <= data.len() {
        let length = u32_at(data, offset) as usize;
        if length < NLMSG_HEADER || offset + length > data.len() {
            break;
        }
        if u16_at(data, offset + 4) == NLMSG_ERROR && u32_at(data, offset + 8) == seq && length >= NLMSG_HEADER + 4 {
            return Some(i32::from_ne_bytes(data[offset + NLMSG_HEADER..offset + NLMSG_HEADER + 4].try_into().expect("four bytes")));
        }
        offset += align(length);
    }
    None
}

fn check(result: libc::c_int) -> io::Result<libc::c_int> {
    if result < 0 { Err(io::Error::last_os_error()) } else { Ok(result) }
}

/// A bound NFLOG group. Closing it unbinds the group (the kernel drops a
/// closed socket's instances).
#[derive(Debug)]
pub struct NflogSocket {
    fd: OwnedFd,
}

impl NflogSocket {
    /// Open, bind and configure `group`. A refusal is the kernel's errno:
    /// EPERM when another socket holds the group or without CAP_NET_ADMIN.
    pub fn bind(group: u16) -> io::Result<NflogSocket> {
        // SAFETY: plain socket creation; the descriptor is owned below.
        let raw = check(unsafe { libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_NETFILTER) })?;
        // SAFETY: a fresh descriptor nobody else owns.
        let socket = NflogSocket { fd: unsafe { OwnedFd::from_raw_fd(raw) } };
        socket.set_option(libc::SO_RCVBUF, &RECEIVE_BUFFER)?;
        // SAFETY: an all-zero sockaddr_nl is valid; the kernel assigns the port id.
        let mut address: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        address.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        let size = std::mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t;
        // SAFETY: `address` is a valid sockaddr_nl of `size` bytes.
        check(unsafe { libc::bind(socket.fd.as_raw_fd(), (&raw const address).cast(), size) })?;
        socket.set_timeout(Duration::from_millis(100))?;
        for (seq, message) in (1u32..).zip(bind_messages(group)) {
            socket.send(&message)?;
            socket.acknowledged(seq)?;
        }
        socket.set_timeout(READ_TIMEOUT)?;
        Ok(socket)
    }

    fn set_option<T>(&self, name: libc::c_int, value: &T) -> io::Result<()> {
        let size = std::mem::size_of::<T>() as libc::socklen_t;
        // SAFETY: `value` points to `size` readable bytes.
        check(unsafe { libc::setsockopt(self.fd.as_raw_fd(), libc::SOL_SOCKET, name, (value as *const T).cast(), size) }).map(drop)
    }

    fn set_timeout(&self, timeout: Duration) -> io::Result<()> {
        let value = libc::timeval { tv_sec: timeout.as_secs() as libc::time_t, tv_usec: timeout.subsec_micros() as libc::suseconds_t };
        self.set_option(libc::SO_RCVTIMEO, &value)
    }

    fn send(&self, message: &[u8]) -> io::Result<()> {
        // SAFETY: `message` is valid for its length.
        let sent = unsafe { libc::send(self.fd.as_raw_fd(), message.as_ptr().cast(), message.len(), 0) };
        if sent < 0 {
            return Err(io::Error::last_os_error());
        }
        if sent as usize != message.len() {
            return Err(io::Error::other("short netlink send"));
        }
        Ok(())
    }

    /// Wait for the kernel's answer to request `seq`; packets read meanwhile
    /// (a leftover table logging already) are dropped.
    fn acknowledged(&self, seq: u32) -> io::Result<()> {
        let deadline = Instant::now() + CONFIG_TIMEOUT;
        let mut buffer = vec![0u8; 64 * 1024];
        while Instant::now() < deadline {
            let read = match self.recv(&mut buffer) {
                Ok(read) => read,
                Err(error) if matches!(error.raw_os_error(), Some(libc::EAGAIN | libc::EINTR | libc::ENOBUFS)) => continue,
                Err(error) => return Err(error),
            };
            match acknowledgement(&buffer[..read], seq) {
                Some(0) => return Ok(()),
                Some(code) => return Err(io::Error::from_raw_os_error(code.checked_neg().unwrap_or(libc::EINVAL))),
                None => {}
            }
        }
        Err(io::Error::new(io::ErrorKind::TimedOut, "NFLOG configuration was not acknowledged"))
    }

    /// One read: its length, or an error (EAGAIN after `READ_TIMEOUT`;
    /// ENOBUFS when the kernel dropped packets for a full buffer).
    pub fn recv(&self, buffer: &mut [u8]) -> io::Result<usize> {
        // SAFETY: `buffer` is writable for its length.
        let read = unsafe { libc::recv(self.fd.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len(), 0) };
        if read < 0 { Err(io::Error::last_os_error()) } else { Ok(read as usize) }
    }
}
