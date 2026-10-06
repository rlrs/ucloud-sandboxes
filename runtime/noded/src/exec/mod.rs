//! Exec sessions on the node (phase 2a): the Rust port of
//! `ucloud_sandboxes/sandbox_exec.py` (`ExecSessionManager`) and of the exec
//! routes in `node_agent.py`.
//!
//! The routes this module answers, once the HTTP front has decided the request
//! is the daemon's (see the phase-2 exec spec, §6.1):
//! - R1 `POST /v1/sandboxes/{id}/exec`: [`ExecRequest::parse`],
//!   [`ExecRequest::check_direct`], [`runsc_exec_argv`], [`ExecManager::start`];
//! - R2 `GET /v1/exec/{sid}`: [`ExecManager::get`];
//! - R3 `GET /v1/exec/{sid}/events`: [`ExecManager::events`];
//! - R4 `POST /v1/exec/{sid}/stdin`: [`ExecManager::stdin`];
//! - R5 `POST /v1/exec/{sid}/close-stdin`: [`ExecManager::close_stdin`];
//! - R6 `POST /v1/exec/{sid}/signal`: [`ExecManager::signal`].
//!
//! Session ids are uuid4, so a session id the manager does not own
//! ([`ExecManager::owns`]) belongs to Python (or to nobody) and is forwarded.
//! Responses are [`Reply`] values whose body encodes with [`json::dumps`],
//! byte for byte as Python's `_write_json`.

mod argv;
mod decode;
mod error;
pub mod json;
mod manager;
mod request;
mod time;

pub use argv::runsc_exec_argv;
pub use decode::Utf8Decoder;
pub use error::{ExecError, Reply};
pub use manager::{ActivityHook, ExecLimits, ExecManager, Guard, StartTimings, new_session_id, valid_session_prefix};
pub use request::{EventsQuery, ExecRequest, StdinRequest, parse_qs, parse_signal, py_float, py_int};
pub use time::isoformat;
