//! ucloud-noded --listen ADDR --upstream-unix PATH [--max-connections N]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};

fn usage(message: &str) -> ExitCode {
    eprintln!("ucloud-noded: {message}");
    eprintln!("usage: ucloud-noded --listen ADDR --upstream-unix PATH [--max-connections N]");
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let mut listen: Option<SocketAddr> = None;
    let mut upstream: Option<PathBuf> = None;
    let mut max_connections: Option<usize> = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let Some(value) = args.next() else { return usage(&format!("{flag} needs a value")) };
        match flag.as_str() {
            "--listen" => match value.parse() {
                Ok(address) => listen = Some(address),
                Err(_) => return usage(&format!("invalid --listen {value}")),
            },
            "--upstream-unix" => upstream = Some(PathBuf::from(value)),
            "--max-connections" => match value.parse() {
                Ok(count) if count > 0 => max_connections = Some(count),
                _ => return usage(&format!("invalid --max-connections {value}")),
            },
            _ => return usage(&format!("unknown argument {flag}")),
        }
    }
    let (Some(listen), Some(upstream)) = (listen, upstream) else {
        return usage("--listen and --upstream-unix are required");
    };
    let mut config = ucloud_noded::Config::new(upstream);
    if let Some(count) = max_connections {
        config.max_connections = count;
    }
    let runtime = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("ucloud-noded: cannot start the runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async move {
        let listener = match TcpListener::bind(listen).await {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!("ucloud-noded: cannot listen on {listen}: {error}");
                return ExitCode::FAILURE;
            }
        };
        let (Ok(mut terminate), Ok(mut interrupt)) =
            (signal(SignalKind::terminate()), signal(SignalKind::interrupt()))
        else {
            eprintln!("ucloud-noded: cannot install signal handlers");
            return ExitCode::FAILURE;
        };
        eprintln!("ucloud-noded: serving {listen} -> unix:{}", config.upstream.display());
        let shutdown = async move {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
        };
        ucloud_noded::serve(listener, config, shutdown).await;
        ExitCode::SUCCESS
    })
}
