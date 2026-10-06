//! The `runsc exec` argv of an HTTP exec, as `DirectRunscWarden.exec_lease`
//! builds it (direct_warden.py):
//!
//! `<runsc> --root=<runtime_root> exec [--cwd=<dir>] [--env=K=V ...] <container_id> <command...>`
//!
//! with the environment sorted by name. HTTP execs pass no user
//! (`DirectExecRuntime.exec_command` has `user=None`), and Python reads nothing
//! from the bundle's config.json here: the identity, the default working
//! directory and the base environment are runsc's defaults for the container
//! (the OCI process), with `--env` overlaid. So neither does this.

use super::request::ExecRequest;

/// `runsc` and `runtime_root` as Python's `str(Path)` renders them
/// (`DirectWardenConfig.runsc`, `.runtime_root`). Call after
/// [`ExecRequest::check_direct`].
pub fn runsc_exec_argv(runsc: &str, runtime_root: &str, container_id: &str, request: &ExecRequest) -> Vec<String> {
    let mut argv = Vec::with_capacity(4 + request.env.len() + request.command.len());
    argv.push(runsc.to_owned());
    argv.push(format!("--root={runtime_root}"));
    argv.push("exec".to_owned());
    if let Some(dir) = &request.working_dir {
        argv.push(format!("--cwd={dir}"));
    }
    let mut env: Vec<&(String, String)> = request.env.iter().collect();
    // Python's sorted(env.items()): names are unique, and str order is code
    // point order, which UTF-8 byte order preserves.
    env.sort();
    argv.extend(env.into_iter().map(|(name, value)| format!("--env={name}={value}")));
    argv.push(container_id.to_owned());
    argv.extend(request.command.iter().cloned());
    argv
}
