"""pidfd fencing for interpreters built without ``os.pidfd_open``.

python-build-standalone 3.10 (what ``uv`` installs) omits ``os.pidfd_open``
and ``signal.pidfd_send_signal``, so the production ``LinuxPidfdFencer``
refuses every fence there. These classes keep its checks and its exact-PID
semantics and only reach the same kernel syscalls through libc. The harness
uses them only when the interpreter lacks the wrappers.
"""

from __future__ import annotations

import ctypes
import os
from pathlib import Path
import select
import signal

from ucloud_sandboxes.direct_warden import (
    DirectWardenError,
    LinuxPidfdFencer,
    LinuxPidfdHandle,
)
from ucloud_sandboxes.hibernation import hibernation_process_identity_matches

# The unified syscall table gives these numbers on every Linux architecture.
_SYS_PIDFD_SEND_SIGNAL = 424
_SYS_PIDFD_OPEN = 434
_libc = ctypes.CDLL(None, use_errno=True)


def native_pidfd_available() -> bool:
    return hasattr(os, "pidfd_open") and hasattr(signal, "pidfd_send_signal")


def _pidfd_open(pid: int) -> int:
    descriptor = _libc.syscall(_SYS_PIDFD_OPEN, ctypes.c_int(pid), ctypes.c_uint(0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def _pidfd_send_signal(descriptor: int, sig: int) -> None:
    result = _libc.syscall(
        _SYS_PIDFD_SEND_SIGNAL, ctypes.c_int(descriptor), ctypes.c_int(sig),
        ctypes.c_void_p(None), ctypes.c_uint(0),
    )
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


class SyscallPidfdHandle(LinuxPidfdHandle):
    def terminate(self, *, timeout: float) -> None:
        # LinuxPidfdHandle.terminate, with the libc signal call.
        if self._closed:
            raise DirectWardenError("process fence is already closed")
        if self.alive():
            _pidfd_send_signal(self.pidfd, signal.SIGKILL)
        poller = select.poll()
        poller.register(self.pidfd, select.POLLIN)
        if not poller.poll(max(1, int(timeout * 1000))):
            raise DirectWardenError(f"timed out waiting for sentry PID {self.pid} to exit")


class SyscallPidfdFencer(LinuxPidfdFencer):
    def __init__(self, *, proc_root: Path = Path("/proc")) -> None:
        super().__init__(proc_root=proc_root)

    def open(self, pid: int, start_time_ticks: int) -> SyscallPidfdHandle:
        # LinuxPidfdFencer.open, with the libc pidfd_open call.
        if type(pid) is not int or pid <= 1:
            raise DirectWardenError("refusing to fence a system process PID")
        if not hibernation_process_identity_matches(pid, start_time_ticks, proc_root=self.proc_root):
            raise DirectWardenError("sentry identity changed before fencing")
        try:
            descriptor = _pidfd_open(pid)
        except OSError as exc:
            raise DirectWardenError("could not open sentry pidfd") from exc
        if not hibernation_process_identity_matches(pid, start_time_ticks, proc_root=self.proc_root):
            os.close(descriptor)
            raise DirectWardenError("sentry identity changed while fencing")
        return SyscallPidfdHandle(pid, start_time_ticks, descriptor, proc_root=self.proc_root)
