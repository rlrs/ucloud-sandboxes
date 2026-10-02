"""Gateway use cases split out of ControlPlaneHandler (plan C6.1).

No module here imports control_plane: the handler depends on these, never the
reverse. Process-wide state lives on use-case instances built once by
build_server; request-scoped I/O stays on the handler behind ``Exchange``.
"""
