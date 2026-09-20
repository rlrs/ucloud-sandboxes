# Bounded streaming file uploads (0.5.59)

The September 20 07:35–07:45 UTC run used 50–262 MB file uploads. The gateway's shared 256 MiB whole-body reservation remained held until the worker acknowledged the write. Large files therefore serialized and blocked tiny writes behind them. In the retained 10%-sampled telemetry, 12 uploads spent 30 seconds at gateway admission without worker spans. These are sampled requests, not a complete failure count.

PUT file requests now stream through a dedicated node connection pool in at most 64 KiB reads. They no longer reserve their entire body in gateway RAM or occupy worker cold-start slots. Existing HTTP request admission and per-file size validation remain. TCP and local disk writes provide backpressure during transfers.

Workers retain at most 64 KiB for small writes; larger requests go to unlinked temporary files on the local node state filesystem. Unwritten bytes are reserved against actual available disk space, with 1 GiB left for node metadata and cleanup. Completed staging files are passed directly as subprocess stdin. Every upload must be completely received before a sandbox command starts. Generation fencing prevents delayed uploads from writing into replacement sandboxes. Temporary files are reclaimed on completion, disconnect, exceptions, and process exit.

Disk admission failures consume the remainder of the bounded request stream before returning the existing safe pre-dispatch retry response. Exceptions after command dispatch are never converted into admission retries. The 0.5.58 bounded wait for active execution capacity remains in effect. New trace attributes distinguish upload size, streaming, received bytes, receive duration, and sandbox-write duration.

Regression tests exercise real gateway and node HTTP servers: a small write passes a stalled large upload even with all old memory reservations and cold-start slots occupied; truncated uploads and changed generations never dispatch; disk-pressure rejection retains its retry code after a large body; disk reservations and file cleanup remain correct. The process runner is tested with actual file-backed stdin. No SDK API or version change is required.

Production qualification results will be recorded after deployment. This change addresses upload head-of-line blocking; it does not establish a new concurrency capacity guarantee or prevent provider node loss.
