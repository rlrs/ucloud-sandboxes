# RAM backing and host memory observations

Read-only statvfs, mount identity and /proc/meminfo were sampled every five
seconds on all four rc13 workers for 480 seconds. The interval spans the end
of natural-512 and the explicit diagnostic-512 repeat; see per-sample UTC times.
There were no tree walks or per-interval subprocesses. All collectors exited.

The smallest observed tmpfs headroom was 12.26 GiB, while the smallest host
MemAvailable was 7.84 GiB. These resident-512 runs therefore did not approach
backing exhaustion. They do not establish the cause of the earlier rc12
pressure-256 SIGBUS failures, for which equivalent contemporaneous backing
samples are unavailable. Future pressure qualification records both resources.
