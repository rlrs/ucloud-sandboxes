# Rejected direct upper-data I/O experiment

Candidate 87fcc74b45ac5cc24a2a5e23172afa6ecc3c2c6cf01b1e399e50e82c2a63ee4c
added O_DIRECT to initial and restacked upper-data file opens, leaving indexes
buffered. It was never installed as the production backend.

245 Rust unit tests passed plus the real io_uring discard/rewrite/reopen test
on UCloud. Actual old→new upgrade, new→old rollback, and bounded-cache restore
qualification passed on idle worker 12399536. Work files lived on ext4 vda1,
not tmpfs. Both versions used private daemons/devices and identical tests.

Two 4-second rounds per workload show an unacceptable regression: random
mixed IOPS fell from 169,732 to 9,341 and sequential bandwidth from 1,438,854,384
to 338,813,510 bytes/s. Metadata time was 0.365 versus 0.390 seconds. Native
loop throughput also varied; the direct candidate still failed the relative
throughput gates by a wide margin. Migration correctness does not establish
acceptable performance. The patch is retained only as experiment evidence.
