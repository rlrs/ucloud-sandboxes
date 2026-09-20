# Resident execution under memory PSI

Node-wide memory PSI includes file-cache reclaim and stalls from individual sandbox cgroups. Treating it as an unconditional admission veto for every command prevents already-resident workloads from making progress, even when physical memory headroom is sufficient.

Resident execution now ignores the PSI placement threshold, as it already ignores CPU placement thresholds. Physical memory headroom, missing-metrics rejection, generation ownership, drain fencing, and full-lifetime execution leases remain enforced. New sandbox creation and restoration retain the existing pressure-based placement checks.

Restore admission now reports its safe-retry guarantee at the point where admission fails, before resume begins. This includes implicit restoration from other API operations and does not depend on a subsequent inventory read succeeding. Capacity errors after entering the resume body are not reclassified by the admission wrapper.

Tests cover resident exec and file operations under PSI, physical-memory exhaustion despite free swap, missing metrics, unchanged create/restore pressure rejection, lease cleanup, and distinguishing admission failures from errors after resume begins.

Validation: 943 server tests (6 skips), 118 SDK tests, Go tests, lint and installed-wheel checks passed. The release also passed 350 Linux tests (1 skip) and boot validation for both node bundles. [CI run 35503069097](https://github.com/rlrs/ucloud-sandboxes/actions/runs/35503069097) passed.
