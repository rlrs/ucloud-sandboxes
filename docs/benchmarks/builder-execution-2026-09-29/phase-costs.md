# Builder execution: retained phase costs

All durations below are milliseconds, shown as median / p95 / maximum. Parent and child timers overlap.

| Nested build phase | Baseline reporting | Baseline | Candidate reporting | Candidate |
| --- | ---: | ---: | ---: | ---: |
| cache_prepare_ms | 48/48 | 3.000 / 7.000 / 8.000 | 48/48 | 2.000 / 6.000 / 6.000 |
| cache_mount_ms | 48/48 | 290.500 / 677.100 / 1095.000 | 48/48 | 271.000 / 462.000 / 708.000 |
| docker_build_and_push_ms | 48/48 | 34214.500 / 50801.400 / 51068.000 | 48/48 | 27074.000 / 44012.000 / 44902.000 |
| immutable_environment_ms | 48/48 | 3020.500 / 9788.900 / 9980.000 | 48/48 | 1805.000 / 3738.000 / 3980.000 |

## all

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_subprocess_ms | unreported / unreported / unreported | 766.294 / 2213.265 / 2319.756 | 46/48 |
| selective_materialization_ms | 1061.155 / 4928.875 / 5024.658 | 262.519 / 1143.519 / 1159.622 | 46/48 |
| squash_ms | 548.001 / 2961.345 / 3135.432 | 172.535 / 707.743 / 772.581 | 46/48 |
| mkfs_ms | 129.338 / 498.663 / 528.007 | 98.796 / 407.370 / 460.770 | 46/48 |
| sign_ms | 10.175 / 34.590 / 45.435 | 7.749 / 21.407 / 22.512 | 46/48 |
| publish_component_ms | 238.075 / 574.048 / 647.691 | 197.867 / 481.199 / 544.448 | 46/48 |
| component_lookup_ms | 1.483 / 2.978 / 3.801 | 1.054 / 1.927 / 2.479 | 46/48 |
| layer_lock_wait_ms | 0.009 / 0.120 / 0.394 | 0.007 / 0.010 / 0.016 | 46/48 |
| preflight_ms | 2701.110 / 9481.278 / 9733.109 | 1558.348 / 3528.610 / 3737.247 | 48/48 |
| extraction_plus_squash_ms | 1664.747 / 7895.393 / 8160.090 | 442.171 / 1829.622 / 1927.718 | 46/48 |

## python-agent

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_subprocess_ms | unreported / unreported / unreported | 766.758 / 791.478 / 825.439 | 15/16 |
| selective_materialization_ms | 1142.508 / 1328.657 / 1369.220 | 263.332 / 290.636 / 298.998 | 15/16 |
| squash_ms | 564.375 / 844.176 / 851.939 | 173.091 / 188.523 / 189.747 | 15/16 |
| mkfs_ms | 129.338 / 147.233 / 147.688 | 98.909 / 115.635 / 118.335 | 15/16 |
| sign_ms | 8.614 / 11.018 / 12.557 | 7.435 / 11.514 / 13.455 | 15/16 |
| publish_component_ms | 234.780 / 647.094 / 647.691 | 204.040 / 327.976 / 336.748 | 15/16 |
| component_lookup_ms | 1.596 / 3.175 / 3.801 | 1.057 / 2.111 / 2.469 | 15/16 |
| layer_lock_wait_ms | 0.052 / 0.370 / 0.394 | 0.008 / 0.010 / 0.010 | 15/16 |
| preflight_ms | 2730.633 / 3507.558 / 3565.404 | 1577.761 / 1777.910 / 1788.869 | 16/16 |
| extraction_plus_squash_ms | 1715.954 / 2167.725 / 2210.808 | 442.389 / 478.159 / 481.332 | 15/16 |

## typescript-multistage

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_subprocess_ms | unreported / unreported / unreported | 414.904 / 470.873 / 472.842 | 16/16 |
| selective_materialization_ms | 78.725 / 150.639 / 185.222 | 71.093 / 81.012 / 82.465 | 16/16 |
| squash_ms | 7.107 / 21.950 / 26.014 | 3.516 / 4.831 / 4.994 | 16/16 |
| mkfs_ms | 43.898 / 63.173 / 65.498 | 28.726 / 37.316 / 37.569 | 16/16 |
| sign_ms | 7.736 / 13.543 / 13.760 | 5.228 / 7.973 / 8.484 | 16/16 |
| publish_component_ms | 205.309 / 283.075 / 296.121 | 128.012 / 172.903 / 176.768 | 16/16 |
| component_lookup_ms | 1.375 / 3.167 / 3.711 | 1.019 / 1.671 / 2.479 | 16/16 |
| layer_lock_wait_ms | 0.007 / 0.026 / 0.059 | 0.007 / 0.010 / 0.016 | 16/16 |
| preflight_ms | 457.903 / 606.312 / 614.299 | 626.895 / 724.679 / 727.690 | 16/16 |
| extraction_plus_squash_ms | 84.822 / 169.500 / 189.697 | 74.810 / 85.173 / 87.242 | 16/16 |

## typescript-tools

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_subprocess_ms | unreported / unreported / unreported | 2078.326 / 2284.644 / 2319.756 | 15/16 |
| selective_materialization_ms | 1898.677 / 5013.649 / 5024.658 | 1077.694 / 1156.883 / 1159.622 | 15/16 |
| squash_ms | 1252.940 / 3133.852 / 3135.432 | 658.485 / 772.181 / 772.581 | 15/16 |
| mkfs_ms | 426.873 / 526.352 / 528.007 | 359.784 / 439.530 / 460.770 | 15/16 |
| sign_ms | 20.898 / 41.244 / 45.435 | 20.463 / 22.014 / 22.512 | 15/16 |
| publish_component_ms | 307.984 / 429.755 / 434.488 | 268.510 / 534.099 / 544.448 | 15/16 |
| component_lookup_ms | 1.432 / 1.916 / 1.986 | 1.072 / 1.819 / 1.839 | 15/16 |
| layer_lock_wait_ms | 0.010 / 0.059 / 0.062 | 0.007 / 0.009 / 0.009 | 15/16 |
| preflight_ms | 4581.311 / 9713.834 / 9733.109 | 3269.767 / 3713.959 / 3737.247 | 16/16 |
| extraction_plus_squash_ms | 3305.833 / 8141.532 / 8160.090 | 1733.698 / 1924.331 / 1927.718 | 15/16 |

## Health-probe interpretation

Configured endpoint: `https://77.42.92.27/healthz`. Within the client window, status counts were `{'200': 19}`.
These are gateway-origin /healthz observations, not external capacity evidence.

## Limits

- Historical runs use identical context hashes but different placement/cache history; deltas are descriptive, not isolated causal speedups.
- Durations are milliseconds with linearly interpolated percentiles. Missing values remain missing; they are not zero.
- Cache preparation/mount are nested inside build-and-push. Preflight contains selective extraction, squash and other work. Do not sum parent and child timings or independently calculated percentiles.
- Selective materialization includes registry download, authentication, decompression, validation and extraction; it is not an extraction-only timer.
- Per-build extraction-plus-squash sums disjoint stages before aggregation; summed wall time across concurrent builds is neither batch elapsed time nor CPU time.
- Sampler health status reflects its configured endpoint. A protected endpoint's HTTP401 response cannot establish successful health during the burst.
- selective_subprocess_ms includes fresh child startup, extraction, squash and IPC; selective_materialization_ms and squash_ms are nested child stages.
- Residual overhead is calculated per build as subprocess wall minus both child stage wall times. It includes startup/imports/IPC and any uninstrumented work, not only process startup.
- Both runs use fresh builder pools, but registry cache history, placement and chronology differ. Runtime effects are not isolated by this historical comparison.

## Child invocation coverage

46/48 records report a positive child timer; 2 use the complete-component-cache-hit metric shape, and 0 are unresolved. Cache-hit bypasses have no child startup or child timer; do not insert zero timings into the child-stage distributions. Exact bypass identities are in JSON.

## Child-invocation residual

Nonnegative residual coverage: 46/48; negative residuals: 0. Median / p95 / maximum: 336.392 / 393.116 / 396.762 ms. This subtracts the two nested child stages per record before aggregation; it is not added to subprocess wall time.
