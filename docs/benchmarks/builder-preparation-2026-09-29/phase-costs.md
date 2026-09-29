# Builder preparation: retained phase costs

All durations below are milliseconds, shown as median / p95 / maximum. Parent and child timers overlap.

| Nested build phase | Baseline reporting | Baseline | Candidate reporting | Candidate |
| --- | ---: | ---: | ---: | ---: |
| cache_prepare_ms | 0/48 | unreported / unreported / unreported | 48/48 | 3.000 / 7.000 / 8.000 |
| cache_mount_ms | 0/48 | unreported / unreported / unreported | 48/48 | 290.500 / 677.100 / 1095.000 |
| docker_build_and_push_ms | 48/48 | 31155.500 / 57641.300 / 63722.000 | 48/48 | 34214.500 / 50801.400 / 51068.000 |
| immutable_environment_ms | 48/48 | 4195.500 / 12288.450 / 13515.000 | 48/48 | 3020.500 / 9788.900 / 9980.000 |

## all

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_materialization_ms | 1334.981 / 4800.361 / 5545.097 | 1061.155 / 4928.875 / 5024.658 | 48/48 |
| squash_ms | 1506.149 / 5718.804 / 6326.620 | 548.001 / 2961.345 / 3135.432 | 48/48 |
| mkfs_ms | 114.945 / 433.967 / 459.276 | 129.338 / 498.663 / 528.007 | 48/48 |
| sign_ms | 9.747 / 25.731 / 45.978 | 10.175 / 34.590 / 45.435 | 48/48 |
| publish_component_ms | 176.147 / 483.779 / 559.021 | 238.075 / 574.048 / 647.691 | 48/48 |
| component_lookup_ms | 1.359 / 2.232 / 3.662 | 1.483 / 2.978 / 3.801 | 48/48 |
| layer_lock_wait_ms | 0.010 / 0.084 / 0.755 | 0.009 / 0.120 / 0.394 | 48/48 |
| preflight_ms | 3846.189 / 12092.381 / 13272.391 | 2701.110 / 9481.278 / 9733.109 | 48/48 |
| extraction_plus_squash_ms | 2840.794 / 10554.974 / 11805.935 | 1664.747 / 7895.393 / 8160.090 | 48/48 |

## python-agent

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_materialization_ms | 1365.861 / 1496.998 / 1499.751 | 1142.508 / 1328.657 / 1369.220 | 16/16 |
| squash_ms | 1648.324 / 1762.232 / 1824.620 | 564.375 / 844.176 / 851.939 | 16/16 |
| mkfs_ms | 114.945 / 142.004 / 168.278 | 129.338 / 147.233 / 147.688 | 16/16 |
| sign_ms | 7.982 / 11.162 / 13.234 | 8.614 / 11.018 / 12.557 | 16/16 |
| publish_component_ms | 170.505 / 314.234 / 350.264 | 234.780 / 647.094 / 647.691 | 16/16 |
| component_lookup_ms | 1.739 / 3.039 / 3.662 | 1.596 / 3.175 / 3.801 | 16/16 |
| layer_lock_wait_ms | 0.017 / 0.331 / 0.755 | 0.052 / 0.370 / 0.394 | 16/16 |
| preflight_ms | 3896.591 / 4108.426 / 4132.168 | 2730.633 / 3507.558 / 3565.404 | 16/16 |
| extraction_plus_squash_ms | 2988.940 / 3171.819 / 3237.517 | 1715.954 / 2167.725 / 2210.808 | 16/16 |

## typescript-multistage

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_materialization_ms | 75.915 / 180.733 / 293.324 | 78.725 / 150.639 / 185.222 | 16/16 |
| squash_ms | 14.450 / 36.189 / 41.852 | 7.107 / 21.950 / 26.014 | 16/16 |
| mkfs_ms | 39.073 / 54.240 / 61.525 | 43.898 / 63.173 / 65.498 | 16/16 |
| sign_ms | 6.162 / 11.262 / 11.848 | 7.736 / 13.543 / 13.760 | 16/16 |
| publish_component_ms | 102.351 / 430.077 / 431.813 | 205.309 / 283.075 / 296.121 | 16/16 |
| component_lookup_ms | 1.302 / 1.791 / 1.968 | 1.375 / 3.167 / 3.711 | 16/16 |
| layer_lock_wait_ms | 0.008 / 0.035 / 0.085 | 0.007 / 0.026 / 0.059 | 16/16 |
| preflight_ms | 321.269 / 715.162 / 732.738 | 457.903 / 606.312 / 614.299 | 16/16 |
| extraction_plus_squash_ms | 90.145 / 218.988 / 320.787 | 84.822 / 169.500 / 189.697 | 16/16 |

## typescript-tools

| Environment subphase | Baseline | Candidate | Candidate reporting |
| --- | ---: | ---: | ---: |
| selective_materialization_ms | 2434.690 / 5495.760 / 5545.097 | 1898.677 / 5013.649 / 5024.658 | 16/16 |
| squash_ms | 1773.542 / 6320.175 / 6326.620 | 1252.940 / 3133.852 / 3135.432 | 16/16 |
| mkfs_ms | 392.593 / 449.377 / 459.276 | 426.873 / 526.352 / 528.007 | 16/16 |
| sign_ms | 19.970 / 31.351 / 45.978 | 20.898 / 41.244 / 45.435 | 16/16 |
| publish_component_ms | 250.000 / 524.463 / 559.021 | 307.984 / 429.755 / 434.488 | 16/16 |
| component_lookup_ms | 1.308 / 1.579 / 1.635 | 1.432 / 1.916 / 1.986 | 16/16 |
| layer_lock_wait_ms | 0.009 / 0.020 / 0.020 | 0.010 / 0.059 / 0.062 | 16/16 |
| preflight_ms | 5541.401 / 13267.139 / 13272.391 | 4581.311 / 9713.834 / 9733.109 | 16/16 |
| extraction_plus_squash_ms | 4208.233 / 11797.641 / 11805.935 | 3305.833 / 8141.532 / 8160.090 | 16/16 |

## Health-probe interpretation

Configured endpoint: `https://77.42.92.27/health`. Within the client window, status counts were `{'401': 23}`.
HTTP401 from /health is an invalid successful-health gate; retain raw failures and use independent correctly configured checks.

## Limits

- Historical runs use identical context hashes but different placement/cache history; deltas are descriptive, not isolated causal speedups.
- Durations are milliseconds with linearly interpolated percentiles. Missing values remain missing; they are not zero.
- Cache preparation/mount are nested inside build-and-push. Preflight contains selective extraction, squash and other work. Do not sum parent and child timings or independently calculated percentiles.
- Selective materialization includes registry download, authentication, decompression, validation and extraction; it is not an extraction-only timer.
- Per-build extraction-plus-squash sums disjoint stages before aggregation; summed wall time across concurrent builds is neither batch elapsed time nor CPU time.
- Sampler health status reflects its configured endpoint. A protected endpoint's HTTP401 response cannot establish successful health during the burst.
