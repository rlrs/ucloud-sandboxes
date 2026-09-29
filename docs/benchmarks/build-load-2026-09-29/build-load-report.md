# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| cold | 12 / 12 | 112.256 / 162.839 | 2.841 / 4.327 | 0.004 / 0.014 | 53.064 / 105.746 | 49.209 / 61.732 | 108.385 / 159.934 | 12 / 12 | 4 |
| cold / python-agent | 4 / 4 | 157.849 / 167.050 | 2.624 / 2.637 | 0.003 / 0.006 | 105.475 / 105.943 | 49.326 / 64.629 | 155.168 / 163.949 | 4 / 4 | 2 |
| cold / typescript-multistage | 4 / 4 | 60.871 / 62.572 | 3.128 / 4.183 | 0.005 / 0.009 | 49.189 / 50.549 | 7.575 / 8.209 | 57.184 / 58.192 | 4 / 4 | 3 |
| cold / typescript-tools | 4 / 4 | 112.256 / 113.020 | 3.613 / 4.334 | 0.004 / 0.018 | 53.064 / 55.211 | 53.362 / 56.584 | 108.385 / 108.645 | 4 / 4 | 3 |
| warm | 24 / 24 | 14.466 / 61.650 | 5.489 / 7.607 | 0.006 / 2.941 | 3.983 / 29.622 | 0.960 / 34.165 | 4.865 / 56.924 | 23 / 14 | 4 |
| warm / python-agent | 8 / 8 | 11.994 / 61.649 | 4.164 / 4.833 | 0.003 / 0.007 | 4.310 / 21.411 | 2.337 / 35.800 | 6.959 / 57.273 | 8 / 8 | 3 |
| warm / typescript-multistage | 8 / 8 | 12.222 / 43.053 | 7.234 / 7.562 | 0.013 / 2.641 | 1.784 / 27.830 | 0.445 / 7.166 | 2.195 / 34.820 | 7 / 4 | 4 |
| warm / typescript-tools | 8 / 8 | 29.100 / 57.817 | 6.149 / 7.616 | 0.039 / 3.939 | 15.815 / 29.839 | 4.088 / 20.749 | 20.735 / 50.437 | 8 / 6 | 4 |
| app | 24 / 24 | 47.613 / 97.943 | 6.604 / 8.218 | 0.010 / 47.080 | 29.212 / 38.328 | 4.899 / 30.179 | 36.562 / 52.551 | 24 / 16 | 4 |
| app / python-agent | 8 / 8 | 20.352 / 56.632 | 5.394 / 6.749 | 0.004 / 0.017 | 7.976 / 19.988 | 4.899 / 32.476 | 12.950 / 52.541 | 8 / 8 | 3 |
| app / typescript-multistage | 8 / 8 | 47.613 / 86.716 | 7.142 / 8.151 | 9.460 / 44.975 | 32.529 / 37.692 | 0.901 / 1.136 | 33.657 / 38.559 | 8 / 5 | 4 |
| app / typescript-tools | 8 / 8 | 56.811 / 99.606 | 7.078 / 8.167 | 0.142 / 44.963 | 32.877 / 38.434 | 12.098 / 16.259 | 46.212 / 52.172 | 8 / 6 | 4 |
| dependency | 6 / 6 | 62.443 / 128.666 | 4.804 / 4.940 | 0.006 / 0.012 | 37.026 / 80.021 | 19.725 / 46.415 | 56.814 / 126.528 | 6 / 6 | 2 |
| dependency / python-agent | 2 / 2 | 128.666 / 128.666 | 1.461 / 1.475 | 0.004 / 0.004 | 80.017 / 80.024 | 46.412 / 46.417 | 126.523 / 126.532 | 2 / 2 | 1 |
| dependency / typescript-multistage | 2 / 2 | 40.755 / 41.238 | 4.899 / 4.924 | 0.006 / 0.006 | 33.705 / 33.736 | 1.537 / 1.566 | 35.308 / 35.367 | 2 / 2 | 1 |
| dependency / typescript-tools | 2 / 2 | 62.443 / 62.446 | 4.841 / 4.934 | 0.010 / 0.014 | 37.026 / 37.125 | 19.725 / 19.736 | 56.814 / 56.931 | 2 / 2 | 1 |
| overload | 48 / 48 | 77.345 / 162.455 | 11.327 / 55.693 | 9.323 / 108.377 | 33.504 / 85.614 | 5.098 / 24.776 | 38.311 / 110.958 | 32 / 16 | 4 |
| overload / python-agent | 16 / 16 | 21.805 / 120.202 | 8.744 / 9.465 | 0.007 / 2.010 | 6.582 / 86.160 | 5.098 / 25.319 | 11.822 / 111.158 | 16 / 15 | 4 |
| overload / typescript-multistage | 16 / 16 | 90.216 / 162.646 | 12.240 / 58.102 | 33.816 / 108.919 | 35.418 / 41.524 | 1.034 / 2.247 | 36.574 / 42.367 | 14 / 8 | 4 |
| overload / typescript-tools | 16 / 16 | 101.883 / 140.454 | 21.030 / 35.747 | 35.413 / 59.973 | 33.573 / 38.861 | 8.918 / 15.444 | 42.457 / 53.050 | 15 / 10 | 4 |
| replacement | 24 / 24 | 96.840 / 146.499 | 6.213 / 7.537 | 0.005 / 99.967 | 34.737 / 53.772 | 16.069 / 69.126 | 62.820 / 106.112 | 24 / 16 | 4 |
| replacement / python-agent | 8 / 8 | 96.840 / 109.832 | 4.143 / 4.404 | 0.003 / 0.004 | 30.317 / 35.620 | 62.154 / 69.201 | 92.546 / 104.834 | 8 / 8 | 3 |
| replacement / typescript-multistage | 8 / 8 | 70.129 / 105.262 | 6.757 / 7.545 | 0.965 / 66.710 | 49.555 / 53.532 | 3.789 / 9.311 | 53.392 / 62.923 | 8 / 5 | 3 |
| replacement / typescript-tools | 8 / 8 | 119.862 / 149.097 | 6.332 / 7.520 | 30.710 / 102.595 | 39.775 / 53.793 | 22.626 / 52.546 | 63.846 / 106.375 | 8 / 6 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| cold / builder-167927353 | 99.4% | 2.050 / 3.510 / 4.855 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| cold / builder-167927354 | 99.4% | 1.691 / 3.455 / 3.955 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| cold / builder-167927367 | 99.4% | 1.715 / 5.350 / 6.305 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| cold / builder-167927368 | 98.2% | 0.859 / 2.375 / 3.435 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| cold / gateway | 98.2% | 0.228 / 0.845 / 1.335 | 0.020 / 0.035 / 0.110 | 0.106 / 0.505 / 1.100 | 0.019 / 0.010 / 1.220 | 0 / 28 | 11.527 |
| warm / builder-167927353 | 95.2% | 1.611 / 3.015 / 3.140 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| warm / builder-167927354 | 95.2% | 1.850 / 3.635 / 3.635 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| warm / builder-167927367 | 95.2% | 2.512 / 4.575 / 4.650 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| warm / builder-167927368 | 98.4% | 1.246 / 2.890 / 3.310 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| warm / gateway | 98.4% | 0.457 / 1.760 / 2.090 | 0.027 / 0.080 / 0.165 | 0.177 / 0.685 / 1.320 | 0.139 / 1.640 / 1.790 | 0 / 10 | 11.811 |
| app / builder-167927353 | 98.0% | 3.908 / 5.900 / 6.011 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| app / builder-167927354 | 98.0% | 1.703 / 4.625 / 4.880 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| app / builder-167927367 | 98.0% | 2.110 / 5.740 / 6.010 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| app / builder-167927368 | 98.0% | 2.634 / 6.660 / 7.065 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| app / gateway | 98.0% | 0.262 / 1.170 / 1.655 | 0.028 / 0.075 / 0.125 | 0.083 / 0.265 / 0.635 | 0.089 / 0.690 / 1.775 | 0 / 16 | 9.978 |
| dependency / builder-167927353 | 99.2% | 1.218 / 1.700 / 2.405 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| dependency / builder-167927354 | 99.2% | 0.947 / 2.450 / 3.070 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| dependency / builder-167927367 | 99.2% | 0.128 / 0.345 / 0.395 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| dependency / builder-167927368 | 97.7% | 0.090 / 0.230 / 0.355 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| dependency / gateway | 97.7% | 0.114 / 0.440 / 1.175 | 0.012 / 0.025 / 0.045 | 0.045 / 0.155 / 1.070 | 0.002 / 0.005 / 0.010 | 0 / 21 | 8.897 |
| overload / builder-167927353 | 98.8% | 3.396 / 6.485 / 6.910 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| overload / builder-167927354 | 98.8% | 3.253 / 6.473 / 6.750 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| overload / builder-167927367 | 98.8% | 3.097 / 6.625 / 6.955 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| overload / builder-167927368 | 98.8% | 2.247 / 6.370 / 7.035 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| overload / gateway | 98.8% | 0.341 / 1.605 / 2.110 | 0.053 / 0.145 / 0.275 | 0.107 / 0.405 / 1.390 | 0.111 / 1.510 / 1.965 | 0 / 29 | 11.037 |
| replacement / builder-167933203 | 98.0% | 2.726 / 5.180 / 5.555 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| replacement / builder-167933204 | 98.0% | 2.909 / 5.314 / 5.715 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| replacement / builder-167933217 | 98.0% | 2.595 / 5.220 / 5.710 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| replacement / builder-167933218 | 98.0% | 1.945 / 5.510 / 6.185 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| replacement / gateway | 98.0% | 0.254 / 1.130 / 1.585 | 0.032 / 0.060 / 0.185 | 0.114 / 0.560 / 1.350 | 0.049 / 0.015 / 1.655 | 0 / 25 | 9.610 |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| cold / builder-167927353 / loop0 | 0.000 / 0.121 | 267.464 / 396.967 | 9.831 | 65.100 |
| cold / builder-167927353 / sda | 0.008 / 1.121 | 261.925 / 406.260 | 5.738 | 22.451 |
| cold / builder-167927353 / sda1 | 0.008 / 1.121 | 261.925 / 406.260 | 5.738 | 25.401 |
| cold / builder-167927354 / loop0 | 0.000 / 0.623 | 252.010 / 460.485 | 6.375 | 54.101 |
| cold / builder-167927354 / sda | 0.010 / 1.121 | 253.375 / 464.228 | 6.032 | 24.350 |
| cold / builder-167927354 / sda1 | 0.010 / 1.121 | 253.375 / 464.228 | 6.032 | 27.400 |
| cold / builder-167927367 / loop0 | 0.000 / 1.484 | 165.320 / 465.908 | 8.457 | 48.100 |
| cold / builder-167927367 / sda | 0.008 / 1.121 | 166.464 / 468.353 | 5.375 | 21.300 |
| cold / builder-167927367 / sda1 | 0.008 / 1.121 | 166.464 / 468.353 | 5.375 | 25.100 |
| cold / builder-167927368 / loop0 | 0.000 / 0.744 | 135.847 / 406.473 | 7.356 | 38.800 |
| cold / builder-167927368 / sda | 0.006 / 1.123 | 161.078 / 416.128 | 4.770 | 6.150 |
| cold / builder-167927368 / sda1 | 0.006 / 1.123 | 161.078 / 416.128 | 4.770 | 8.350 |
| cold / gateway / sda | 0.000 / 0.002 | 4.727 / 12.977 | 1.000 | 0.400 |
| cold / gateway / sda1 | 0.000 / 0.002 | 4.727 / 12.977 | 1.000 | 0.500 |
| cold / gateway / sdb | 0.002 / 0.004 | 250.651 / 301.019 | 180.504 | flagged |
| warm / builder-167927353 / loop0 | 0.121 / 0.619 | 66.166 / 141.884 | 4.789 | 72.749 |
| warm / builder-167927353 / sda | 0.002 / 0.006 | 69.634 / 145.942 | 1.858 | 56.941 |
| warm / builder-167927353 / sda1 | 0.002 / 0.006 | 69.634 / 145.942 | 1.858 | 57.890 |
| warm / builder-167927354 / loop0 | 0.000 / 0.121 | 61.751 / 121.656 | 2.783 | 74.849 |
| warm / builder-167927354 / sda | 0.000 / 0.002 | 65.305 / 123.036 | 2.736 | 52.049 |
| warm / builder-167927354 / sda1 | 0.000 / 0.002 | 65.305 / 123.036 | 2.736 | 53.099 |
| warm / builder-167927367 / loop0 | 0.000 / 0.121 | 277.563 / 416.589 | 5.744 | 80.950 |
| warm / builder-167927367 / sda | 0.000 / 0.002 | 314.124 / 423.376 | 3.430 | 51.151 |
| warm / builder-167927367 / sda1 | 0.000 / 0.002 | 314.124 / 423.376 | 3.430 | 51.351 |
| warm / builder-167927368 / loop0 | 0.121 / 0.621 | 44.659 / 192.869 | 7.125 | 36.000 |
| warm / builder-167927368 / sda | 0.000 / 0.002 | 50.113 / 195.546 | 0.959 | 16.100 |
| warm / builder-167927368 / sda1 | 0.000 / 0.002 | 50.113 / 195.546 | 1.000 | 16.700 |
| warm / gateway / sda | 0.000 / 0.000 | 20.295 / 22.308 | 1.369 | 0.700 |
| warm / gateway / sda1 | 0.000 / 0.000 | 20.295 / 22.308 | 1.369 | 0.900 |
| warm / gateway / sdb | 0.002 / 0.004 | 67.405 / 83.136 | 20.093 | flagged |
| app / builder-167927353 / loop0 | 0.121 / 0.746 | 288.671 / 384.093 | 4.554 | 72.899 |
| app / builder-167927353 / sda | 0.010 / 0.045 | 293.350 / 356.102 | 3.643 | 41.199 |
| app / builder-167927353 / sda1 | 0.010 / 0.045 | 293.350 / 356.102 | 3.643 | 42.252 |
| app / builder-167927354 / loop0 | 0.000 / 0.742 | 34.037 / 56.622 | 3.278 | 73.500 |
| app / builder-167927354 / sda | 0.000 / 0.006 | 39.642 / 75.650 | 2.111 | 43.450 |
| app / builder-167927354 / sda1 | 0.000 / 0.006 | 39.642 / 75.650 | 2.111 | 45.650 |
| app / builder-167927367 / loop0 | 0.002 / 0.746 | 61.166 / 99.732 | 4.787 | 53.800 |
| app / builder-167927367 / sda | 0.000 / 0.027 | 71.507 / 105.940 | 1.993 | 38.600 |
| app / builder-167927367 / sda1 | 0.000 / 0.027 | 71.507 / 105.940 | 1.993 | 38.800 |
| app / builder-167927368 / loop0 | 0.000 / 1.486 | 67.342 / 153.726 | 3.764 | 58.452 |
| app / builder-167927368 / sda | 0.008 / 0.102 | 94.478 / 161.546 | 1.978 | 13.350 |
| app / builder-167927368 / sda1 | 0.008 / 0.102 | 94.478 / 161.546 | 1.978 | 14.000 |
| app / gateway / sda | 0.000 / 0.006 | 11.338 / 23.422 | 1.000 | 0.700 |
| app / gateway / sda1 | 0.000 / 0.006 | 11.338 / 23.422 | 1.047 | 0.950 |
| app / gateway / sdb | 0.002 / 0.004 | 31.813 / 240.572 | 21.928 | flagged |
| dependency / builder-167927353 / loop0 | 0.000 / 0.000 | 150.546 / 365.822 | 9.958 | 33.251 |
| dependency / builder-167927353 / sda | 0.000 / 0.008 | 151.565 / 411.904 | 5.050 | 16.250 |
| dependency / builder-167927353 / sda1 | 0.000 / 0.008 | 151.565 / 411.904 | 5.050 | 16.700 |
| dependency / builder-167927354 / loop0 | 0.000 / 0.740 | 35.260 / 90.976 | 4.138 | 40.350 |
| dependency / builder-167927354 / sda | 0.000 / 0.000 | 36.137 / 92.854 | 3.145 | 19.150 |
| dependency / builder-167927354 / sda1 | 0.000 / 0.000 | 36.139 / 92.854 | 3.145 | 19.350 |
| dependency / builder-167927367 / sda | 0.000 / 0.000 | 0.098 / 0.637 | 1.062 | 0.100 |
| dependency / builder-167927367 / sda1 | 0.000 / 0.000 | 0.098 / 0.637 | 1.091 | 0.100 |
| dependency / builder-167927368 / sda | 0.000 / 0.000 | 0.100 / 0.523 | 1.000 | 0.050 |
| dependency / builder-167927368 / sda1 | 0.000 / 0.000 | 0.100 / 0.523 | 1.000 | 0.050 |
| dependency / gateway / sda | 0.000 / 0.000 | 1.527 / 11.056 | 0.500 | 0.300 |
| dependency / gateway / sda1 | 0.000 / 0.000 | 1.527 / 11.056 | 0.781 | 0.350 |
| dependency / gateway / sdb | 0.000 / 0.000 | 99.069 / 289.177 | 172.605 | 80.250 |
| overload / builder-167927353 / loop0 | 0.619 / 1.492 | 62.334 / 111.438 | 3.000 | 63.181 |
| overload / builder-167927353 / sda | 1.359 / 3.287 | 81.220 / 115.006 | 1.923 | 40.900 |
| overload / builder-167927353 / sda1 | 1.359 / 3.287 | 81.220 / 115.006 | 1.924 | 42.100 |
| overload / builder-167927354 / loop0 | 0.121 / 1.863 | 89.471 / 109.959 | 3.939 | 64.727 |
| overload / builder-167927354 / sda | 0.000 / 0.061 | 91.726 / 113.790 | 2.106 | 41.401 |
| overload / builder-167927354 / sda1 | 0.000 / 0.061 | 91.726 / 113.790 | 2.106 | 42.201 |
| overload / builder-167927367 / loop0 | 0.121 / 1.859 | 66.397 / 168.000 | 2.536 | flagged |
| overload / builder-167927367 / sda | 0.010 / 0.045 | 73.536 / 171.817 | 2.966 | 45.451 |
| overload / builder-167927367 / sda1 | 0.010 / 0.045 | 73.536 / 171.817 | 2.966 | 45.951 |
| overload / builder-167927368 / loop0 | 0.000 / 1.979 | 142.951 / 416.278 | 7.148 | 43.500 |
| overload / builder-167927368 / sda | 0.000 / 0.012 | 147.482 / 416.132 | 2.131 | 9.800 |
| overload / builder-167927368 / sda1 | 0.000 / 0.012 | 147.482 / 416.132 | 2.130 | 10.800 |
| overload / gateway / sda | 0.000 / 0.000 | 3.752 / 27.820 | 0.822 | 1.100 |
| overload / gateway / sda1 | 0.000 / 0.000 | 3.752 / 27.820 | 0.998 | 1.100 |
| overload / gateway / sdb | 0.004 / 0.006 | 157.581 / 300.720 | 108.993 | 90.150 |
| replacement / builder-167933203 / loop0 | 0.002 / 1.244 | 417.716 / 696.251 | 6.168 | 77.898 |
| replacement / builder-167933203 / sda | 0.037 / 1.121 | 425.129 / 736.799 | 5.196 | 33.702 |
| replacement / builder-167933203 / sda1 | 0.037 / 1.121 | 425.129 / 736.799 | 5.196 | 35.950 |
| replacement / builder-167933204 / loop0 | 0.002 / 0.242 | 328.339 / 647.028 | 8.749 | 70.000 |
| replacement / builder-167933204 / sda | 0.057 / 1.121 | 331.157 / 664.073 | 4.567 | 35.700 |
| replacement / builder-167933204 / sda1 | 0.057 / 1.121 | 331.157 / 664.073 | 4.568 | 38.700 |
| replacement / builder-167933217 / loop0 | 0.121 / 0.625 | 387.564 / 691.893 | 5.435 | 82.400 |
| replacement / builder-167933217 / sda | 0.018 / 1.121 | 390.689 / 618.066 | 5.027 | 35.001 |
| replacement / builder-167933217 / sda1 | 0.018 / 1.121 | 390.689 / 618.066 | 5.027 | 39.551 |
| replacement / builder-167933218 / loop0 | 0.002 / 0.623 | 213.122 / 450.536 | 9.787 | 56.447 |
| replacement / builder-167933218 / sda | 0.066 / 1.121 | 217.266 / 452.205 | 5.413 | 30.300 |
| replacement / builder-167933218 / sda1 | 0.066 / 1.121 | 217.266 / 452.205 | 5.412 | 32.850 |
| replacement / gateway / sda | 0.000 / 0.002 | 3.713 / 5.867 | 0.671 | 0.300 |
| replacement / gateway / sda1 | 0.000 / 0.002 | 3.713 / 5.867 | 0.795 | 0.450 |
| replacement / gateway / sdb | 0.002 / 0.004 | 216.315 / 302.350 | 90.601 | flagged |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

Builders with retained sample lifespans entirely outside a phase and no builds owned in it are omitted from that phase's display and coverage warnings. Their evidence remains in JSON as `outside_phase_window`. Expected owners retain missing-coverage warnings.

## Observed evidence

- **cold:** 12/12 builds succeeded; 4 known builder owners; observed admitted/executing peaks 12/12. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for cold: 29 groups reused, 23 built, 2520211456 newly built bytes. Cached multistage compile-vertex evidence: 2 records; this does not exclude concurrent executed vertices.
- **warm:** 24/24 builds succeeded; 4 known builder owners; observed admitted/executing peaks 23/14. 12 distinct recorded fixture hashes for 24 submissions; repeated graphs can share BuildKit work. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for warm: 94 groups reused, 10 built, 253526016 newly built bytes. Cached multistage compile-vertex evidence: 8 records; this does not exclude concurrent executed vertices.
- **app:** 24/24 builds succeeded; 4 known builder owners; observed admitted/executing peaks 24/16. Observed 3 HTTP 503 responses and 3 repeated submit attempts; use categories to distinguish admission from polling failures. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for app: 80 groups reused, 24 built, 191766528 newly built bytes. Cached multistage compile-vertex evidence: 2 records; this does not exclude concurrent executed vertices.
- **dependency:** 6/6 builds succeeded; 2 known builder owners; observed admitted/executing peaks 6/6. 3 distinct recorded fixture hashes for 6 submissions; repeated graphs can share BuildKit work.
- EROFS counters for dependency: 20 groups reused, 6 built, 809365504 newly built bytes. Cached multistage compile-vertex evidence: 1 records; this does not exclude concurrent executed vertices.
- **overload:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 32/16. Observed 133 HTTP 503 responses and 133 repeated submit attempts; use categories to distinguish admission from polling failures. builder-167927367.jsonl.gz: unreliable disk-utilization counters flagged for disk/loop0/busy_percent.
- EROFS counters for overload: 158 groups reused, 50 built, 996818944 newly built bytes. Cached multistage compile-vertex evidence: 1 records; this does not exclude concurrent executed vertices.
- **replacement:** 24/24 builds succeeded; 4 known builder owners; observed admitted/executing peaks 24/16. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for replacement: 82 groups reused, 22 built, 182722560 newly built bytes. Cached multistage compile-vertex evidence: 1 records; this does not exclude concurrent executed vertices.

## Interpretation limits

- Client latency includes SDK compression/upload and gateway admission; worker queue time begins after context materialization.
- Execution overlap spans execution_started_at to finished_at: it includes build/push and EROFS publication, not only RUN instructions, and excludes subsequent cleanup.
- Overlap is computed from complete, unique build intervals. Missing intervals make the observed peak a lower bound; timestamps across hosts depend on synchronized clocks.
- CACHED evidence proves only the described vertex. It may follow waiting for a concurrently computed stage and does not prove the request avoided compilation. Missing/truncated evidence is unknown, not a miss; a warm local hit does not prove cross-builder sharing.
- Repeated submit HTTP attempts and 503s are transport observations, not duplicate execution. Context GET 404s are normal cache lookup misses.
- Fixture hashes exclude fixture.json and differ from SDK archive hashes. Repeated fixtures can share/coalesce BuildKit graphs even with unique image/build IDs.
- EROFS bytes built exclude reused components and are neither total image size nor registry physical growth.
- Gateway-local drivers consume gateway CPU. Driver process attribution is separate but incomplete for short-lived processes; subtracting process p95 from host p95 is invalid.
- Disk devices and network interfaces are separate accounting views; their rates must not be summed. Health probes originate on the sampled host.
- Disk busy counters exceeding 105% over an interval are flagged as unreliable utilization (possible delayed/batched counter accounting). Raw derived statistics are retained in JSON, without clamping.
- Record UTC timestamps have one-second precision. Telemetry includes only complete intervals within that recorded client window; latency distributions use all records with that measurement, including failures.
