# Build load report

Latency values are seconds. Each cell is p50 / p95; observation counts and maxima are in the JSON report.

| Phase / recipe | Success / cases | Client | Submit | Queue | Build + push | EROFS publication | Execution | Admitted / executing peak | Owners |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| slotq-a-condition | 48 / 48 | 79.915 / 101.281 | 55.758 / 90.508 | 0.003 / 0.006 | 30.690 / 55.362 | 1.805 / 4.182 | 32.067 / 57.769 | 16 / 16 | 4 |
| slotq-a-condition / python-agent | 16 / 16 | 79.915 / 96.658 | 58.965 / 88.486 | 0.004 / 0.006 | 7.310 / 28.292 | 1.861 / 2.185 | 9.309 / 30.196 | 5 / 5 | 4 |
| slotq-a-condition / typescript-multistage | 16 / 16 | 68.660 / 104.206 | 31.446 / 73.141 | 0.004 / 0.006 | 36.406 / 56.517 | 0.937 / 1.146 | 37.410 / 57.547 | 9 / 9 | 4 |
| slotq-a-condition / typescript-tools | 16 / 16 | 87.503 / 103.167 | 49.413 / 89.332 | 0.003 / 0.005 | 33.200 / 54.319 | 3.941 / 4.323 | 36.903 / 58.393 | 8 / 8 | 4 |
| slotq-a-warm | 48 / 48 | 10.729 / 17.103 | 6.473 / 13.996 | 0.005 / 0.017 | 2.421 / 3.185 | 0.754 / 1.059 | 3.185 / 4.752 | 16 / 16 | 4 |
| slotq-a-warm / python-agent | 16 / 16 | 10.176 / 15.430 | 6.510 / 12.322 | 0.005 / 0.008 | 2.251 / 2.956 | 0.944 / 1.048 | 3.181 / 3.946 | 7 / 6 | 4 |
| slotq-a-warm / typescript-multistage | 16 / 16 | 10.580 / 17.092 | 6.387 / 13.968 | 0.005 / 0.018 | 2.434 / 3.637 | 0.539 / 0.658 | 3.019 / 4.347 | 7 / 5 | 4 |
| slotq-a-warm / typescript-tools | 16 / 16 | 11.087 / 17.234 | 6.618 / 14.133 | 0.005 / 0.019 | 2.517 / 3.013 | 0.847 / 4.258 | 3.311 / 6.984 | 6 / 6 | 4 |
| slotq-a-cold | 48 / 48 | 160.435 / 289.441 | 61.942 / 173.823 | 0.006 / 0.014 | 45.123 / 97.875 | 10.902 / 58.202 | 57.171 / 153.673 | 16 / 16 | 4 |
| slotq-a-cold / python-agent | 16 / 16 | 172.806 / 295.672 | 45.738 / 163.687 | 0.006 / 0.015 | 91.602 / 100.637 | 40.782 / 59.994 | 130.165 / 159.817 | 11 / 11 | 4 |
| slotq-a-cold / typescript-multistage | 16 / 16 | 103.550 / 222.611 | 61.978 / 179.178 | 0.005 / 0.007 | 39.689 / 44.074 | 0.772 / 1.249 | 40.882 / 44.729 | 6 / 6 | 4 |
| slotq-a-cold / typescript-tools | 16 / 16 | 136.153 / 228.157 | 83.158 / 170.238 | 0.006 / 0.015 | 44.070 / 51.945 | 10.902 / 13.976 | 57.171 / 62.736 | 6 / 6 | 4 |
| slotq-b-warm | 48 / 48 | 11.128 / 15.793 | 7.476 / 12.510 | 0.010 / 0.053 | 2.232 / 3.785 | 0.687 / 0.951 | 3.253 / 4.994 | 18 / 17 | 4 |
| slotq-b-warm / python-agent | 16 / 16 | 9.118 / 15.508 | 5.168 / 11.817 | 0.010 / 0.033 | 1.959 / 3.574 | 0.793 / 0.988 | 3.204 / 5.142 | 9 / 8 | 4 |
| slotq-b-warm / typescript-multistage | 16 / 16 | 13.092 / 15.862 | 9.815 / 12.955 | 0.008 / 0.052 | 2.135 / 3.388 | 0.450 / 0.506 | 2.853 / 4.165 | 7 / 7 | 4 |
| slotq-b-warm / typescript-tools | 16 / 16 | 12.369 / 15.293 | 7.718 / 11.595 | 0.012 / 0.048 | 2.353 / 4.066 | 0.702 / 1.970 | 3.498 / 6.251 | 8 / 8 | 4 |
| slotq-b-cold | 48 / 48 | 149.449 / 275.933 | 53.721 / 152.431 | 0.010 / 0.027 | 50.386 / 105.680 | 11.201 / 43.013 | 62.002 / 148.184 | 24 / 24 | 4 |
| slotq-b-cold / python-agent | 16 / 16 | 205.992 / 283.235 | 68.938 / 153.240 | 0.011 / 0.022 | 99.731 / 106.425 | 40.502 / 43.676 | 139.495 / 150.112 | 12 / 12 | 4 |
| slotq-b-cold / typescript-multistage | 16 / 16 | 89.813 / 193.619 | 49.037 / 153.786 | 0.009 / 0.031 | 41.260 / 49.254 | 0.837 / 1.147 | 42.354 / 54.477 | 5 / 5 | 4 |
| slotq-b-cold / typescript-tools | 16 / 16 | 119.413 / 185.978 | 59.932 / 120.626 | 0.011 / 0.023 | 50.386 / 59.589 | 11.201 / 15.418 | 62.002 / 70.907 | 8 / 8 | 4 |

## Host telemetry

CPU is occupied cores, not percent. Mean / p95 / maximum refer to sampled intervals; driver CPU is reported separately.

| Phase / host | Coverage | Host CPU | Gateway API CPU | Registry CPU | Driver CPU | Health failures / probes | Health p95 ms |
|---|---:|---:|---:|---:|---:|---:|---:|
| slotq-a-condition / builder-168008406 | 26.9% | 2.291 / 5.710 / 5.710 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-condition / builder-168008407 | 26.9% | 2.761 / 4.360 / 4.360 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-condition / builder-168008410 | 26.9% | 0.864 / 4.960 / 4.960 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-condition / builder-168008411 | 26.9% | 1.350 / 4.980 / 4.980 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-condition / gateway | 97.4% | 0.367 / 1.660 / 2.350 | 0.079 / 0.145 / 0.200 | 0.115 / 0.420 / 0.730 | 0.078 / 1.230 / 1.310 | 0 / 0 | — |
| slotq-a-warm / builder-168008406 | 91.0% | 1.816 / 2.835 / 2.835 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-warm / builder-168008407 | 91.0% | 2.222 / 2.925 / 2.925 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-warm / builder-168008410 | 91.0% | 1.426 / 2.490 / 2.490 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-warm / builder-168008411 | 91.0% | 2.042 / 2.835 / 2.835 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-warm / gateway | 91.0% | 1.384 / 2.485 / 2.485 | 0.123 / 0.215 / 0.215 | 0.454 / 0.745 / 0.745 | 0.617 / 1.355 / 1.355 | 0 / 0 | — |
| slotq-a-cold / builder-168008406 | 99.6% | 3.911 / 6.052 / 6.840 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-cold / builder-168008407 | 99.6% | 3.985 / 5.985 / 6.660 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-cold / builder-168008410 | 99.6% | 4.009 / 6.225 / 6.680 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-cold / builder-168008411 | 99.6% | 3.553 / 6.050 / 6.660 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-a-cold / gateway | 99.6% | 0.353 / 1.090 / 2.090 | 0.068 / 0.145 / 0.220 | 0.143 / 0.665 / 1.415 | 0.046 / 0.025 / 1.390 | 0 / 0 | — |
| slotq-b-warm / builder-168008406 | 93.1% | 2.442 / 3.230 / 3.230 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-warm / builder-168008407 | 81.4% | 1.984 / 3.145 / 3.145 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-warm / builder-168008410 | 81.4% | 2.308 / 3.145 / 3.145 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-warm / builder-168008411 | 81.4% | 2.247 / 3.430 / 3.430 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-warm / gateway | 81.4% | 1.594 / 2.390 / 2.390 | 0.129 / 0.180 / 0.180 | 0.545 / 0.775 / 0.775 | 0.688 / 1.095 / 1.095 | 0 / 0 | — |
| slotq-b-cold / builder-168008406 | 98.9% | 3.570 / 6.859 / 7.290 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-cold / builder-168008407 | 99.6% | 4.300 / 6.535 / 6.860 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-cold / builder-168008410 | 98.9% | 4.384 / 6.780 / 7.120 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-cold / builder-168008411 | 99.6% | 3.940 / 6.620 / 6.880 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0.000 / 0.000 / 0.000 | 0 / 0 | — |
| slotq-b-cold / gateway | 99.6% | 0.376 / 1.130 / 2.555 | 0.069 / 0.155 / 0.230 | 0.150 / 0.600 / 1.955 | 0.048 / 0.025 / 1.090 | 0 / 0 | — |

## Disk activity

Active devices are separate accounting views: a physical disk and its partition/loop device can describe the same I/O. This table does not attribute all disk traffic to the registry.

| Phase / host / device | Read MiB/s p95 / max | Write MiB/s p95 / max | I/O wait ms p95 | Busy % p95 |
|---|---:|---:|---:|---:|
| slotq-a-condition / builder-168008406 / loop0 | 1.238 / 1.238 | 43.874 / 43.874 | 1.717 | 49.650 |
| slotq-a-condition / builder-168008406 / sda | 0.002 / 0.002 | 46.751 / 46.751 | 2.176 | 41.100 |
| slotq-a-condition / builder-168008406 / sda1 | 0.002 / 0.002 | 46.751 / 46.751 | 2.176 | 41.800 |
| slotq-a-condition / builder-168008407 / loop0 | 1.365 / 1.365 | 105.644 / 105.644 | 5.257 | 75.751 |
| slotq-a-condition / builder-168008407 / sda | 0.002 / 0.002 | 108.075 / 108.075 | 2.733 | 59.401 |
| slotq-a-condition / builder-168008407 / sda1 | 0.002 / 0.002 | 108.075 / 108.075 | 2.733 | 61.151 |
| slotq-a-condition / builder-168008410 / loop0 | 0.242 / 0.242 | 31.410 / 31.410 | 1.116 | 39.601 |
| slotq-a-condition / builder-168008410 / sda | 0.002 / 0.002 | 33.588 / 33.588 | 7.182 | 29.050 |
| slotq-a-condition / builder-168008410 / sda1 | 0.002 / 0.002 | 33.588 / 33.588 | 7.182 | 30.250 |
| slotq-a-condition / builder-168008411 / loop0 | 0.621 / 0.621 | 26.045 / 26.045 | 1.774 | 65.401 |
| slotq-a-condition / builder-168008411 / sda | 0.000 / 0.000 | 47.463 / 47.463 | 3.513 | 46.450 |
| slotq-a-condition / builder-168008411 / sda1 | 0.000 / 0.000 | 47.463 / 47.463 | 3.487 | 47.850 |
| slotq-a-condition / gateway / sda | 0.082 / 0.691 | 4.443 / 7.666 | 0.825 | 1.100 |
| slotq-a-condition / gateway / sda1 | 0.082 / 0.691 | 4.443 / 7.666 | 0.825 | 1.150 |
| slotq-a-condition / gateway / sdb | 0.021 / 0.072 | 25.861 / 31.035 | 8.563 | 83.650 |
| slotq-a-warm / builder-168008406 / loop0 | 0.000 / 0.000 | 12.698 / 12.698 | 0.467 | 88.750 |
| slotq-a-warm / builder-168008406 / sda | 0.000 / 0.000 | 21.934 / 21.934 | 0.435 | 70.500 |
| slotq-a-warm / builder-168008406 / sda1 | 0.000 / 0.000 | 21.936 / 21.936 | 0.435 | 71.350 |
| slotq-a-warm / builder-168008407 / loop0 | 0.121 / 0.121 | 12.032 / 12.032 | 0.429 | 70.298 |
| slotq-a-warm / builder-168008407 / sda | 0.000 / 0.000 | 42.204 / 42.204 | 0.361 | 56.648 |
| slotq-a-warm / builder-168008407 / sda1 | 0.000 / 0.000 | 42.204 / 42.204 | 0.361 | 57.698 |
| slotq-a-warm / builder-168008410 / loop0 | 0.000 / 0.000 | 11.591 / 11.591 | 0.674 | 88.797 |
| slotq-a-warm / builder-168008410 / sda | 0.000 / 0.000 | 21.416 / 21.416 | 0.545 | 71.348 |
| slotq-a-warm / builder-168008410 / sda1 | 0.000 / 0.000 | 21.416 / 21.416 | 0.545 | 72.198 |
| slotq-a-warm / builder-168008411 / loop0 | 0.121 / 0.121 | 13.578 / 13.578 | 0.528 | 87.600 |
| slotq-a-warm / builder-168008411 / sda | 0.000 / 0.000 | 52.501 / 52.501 | 0.553 | 70.350 |
| slotq-a-warm / builder-168008411 / sda1 | 0.000 / 0.000 | 52.501 / 52.501 | 0.550 | 71.650 |
| slotq-a-warm / gateway / sda | 0.000 / 0.000 | 2.641 / 2.641 | 0.141 | 2.300 |
| slotq-a-warm / gateway / sda1 | 0.000 / 0.000 | 2.641 / 2.641 | 0.141 | 2.450 |
| slotq-a-warm / gateway / sdb | 0.008 / 0.008 | 23.049 / 23.049 | 2.685 | 97.401 |
| slotq-a-cold / builder-168008406 / loop0 | 11.772 / 29.612 | 231.184 / 580.515 | 5.056 | flagged |
| slotq-a-cold / builder-168008406 / sda | 3.215 / 9.379 | 242.237 / 612.561 | 7.897 | 29.999 |
| slotq-a-cold / builder-168008406 / sda1 | 3.215 / 9.379 | 242.237 / 612.561 | 7.897 | 32.599 |
| slotq-a-cold / builder-168008407 / loop0 | 25.018 / 63.625 | 384.515 / 525.735 | 4.700 | 69.499 |
| slotq-a-cold / builder-168008407 / sda | 16.176 / 33.107 | 382.370 / 519.570 | 6.909 | 34.950 |
| slotq-a-cold / builder-168008407 / sda1 | 16.147 / 33.107 | 382.370 / 519.570 | 6.910 | 39.250 |
| slotq-a-cold / builder-168008410 / loop0 | 0.621 / 10.166 | 256.111 / 405.249 | 4.639 | 62.550 |
| slotq-a-cold / builder-168008410 / sda | 0.037 / 2.563 | 278.638 / 461.717 | 4.053 | 35.901 |
| slotq-a-cold / builder-168008410 / sda1 | 0.037 / 2.563 | 278.638 / 461.717 | 4.053 | 38.201 |
| slotq-a-cold / builder-168008411 / loop0 | 0.619 / 16.734 | 280.618 / 566.283 | 4.866 | 61.950 |
| slotq-a-cold / builder-168008411 / sda | 0.715 / 15.795 | 290.113 / 615.556 | 6.728 | 33.300 |
| slotq-a-cold / builder-168008411 / sda1 | 0.715 / 15.795 | 290.113 / 615.556 | 6.728 | 37.550 |
| slotq-a-cold / gateway / sda | 0.232 / 0.809 | 8.452 / 12.104 | 0.801 | 0.750 |
| slotq-a-cold / gateway / sda1 | 0.232 / 0.809 | 8.452 / 12.104 | 0.804 | 0.750 |
| slotq-a-cold / gateway / sdb | 0.006 / 0.066 | 251.013 / 299.059 | 153.855 | flagged |
| slotq-b-warm / builder-168008406 / loop0 | 0.219 / 0.219 | 12.530 / 12.530 | 0.437 | 89.000 |
| slotq-b-warm / builder-168008406 / sda | 0.717 / 0.717 | 24.758 / 24.758 | 0.401 | 68.650 |
| slotq-b-warm / builder-168008406 / sda1 | 0.717 / 0.717 | 24.758 / 24.758 | 0.401 | 70.300 |
| slotq-b-warm / builder-168008407 / loop0 | 0.348 / 0.348 | 10.653 / 10.653 | 1.259 | 86.300 |
| slotq-b-warm / builder-168008407 / sda | 4.869 / 4.869 | 21.190 / 21.190 | 0.596 | 67.900 |
| slotq-b-warm / builder-168008407 / sda1 | 4.869 / 4.869 | 21.190 / 21.190 | 0.596 | 69.650 |
| slotq-b-warm / builder-168008410 / loop0 | 0.248 / 0.248 | 14.817 / 14.817 | 0.443 | 79.849 |
| slotq-b-warm / builder-168008410 / sda | 1.381 / 1.381 | 26.130 / 26.130 | 0.406 | 64.149 |
| slotq-b-warm / builder-168008410 / sda1 | 1.381 / 1.381 | 26.130 / 26.130 | 0.406 | 65.499 |
| slotq-b-warm / builder-168008411 / loop0 | 18.109 / 18.109 | 13.158 / 13.158 | 0.497 | 90.801 |
| slotq-b-warm / builder-168008411 / sda | 26.537 / 26.537 | 63.676 / 63.676 | 0.353 | 71.851 |
| slotq-b-warm / builder-168008411 / sda1 | 26.537 / 26.537 | 63.676 / 63.676 | 0.353 | 74.651 |
| slotq-b-warm / gateway / sda | 32.260 / 32.260 | 32.072 / 32.072 | 0.951 | 47.400 |
| slotq-b-warm / gateway / sda1 | 32.260 / 32.260 | 32.072 / 32.072 | 0.951 | 65.251 |
| slotq-b-warm / gateway / sdb | 0.389 / 0.389 | 22.316 / 22.316 | 4.290 | 96.900 |
| slotq-b-cold / builder-168008406 / loop0 | 29.634 / 55.354 | 200.450 / 567.999 | 4.400 | 64.352 |
| slotq-b-cold / builder-168008406 / sda | 19.650 / 46.010 | 251.607 / 574.688 | 9.793 | 35.851 |
| slotq-b-cold / builder-168008406 / sda1 | 19.650 / 46.010 | 251.607 / 574.688 | 9.793 | 39.001 |
| slotq-b-cold / builder-168008407 / loop0 | 53.044 / 67.139 | 289.199 / 608.439 | 3.039 | 74.149 |
| slotq-b-cold / builder-168008407 / sda | 34.787 / 56.591 | 298.300 / 663.557 | 5.266 | 40.199 |
| slotq-b-cold / builder-168008407 / sda1 | 34.787 / 56.591 | 298.300 / 663.557 | 5.266 | 43.299 |
| slotq-b-cold / builder-168008410 / loop0 | 37.797 / 78.219 | 331.773 / 602.403 | 4.001 | 76.451 |
| slotq-b-cold / builder-168008410 / sda | 27.680 / 66.564 | 338.386 / 603.908 | 5.570 | 40.598 |
| slotq-b-cold / builder-168008410 / sda1 | 27.680 / 66.564 | 338.386 / 603.908 | 5.570 | 46.680 |
| slotq-b-cold / builder-168008411 / loop0 | 38.551 / 64.201 | 298.833 / 467.431 | 3.729 | 72.499 |
| slotq-b-cold / builder-168008411 / sda | 26.662 / 58.859 | 258.099 / 470.715 | 5.942 | 35.100 |
| slotq-b-cold / builder-168008411 / sda1 | 26.662 / 58.859 | 258.099 / 470.715 | 5.942 | 40.050 |
| slotq-b-cold / gateway / sda | 7.484 / 34.500 | 15.823 / 23.889 | 0.599 | 3.950 |
| slotq-b-cold / gateway / sda1 | 7.484 / 34.500 | 15.823 / 23.889 | 0.695 | 4.100 |
| slotq-b-cold / gateway / sdb | 0.090 / 19.430 | 275.754 / 300.900 | 167.696 | 97.899 |

Interface rates, CPU pressure, disk queue depth and other process groups remain separate in JSON telemetry summaries.

## Observed evidence

- **slotq-a-condition:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 791 HTTP 503 responses and 791 repeated submit attempts; use categories to distinguish admission from polling failures. builder-168008406.jsonl.gz covers 26.9% of the recorded client window; host statistics are partial. builder-168008407.jsonl.gz covers 26.9% of the recorded client window; host statistics are partial. builder-168008410.jsonl.gz covers 26.9% of the recorded client window; host statistics are partial. builder-168008411.jsonl.gz covers 26.9% of the recorded client window; host statistics are partial.
- EROFS counters for slotq-a-condition: 162 groups reused, 46 built, 364888064 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-a-warm:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 37 HTTP 503 responses and 37 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for slotq-a-warm: 206 groups reused, 2 built, 29843456 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-a-cold:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 16/16. Observed 1497 HTTP 503 responses and 1497 repeated submit attempts; use categories to distinguish admission from polling failures. builder-168008406.jsonl.gz: unreliable disk-utilization counters flagged for disk/loop0/busy_percent. gateway.jsonl.gz: unreliable disk-utilization counters flagged for disk/sdb/busy_percent.
- EROFS counters for slotq-a-cold: 117 groups reused, 91 built, 12904275968 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-b-warm:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 18/17. Observed 16 HTTP 503 responses and 16 repeated submit attempts; use categories to distinguish admission from polling failures. builder-168008407.jsonl.gz covers 81.4% of the recorded client window; host statistics are partial. builder-168008410.jsonl.gz covers 81.4% of the recorded client window; host statistics are partial. builder-168008411.jsonl.gz covers 81.4% of the recorded client window; host statistics are partial. gateway.jsonl.gz covers 81.4% of the recorded client window; host statistics are partial.
- EROFS counters for slotq-b-warm: 207 groups reused, 1 built, 14921728 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.
- **slotq-b-cold:** 48/48 builds succeeded; 4 known builder owners; observed admitted/executing peaks 24/24. Observed 1236 HTTP 503 responses and 1236 repeated submit attempts; use categories to distinguish admission from polling failures.
- EROFS counters for slotq-b-cold: 118 groups reused, 90 built, 12900573184 newly built bytes. Cached multistage compile-vertex evidence: 0 records; this does not exclude concurrent executed vertices.

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
