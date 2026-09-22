# Shared storage quota investigation, 2026-09-22

The outage cannot be attributed solely to unrelated project data. Both training data and sandbox infrastructure contribute to shared UCloud storage.

## Accounting

The raw storage wallet reports **178,098 GB total/local usage against 173,000 GB quota**, with no usable headroom. The CLI `files df` summary reports active usage of 173,000 GB, which hides the 5,098 GB excess. The last significant wallet update is 09:17 UTC. Immediately after cleanup the wallet still reported those values. Before production recovery, the raw wallet updated to **159,624 GB usage / 173,000 GB quota**, leaving **13,376 GB usable headroom**. Do not attribute the entire accounting decrease to these deletions: VM retirement and asynchronous accounting also occurred.

Accounting history shows 109,206 GB at the beginning of the September 8–22 window, 164,941 GB at 04:00 UTC September 22, and 178,098 GB in the live wallet. Repeated changes around 2,147 GB match the configured logical size of a 2,000 GiB worker disk. This is a correlation, not proof of the deployed provider's sparse-file charging rules.

## Sandbox-owned storage

- **63 obsolete Docker XFS images**, 61 × 200 GiB and 2 × 30 GiB, remained under `/1020791/Jobs/VirtualMachines/<job>/logs/ucloud-sandboxes/docker-xfs.img`. Total logical size: **13,164,074,762,240 bytes**. All associated jobs were freshly verified as `SUCCESS` with sandbox job names. Recorded file dates were June 29–July 4. These are retained job-output files, separate from the VM boot disks.
- At inspection, five running workers had 2,000 GiB `disk.img` files and the suspended gateway had a 250 GiB image. Logical capacity is not physical allocation. Worker 12398499 reported 182 GiB used in its 1.9 TiB filesystem, including approximately 191 GB allocated under `/var/lib/ucloud-sandboxes`.
- Current worker Docker storage defaults to `/var/lib/ucloud-sandboxes`, rather than the historical job-output paths.
- Registry enumeration is incomplete: the old registry contains at least **351 GB**, and the live deployment at least **212 GB** of enumerated registry files. Do not report these as complete totals or conclude the live registry is small from the earlier 600-request scan.

## Other identified storage

Bounded metadata scans found at least **20.41 TB in `/1003507/FlexMoRE`**, **1.42 TB in `/1003507/brainsurgery`**, **1.18 TB in `/1003507/offpolicy_kd`**, and **2.66 TB in `/998037/.home/.cache/huggingface`** after the selected model cleanup. These are logical file sizes and incomplete lower bounds; some directories were not traversed because of time/depth limits or API 502 responses. Recent modification dates cannot by themselves identify which workload caused the quota crossing. Full project attribution remains incomplete.

## Authorized cleanup completed

1. Permanently removed the three user-selected Hugging Face caches: Step-3.5-Flash, MiniMax-M2, and Hunyuan-A13B-Instruct (789,761,225,715 enumerated blob bytes). NVIDIA cache preserved.
2. Following the user's request to remove obsolete disk images, permanently removed exactly the 63 old Docker XFS images above. Freshly checked job completion and unchanged file metadata first. UCloud direct DELETE was unsupported, so the supported trash workflow was used. Trash was initially absent/empty; exactly the expected 63 files were verified before emptying it. UCloud updates timestamps during move-to-trash, so final verification used successful move responses, exact names/count/size histogram, stable trash paths, and absence of all original paths. Trash is now empty.

Deletion manifest: `../benchmarks/storage-quota-2026-09-22/deleted-obsolete-images.json`.

No running worker disk, gateway boot disk, registry, checkpoint, or other training data was deleted. The 13.16 TB figure is logical size, not a verified physical-space or quota reduction. Quota recovery was confirmed and production resumed. The provider had also detached the gateway data-drive resource; reattaching `/998037` required a clean suspend/resume before `/work/data` returned. PostgreSQL data remained intact on the gateway-local disk; its authority and backups were verified before rollout. See the release review.
