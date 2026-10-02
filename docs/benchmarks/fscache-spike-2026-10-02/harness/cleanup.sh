#!/bin/bash
cd /root/s11
for c in $(bundle/runtime/direct/runsc --root=/root/s11/runsc-root list -q 2>/dev/null); do bundle/runtime/direct/runsc --root=/root/s11/runsc-root delete --force $c; done
pkill -9 -f "[r]unsc-(gofer|sandbox) --root=/root/s11"; sleep 0.5; pkill -9 -f "[n]bd_backend.py"; pkill -9 -f "[s]moke.py"; pkill -9 -f "[b]ench.py"; pkill -9 -f "[v]erify_fscache.py"
for m in $(awk '{print $5}' /proc/self/mountinfo | grep -E '^/root/s11/(sb|mnt|envstore|envio)/' | sort -r); do umount $m 2>/dev/null || umount -l $m; done
pkill -x nydusd; sleep 1; pkill -9 -x nydusd
umount /root/s11/runsc-root/null-netns 2>/dev/null; rm -rf /root/s11/runsc-root
rm -rf /root/s11/fscache-cache /root/s11/sb /root/s11/boot /root/s11/envio /root/s11/envstore
awk '{print $5}' /proc/self/mountinfo | grep -c '^/root/s11/' || true
pgrep -a nydusd || true
