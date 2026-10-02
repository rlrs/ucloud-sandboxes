#!/bin/bash
set -u
RUNSC=/usr/local/libexec/ucloud-gvisor/runsc; ROOT=/var/lib/rl-spike/s5; STATE=$ROOT/state
R="$RUNSC --root=$STATE --network=sandbox --platform=systrap"
cleanup() { for c in s5-src s5-c1 s5-c2; do $RUNSC --root=$STATE delete --force $c >/dev/null 2>&1; done
  for n in a b c; do ip netns del s5$n >/dev/null 2>&1; ip link del s5h$n >/dev/null 2>&1; done; kill $(jobs -p) 2>/dev/null; }
trap cleanup EXIT; cleanup
rm -rf $ROOT/ckpt $ROOT/c1 $ROOT/c2; mkdir -p $ROOT/ckpt
mkns() { ip netns add s5$1; ip link add s5h$1 type veth peer name eth0 netns s5$1; ip addr add 10.200.$2.1/24 dev s5h$1; ip link set s5h$1 up
  ip netns exec s5$1 ip addr add 10.200.$2.2/24 dev eth0; ip netns exec s5$1 ip link set eth0 up; ip netns exec s5$1 ip link set lo up; ip netns exec s5$1 ip route add default via 10.200.$2.1; }
bundle() { mkdir -p $1; ln -sfn $ROOT/rootfs $1/rootfs; sed "s#/var/run/netns/s5a#/var/run/netns/$2#" $ROOT/src/config.json > $1/config.json; }
mkns a 1; mkns b 2; mkns c 3; bundle $ROOT/c1 s5b; bundle $ROOT/c2 s5c
for g in 1 2 3; do python3 -m http.server --bind 10.200.$g.1 8099 --directory /tmp >/dev/null 2>&1 & done; sleep 1
$R create --bundle=$ROOT/src s5-src && $R start s5-src
echo "src -> own gw: $($R exec s5-src /bin/wget -qO- -T 3 http://10.200.1.1:8099/ >/dev/null 2>&1 && echo ok || echo FAIL)"
$R exec s5-src /bin/sh -c 'echo seed-$RANDOM > /tmp/seed; cat /tmp/seed'
t0=$(date +%s.%N); $R checkpoint --image-path=$ROOT/ckpt s5-src; t1=$(date +%s.%N)
for c in 1 2; do s=$(date +%s.%N); $R restore --image-path=$ROOT/ckpt --bundle=$ROOT/c$c --detach s5-c$c; e=$(date +%s.%N); echo "child $c restore $(python3 -c "print(round($e-$s,3))") s"; done
echo "checkpoint $(python3 -c "print(round($t1-$t0,3))") s"
for c in 1 2; do g=$((c+1)); echo "child $c: addr $($R exec s5-c$c /bin/ip -4 addr show eth0 | grep -o 'inet [0-9.]*') seed $($R exec s5-c$c /bin/cat /tmp/seed) RANDOM $($R exec s5-c$c /bin/sh -c 'echo $RANDOM') new-gw $($R exec s5-c$c /bin/wget -qO- -T 3 http://10.200.$g.1:8099/ >/dev/null 2>&1 && echo ok || echo FAIL) old-gw $($R exec s5-c$c /bin/wget -qO- -T 3 http://10.200.1.1:8099/ >/dev/null 2>&1 && echo reachable || echo unreachable)"; done
echo "urandom differs across children: $($R exec s5-c1 /bin/sh -c 'head -c8 /dev/urandom | od -An -tx1') vs $($R exec s5-c2 /bin/sh -c 'head -c8 /dev/urandom | od -An -tx1')"
