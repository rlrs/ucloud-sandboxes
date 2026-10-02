#!/bin/bash
# S5: can a gVisor checkpoint restore into a different netns with a different address?
# Disposable VM only. Creates netns s5a/s5b, veths, bundles under /var/lib/rl-spike/s5.
set -u
RUNSC=/usr/local/libexec/ucloud-gvisor/runsc
ROOT=/var/lib/rl-spike/s5
STATE=$ROOT/state
log() { echo "== $*"; }
cleanup() {
  for c in s5-src s5-dst s5-dst2; do $RUNSC --root=$STATE delete --force $c >/dev/null 2>&1; done
  for n in a b; do ip netns del s5$n >/dev/null 2>&1; ip link del s5h$n >/dev/null 2>&1; done
}
trap cleanup EXIT
cleanup
rm -rf $ROOT; mkdir -p $ROOT/rootfs/bin $STATE $ROOT/ckpt
cp /usr/bin/busybox $ROOT/rootfs/bin/busybox
for t in sh sleep ip cat wget nc ping ifconfig route; do ln -sf busybox $ROOT/rootfs/bin/$t; done

mkns() { # name idx
  ip netns add s5$1
  ip link add s5h$1 type veth peer name eth0 netns s5$1
  ip addr add 10.200.$2.1/24 dev s5h$1; ip link set s5h$1 up
  ip netns exec s5$1 ip addr add 10.200.$2.2/24 dev eth0
  ip netns exec s5$1 ip link set eth0 up
  ip netns exec s5$1 ip link set lo up
  ip netns exec s5$1 ip route add default via 10.200.$2.1
}
bundle() { # dir netns
  mkdir -p $1; ln -sfn $ROOT/rootfs $1/rootfs
  cat > $1/config.json <<EOF
{"ociVersion":"1.0.2","process":{"user":{"uid":0,"gid":0},"args":["/bin/sleep","100000"],"env":["PATH=/bin"],"cwd":"/"},
 "root":{"path":"rootfs","readonly":false},"hostname":"s5",
 "mounts":[{"destination":"/proc","type":"proc","source":"proc"},{"destination":"/tmp","type":"tmpfs","source":"tmpfs"}],
 "linux":{"namespaces":[{"type":"pid"},{"type":"ipc"},{"type":"uts"},{"type":"mount"},{"type":"network","path":"/var/run/netns/$2"}]}}
EOF
}
mkns a 1; mkns b 2
bundle $ROOT/src s5a; bundle $ROOT/dst s5b
R="$RUNSC --root=$STATE --network=sandbox --platform=systrap"
log "create source in s5a (10.200.1.2)"
$R create --bundle=$ROOT/src s5-src && $R start s5-src || exit 1
log "source address"; $R exec s5-src /bin/ip addr show eth0 | grep inet
log "source reaches its gateway"; $R exec s5-src /bin/ping -c1 -W2 10.200.1.1 | tail -1
log "guest writes a marker"; $R exec s5-src /bin/sh -c 'echo marker-$$ > /tmp/marker; cat /tmp/marker'
log "checkpoint (stock, leaves source stopped)"
$R checkpoint --image-path=$ROOT/ckpt s5-src; echo "checkpoint rc=$?"
ls -la $ROOT/ckpt | head
log "restore into s5b (10.200.2.x) as new container s5-dst"
$R restore --image-path=$ROOT/ckpt --bundle=$ROOT/dst --detach s5-dst; echo "restore rc=$?"
$R state s5-dst 2>&1 | grep -E '"status"|"pid"' | head -2
log "restored marker"; $R exec s5-dst /bin/cat /tmp/marker
log "restored address"; $R exec s5-dst /bin/ip addr show eth0 | grep inet
log "restored reaches NEW gateway 10.200.2.1"; $R exec s5-dst /bin/ping -c1 -W2 10.200.2.1 | tail -1
log "restored reaches OLD gateway 10.200.1.1"; $R exec s5-dst /bin/ping -c1 -W2 10.200.1.1 | tail -1
log "second restore from the same image (fork-like) into s5b fails or succeeds?"
$R delete --force s5-dst >/dev/null 2>&1
$R restore --image-path=$ROOT/ckpt --bundle=$ROOT/dst --detach s5-dst2; echo "second restore rc=$?"
$R exec s5-dst2 /bin/cat /tmp/marker
