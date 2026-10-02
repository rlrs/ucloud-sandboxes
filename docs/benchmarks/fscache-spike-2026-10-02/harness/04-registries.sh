#!/bin/bash
# Two local registries: 5001 holds the converted Nydus images; 5002 is a read-only
# pull-through cache of the production registry for our EROFS/NBD path.
set -euxo pipefail
W=/root/s11; mkdir -p $W/reg-nydus $W/reg-proxy $W/logs
cat > $W/reg-nydus.yml <<Y
version: 0.1
log: {level: warn}
storage: {filesystem: {rootdirectory: $W/reg-nydus}, delete: {enabled: true}}
http: {addr: 127.0.0.1:5001}
Y
cat > $W/reg-proxy.yml <<Y
version: 0.1
log: {level: warn}
storage: {filesystem: {rootdirectory: $W/reg-proxy}}
http: {addr: 127.0.0.1:5002}
proxy: {remoteurl: http://10.42.0.2:5000}
Y
pkill -f "registry serve" || true
nohup registry serve $W/reg-nydus.yml > $W/logs/reg-nydus.log 2>&1 &
nohup registry serve $W/reg-proxy.yml > $W/logs/reg-proxy.log 2>&1 &
sleep 2
curl -fsS http://127.0.0.1:5001/v2/ && curl -fsS http://127.0.0.1:5002/v2/ && curl -fsS -o /dev/null -w "prod %{http_code}\n" http://10.42.0.2:5000/v2/
# Byte counters for what each registry sends (loopback), read by the benchmark.
nft -f - <<N
table inet s11 {
  counter nydus_tx {}
  counter proxy_tx {}
  chain out { type filter hook output priority 0; policy accept;
    tcp sport 5001 counter name "nydus_tx"
    tcp sport 5002 counter name "proxy_tx"
  }
}
N
nft list counters table inet s11
