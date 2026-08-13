#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: install_hetzner_otlp_stack.sh \
  --private-bind-ip <ipv4> \
  --s3-endpoint <https-url> \
  --s3-bucket <bucket> \
  --s3-region <region> \
  --s3-prefix <prefix> \
  --credentials-env-file <path> \
  [--data-root <path>]

The credential file must define HETZNER_S3_ACCESS_KEY and
HETZNER_S3_SECRET_KEY. OTLP/HTTP is exposed only on the supplied private IP;
Tempo and VictoriaMetrics query ports remain loopback-only.
EOF
}

private_bind_ip=""
s3_endpoint=""
s3_bucket=""
s3_region=""
s3_prefix=""
credentials_env_file=""
data_root=/mnt/ucloud-registry/telemetry
while (($#)); do
  case "$1" in
    --private-bind-ip) private_bind_ip="${2:-}"; shift 2 ;;
    --s3-endpoint) s3_endpoint="${2:-}"; shift 2 ;;
    --s3-bucket) s3_bucket="${2:-}"; shift 2 ;;
    --s3-region) s3_region="${2:-}"; shift 2 ;;
    --s3-prefix) s3_prefix="${2:-}"; shift 2 ;;
    --credentials-env-file) credentials_env_file="${2:-}"; shift 2 ;;
    --data-root) data_root="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

if [[ "$EUID" -ne 0 ]]; then
  echo "OTLP stack installation must run as root" >&2
  exit 2
fi
if [[ ! "$private_bind_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "--private-bind-ip must be an IPv4 address" >&2
  exit 2
fi
if [[ ! "$s3_endpoint" =~ ^https://[^/]+/?$ ]]; then
  echo "--s3-endpoint must be one HTTPS origin" >&2
  exit 2
fi
if [[ ! "$s3_bucket" =~ ^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$ ]]; then
  echo "--s3-bucket is invalid" >&2
  exit 2
fi
if [[ ! "$s3_region" =~ ^[A-Za-z0-9-]+$ ]]; then
  echo "--s3-region is invalid" >&2
  exit 2
fi
if [[ ! "$s3_prefix" =~ ^[A-Za-z0-9._/-]+$ || "$s3_prefix" == /* || "$s3_prefix" == */ ]]; then
  echo "--s3-prefix must be a non-empty relative object prefix" >&2
  exit 2
fi
if [[ ! -s "$credentials_env_file" ]]; then
  echo "credential environment file is absent" >&2
  exit 2
fi
for variable in HETZNER_S3_ACCESS_KEY HETZNER_S3_SECRET_KEY; do
  if ! grep -qE "^${variable}=.+" "$credentials_env_file"; then
    echo "credential environment file does not define $variable" >&2
    exit 2
  fi
done
if [[ "$data_root" != /* ]]; then
  echo "--data-root must be absolute" >&2
  exit 2
fi

config_root=/etc/ucloud-sandboxes/telemetry
s3_host="${s3_endpoint#https://}"
s3_host="${s3_host%/}"
install -d -m 0755 "$config_root" "$data_root/tempo" "$data_root/victoria-metrics"
chmod 0700 "$data_root/tempo" "$data_root/victoria-metrics"

python3 - \
  "$config_root/tempo.yaml" "$s3_host" "$s3_bucket" "$s3_region" "$s3_prefix" <<'PY'
import json
from pathlib import Path
import sys

target, endpoint, bucket, region, prefix = sys.argv[1:]
quote = json.dumps
Path(target).write_text(
    f"""stream_over_http_enabled: true
server:
  http_listen_port: 3200
  grpc_listen_port: 9095
distributor:
  receivers:
    otlp:
      protocols:
        grpc:
          endpoint: 0.0.0.0:4317
        http:
          endpoint: 0.0.0.0:4318
ingester:
  max_block_duration: 5m
  complete_block_timeout: 10m
  flush_all_on_shutdown: true
compactor:
  compaction:
    block_retention: 168h
storage:
  trace:
    backend: s3
    wal:
      path: /var/tempo/wal
    s3:
      endpoint: {quote(endpoint)}
      bucket: {quote(bucket)}
      region: {quote(region)}
      prefix: {quote(prefix)}
      access_key: ${{HETZNER_S3_ACCESS_KEY}}
      secret_key: ${{HETZNER_S3_SECRET_KEY}}
      insecure: false
      forcepathstyle: false
usage_report:
  reporting_enabled: false
""",
    encoding="utf-8",
)
PY

cat >"$config_root/collector.yaml" <<'EOF'
extensions:
  health_check:
    endpoint: 0.0.0.0:13133
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
processors:
  memory_limiter:
    check_interval: 1s
    limit_mib: 128
    spike_limit_mib: 32
  batch:
    send_batch_size: 512
    timeout: 2s
exporters:
  otlp_grpc/tempo:
    endpoint: ucloud-telemetry-tempo:4317
    tls:
      insecure: true
    sending_queue:
      enabled: true
      queue_size: 2048
    retry_on_failure:
      enabled: true
      max_elapsed_time: 60s
  prometheusremotewrite/victoria:
    endpoint: http://ucloud-telemetry-victoria:8428/api/v1/write
    tls:
      insecure: true
    resource_to_telemetry_conversion:
      enabled: true
    remote_write_queue:
      enabled: true
      queue_size: 2048
    max_batch_request_parallelism: 1
service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [otlp_grpc/tempo]
    metrics:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [prometheusremotewrite/victoria]
  telemetry:
    metrics:
      level: normal
EOF

install -d -m 0755 /etc/systemd/system
cat >/etc/systemd/system/ucloud-telemetry-tempo.service <<EOF
[Unit]
Description=UCloud Tempo trace store
After=docker.service network-online.target
Wants=docker.service network-online.target
RequiresMountsFor=$data_root

[Service]
Restart=always
RestartSec=5
ExecStartPre=-/usr/bin/docker network create ucloud-telemetry
ExecStartPre=-/usr/bin/docker rm -f ucloud-telemetry-tempo
ExecStart=/usr/bin/docker run --name ucloud-telemetry-tempo --network ucloud-telemetry --memory 512m --cpus 1 --user 0:0 --env-file $credentials_env_file --log-driver journald -p 127.0.0.1:3200:3200 -v $config_root/tempo.yaml:/etc/tempo.yaml:ro -v $data_root/tempo:/var/tempo grafana/tempo:2.10.7 -config.file=/etc/tempo.yaml -config.expand-env=true
ExecStop=/usr/bin/docker stop --time 30 ucloud-telemetry-tempo
TimeoutStopSec=40

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/ucloud-telemetry-victoria.service <<EOF
[Unit]
Description=UCloud VictoriaMetrics store
After=docker.service network-online.target
Wants=docker.service network-online.target
RequiresMountsFor=$data_root

[Service]
Restart=always
RestartSec=5
ExecStartPre=-/usr/bin/docker network create ucloud-telemetry
ExecStartPre=-/usr/bin/docker rm -f ucloud-telemetry-victoria
ExecStart=/usr/bin/docker run --name ucloud-telemetry-victoria --network ucloud-telemetry --memory 256m --cpus 0.5 --user 0:0 --log-driver journald -p 127.0.0.1:8428:8428 -v $data_root/victoria-metrics:/victoria-metrics-data victoriametrics/victoria-metrics:v1.148.0 -storageDataPath=/victoria-metrics-data -retentionPeriod=14d -storage.minFreeDiskSpaceBytes=5GB -memory.allowedPercent=50 -selfScrapeInterval=10s
ExecStop=/usr/bin/docker stop --time 30 ucloud-telemetry-victoria
TimeoutStopSec=40

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/ucloud-telemetry-collector.service <<EOF
[Unit]
Description=UCloud OpenTelemetry Collector
After=docker.service network-online.target ucloud-telemetry-tempo.service ucloud-telemetry-victoria.service
Wants=docker.service network-online.target
Requires=ucloud-telemetry-tempo.service ucloud-telemetry-victoria.service

[Service]
Restart=always
RestartSec=5
ExecStartPre=-/usr/bin/docker network create ucloud-telemetry
ExecStartPre=-/usr/bin/docker rm -f ucloud-telemetry-collector
ExecStart=/usr/bin/docker run --name ucloud-telemetry-collector --network ucloud-telemetry --memory 192m --cpus 0.5 --log-driver journald -p $private_bind_ip:4318:4318 -p 127.0.0.1:13133:13133 -v $config_root/collector.yaml:/etc/otelcol-contrib/config.yaml:ro otel/opentelemetry-collector-contrib:0.153.0 --config=/etc/otelcol-contrib/config.yaml
ExecStop=/usr/bin/docker stop --time 20 ucloud-telemetry-collector
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

docker pull grafana/tempo:2.10.7
docker pull victoriametrics/victoria-metrics:v1.148.0
docker pull otel/opentelemetry-collector-contrib:0.153.0
systemctl daemon-reload
systemctl enable \
  ucloud-telemetry-tempo.service \
  ucloud-telemetry-victoria.service \
  ucloud-telemetry-collector.service
systemctl restart \
  ucloud-telemetry-tempo.service \
  ucloud-telemetry-victoria.service
systemctl restart ucloud-telemetry-collector.service

for url in \
  http://127.0.0.1:3200/ready \
  http://127.0.0.1:8428/-/healthy \
  http://127.0.0.1:13133/; do
  for attempt in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null; then break; fi
    if [[ "$attempt" == 60 ]]; then
      systemctl --no-pager --full status \
        ucloud-telemetry-tempo.service \
        ucloud-telemetry-victoria.service \
        ucloud-telemetry-collector.service
      exit 1
    fi
    sleep 1
  done
done

printf 'otlp_endpoint=http://%s:4318\n' "$private_bind_ip"
printf 'tempo_query=http://127.0.0.1:3200\n'
printf 'metrics_query=http://127.0.0.1:8428\n'
