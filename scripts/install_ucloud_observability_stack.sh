#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: install_ucloud_observability_stack.sh \
  --private-bind-ip <ipv4> \
  [--data-root <absolute-path>]

Installs a bounded single-gateway observability stack for UCloud. OTLP/HTTP is
exposed only on the supplied private IP. Grafana, Tempo, and VictoriaMetrics
query ports remain loopback-only and are intended to be reached through SSH.
EOF
}

private_bind_ip=""
data_root=/var/lib/ucloud-sandboxes/telemetry
while (($#)); do
  case "$1" in
    --private-bind-ip) private_bind_ip="${2:-}"; shift 2 ;;
    --data-root) data_root="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

if [[ "$EUID" -ne 0 ]]; then
  echo "UCloud observability installation must run as root" >&2
  exit 2
fi
if [[ ! "$private_bind_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "--private-bind-ip must be an IPv4 address" >&2
  exit 2
fi
if ! ip -4 address show | grep -Fq " $private_bind_ip/"; then
  echo "--private-bind-ip is not assigned to this gateway" >&2
  exit 2
fi
if [[ "$data_root" != /* ]]; then
  echo "--data-root must be absolute" >&2
  exit 2
fi
case "$data_root/" in
  /work/*|/mnt/ucloud/*)
    echo "--data-root must use gateway-local storage, not shared UCloud storage" >&2
    exit 2
    ;;
esac

config_root=/etc/ucloud-sandboxes/telemetry
grafana_provisioning="$config_root/grafana/provisioning"
grafana_env="$config_root/grafana.env"
installer_dir="$(cd "$(dirname "$0")" && pwd)"
report_source="$installer_dir/ucloud_observability_report.py"

install -d -m 0755 \
  "$config_root" \
  "$grafana_provisioning/datasources" \
  "$grafana_provisioning/dashboards" \
  "$config_root/grafana/dashboards" \
  "$data_root/tempo" \
  "$data_root/victoria-metrics" \
  "$data_root/grafana"
chmod 0700 "$data_root/tempo" "$data_root/victoria-metrics"
chown -R 472:0 "$data_root/grafana"
chmod 0750 "$data_root/grafana"

cat >"$config_root/tempo.yaml" <<'EOF'
stream_over_http_enabled: true
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
    block_retention: 72h
storage:
  trace:
    backend: local
    wal:
      path: /var/tempo/wal
    local:
      path: /var/tempo/blocks
usage_report:
  reporting_enabled: false
EOF

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
    limit_mib: 192
    spike_limit_mib: 48
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

cat >"$grafana_provisioning/datasources/ucloud.yaml" <<'EOF'
apiVersion: 1
datasources:
  - name: UCloud Metrics
    uid: ucloud-metrics
    type: prometheus
    access: proxy
    url: http://ucloud-telemetry-victoria:8428
    isDefault: true
    editable: false
    jsonData:
      httpMethod: POST
      timeInterval: 5s
  - name: UCloud Traces
    uid: ucloud-traces
    type: tempo
    access: proxy
    url: http://ucloud-telemetry-tempo:3200
    editable: false
    jsonData:
      tracesToMetrics:
        datasourceUid: ucloud-metrics
      serviceMap:
        datasourceUid: ucloud-metrics
      nodeGraph:
        enabled: true
EOF

cat >"$grafana_provisioning/dashboards/ucloud.yaml" <<'EOF'
apiVersion: 1
providers:
  - name: UCloud
    orgId: 1
    folder: UCloud
    type: file
    disableDeletion: true
    updateIntervalSeconds: 30
    allowUiUpdates: false
    options:
      path: /var/lib/grafana/dashboards
EOF

cat >"$config_root/grafana/dashboards/platform-hot-paths.json" <<'EOF'
{
  "annotations": {"list": []},
  "editable": false,
  "graphTooltip": 1,
  "links": [],
  "panels": [
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Completed instrumented platform operations per second.",
      "fieldConfig": {"defaults": {"unit": "ops", "color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 6, "x": 0, "y": 0},
      "id": 1,
      "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"expr": "sum(rate(ucloud_platform_operation_count_total[$__rate_interval]))", "refId": "A"}],
      "title": "Current operation rate",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Share of completed operations whose instrumented status is error.",
      "fieldConfig": {"defaults": {"unit": "percentunit", "min": 0, "max": 1, "color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "yellow", "value": 0.005}, {"color": "red", "value": 0.02}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 6, "x": 6, "y": 0},
      "id": 2,
      "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"expr": "sum(rate(ucloud_platform_operation_count_total{status=\"error\"}[$__rate_interval])) / clamp_min(sum(rate(ucloud_platform_operation_count_total[$__rate_interval])), 0.000001)", "refId": "A"}],
      "title": "Current error ratio",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Overall p95 across instrumented operations. Use the breakdown below to find the responsible operation.",
      "fieldConfig": {"defaults": {"unit": "s", "color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": null}, {"color": "yellow", "value": 1}, {"color": "red", "value": 5}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 6, "x": 12, "y": 0},
      "id": 3,
      "options": {"colorMode": "value", "graphMode": "area", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"expr": "histogram_quantile(0.95, sum by (le) (rate(ucloud_platform_operation_duration_seconds_bucket[$__rate_interval])))", "refId": "A"}],
      "title": "Overall latency p95",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Number of platform services that emitted operation metrics.",
      "fieldConfig": {"defaults": {"decimals": 0, "color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": null}, {"color": "yellow", "value": 2}, {"color": "green", "value": 4}]}}, "overrides": []},
      "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
      "id": 4,
      "options": {"colorMode": "value", "graphMode": "none", "justifyMode": "auto", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": false}, "textMode": "auto"},
      "targets": [{"expr": "count(count by (service_name) (ucloud_platform_operation_count_total))", "refId": "A"}],
      "title": "Services reporting",
      "type": "stat"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Tail latency by operation for the selected services. Parent operations include child time.",
      "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
      "gridPos": {"h": 9, "w": 16, "x": 0, "y": 4},
      "id": 5,
      "options": {"legend": {"calcs": ["lastNotNull", "max"], "displayMode": "table", "placement": "right"}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [
        {"expr": "histogram_quantile(0.95, sum by (le, operation) (rate(ucloud_platform_operation_duration_seconds_bucket{service_name=~\"$service\"}[$__rate_interval])))", "legendFormat": "p95 {{operation}}", "refId": "A"},
        {"expr": "histogram_quantile(0.99, sum by (le, operation) (rate(ucloud_platform_operation_duration_seconds_bucket{service_name=~\"$service\"}[$__rate_interval])))", "legendFormat": "p99 {{operation}}", "refId": "B"}
      ],
      "title": "Which operation is slow?",
      "type": "timeseries"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Operations with errors in the visible time range, split by service.",
      "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
      "gridPos": {"h": 9, "w": 8, "x": 16, "y": 4},
      "id": 6,
      "options": {"showHeader": true, "sortBy": [{"desc": true, "displayName": "Value"}]},
      "targets": [{"expr": "sum by (service_name, operation) (increase(ucloud_platform_operation_count_total{status=\"error\",service_name=~\"$service\"}[$__range]))", "format": "table", "instant": true, "refId": "A"}],
      "title": "What is failing?",
      "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": true}, "renameByName": {"Value": "Errors", "operation": "Operation", "service_name": "Service"}}}],
      "type": "table"
    },
    {
      "datasource": {"type": "prometheus", "uid": "ucloud-metrics"},
      "description": "Traffic split by operation and outcome. Select a service at the top to narrow it.",
      "fieldConfig": {"defaults": {"unit": "ops"}, "overrides": []},
      "gridPos": {"h": 8, "w": 12, "x": 0, "y": 13},
      "id": 7,
      "options": {"legend": {"calcs": [], "displayMode": "table", "placement": "right"}, "tooltip": {"mode": "multi", "sort": "desc"}},
      "targets": [{"expr": "sum by (operation, status) (rate(ucloud_platform_operation_count_total{service_name=~\"$service\"}[$__rate_interval]))", "legendFormat": "{{operation}} / {{status}}", "refId": "A"}],
      "title": "What is the system doing?",
      "type": "timeseries"
    },
    {
      "datasource": {"type": "tempo", "uid": "ucloud-traces"},
      "description": "Sampled traces over 100 ms. Open one to inspect phase spans and thread CPU attributes.",
      "gridPos": {"h": 8, "w": 12, "x": 12, "y": 13},
      "id": 8,
      "options": {"dedupStrategy": "none", "enableLogDetails": true, "prettifyLogMessage": false, "showCommonLabels": false, "showLabels": false, "showTime": true, "sortOrder": "Descending", "wrapLogMessage": false},
      "targets": [{"limit": 50, "query": "{ duration > 100ms }", "queryType": "traceql", "refId": "A", "tableType": "traces"}],
      "title": "Why was it slow?",
      "type": "traces"
    }
  ],
  "refresh": "10s",
  "schemaVersion": 41,
  "tags": ["ucloud", "production", "hot-path"],
  "templating": {"list": [{"allValue": ".*", "current": {"text": "All", "value": "$__all"}, "datasource": {"type": "prometheus", "uid": "ucloud-metrics"}, "definition": "label_values(ucloud_platform_operation_count_total, service_name)", "includeAll": true, "label": "Service", "multi": true, "name": "service", "options": [], "query": {"query": "label_values(ucloud_platform_operation_count_total, service_name)", "refId": "StandardVariableQuery"}, "refresh": 1, "type": "query"}]},
  "time": {"from": "now-1h", "to": "now"},
  "timezone": "browser",
  "title": "UCloud production behavior",
  "uid": "ucloud-platform-hot-paths",
  "version": 2
}
EOF

if [[ ! -s "$grafana_env" ]]; then
  umask 077
  {
    printf 'GF_SECURITY_ADMIN_USER=admin\n'
    printf 'GF_SECURITY_ADMIN_PASSWORD=%s\n' "$(openssl rand -hex 24)"
    printf 'GF_SECURITY_SECRET_KEY=%s\n' "$(openssl rand -hex 32)"
  } >"$grafana_env"
fi
chmod 0600 "$grafana_env"
if [[ -f "$report_source" ]]; then
  install -m 0755 "$report_source" /usr/local/bin/ucloud-observability-report
else
  echo "warning: ucloud_observability_report.py was not beside the installer" >&2
fi

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
ExecStart=/usr/bin/docker run --name ucloud-telemetry-tempo --network ucloud-telemetry --memory 768m --cpus 0.60 --cpu-shares 256 --user 0:0 --log-driver journald -p 127.0.0.1:3200:3200 -v $config_root/tempo.yaml:/etc/tempo.yaml:ro -v $data_root/tempo:/var/tempo grafana/tempo:2.10.7 -config.file=/etc/tempo.yaml
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
ExecStart=/usr/bin/docker run --name ucloud-telemetry-victoria --network ucloud-telemetry --memory 384m --cpus 0.40 --cpu-shares 256 --user 0:0 --log-driver journald -p 127.0.0.1:8428:8428 -v $data_root/victoria-metrics:/victoria-metrics-data victoriametrics/victoria-metrics:v1.148.0 -storageDataPath=/victoria-metrics-data -retentionPeriod=14d -storage.minFreeDiskSpaceBytes=20GB -memory.allowedPercent=50 -selfScrapeInterval=10s
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
ExecStart=/usr/bin/docker run --name ucloud-telemetry-collector --network ucloud-telemetry --memory 320m --cpus 0.50 --cpu-shares 256 --log-driver journald -p $private_bind_ip:4318:4318 -p 127.0.0.1:13133:13133 -v $config_root/collector.yaml:/etc/otelcol-contrib/config.yaml:ro otel/opentelemetry-collector-contrib:0.153.0 --config=/etc/otelcol-contrib/config.yaml
ExecStop=/usr/bin/docker stop --time 20 ucloud-telemetry-collector
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

cat >/etc/systemd/system/ucloud-telemetry-grafana.service <<EOF
[Unit]
Description=UCloud Grafana observability UI
After=docker.service network-online.target ucloud-telemetry-tempo.service ucloud-telemetry-victoria.service
Wants=docker.service network-online.target
Requires=ucloud-telemetry-tempo.service ucloud-telemetry-victoria.service
RequiresMountsFor=$data_root

[Service]
Restart=always
RestartSec=5
ExecStartPre=-/usr/bin/docker network create ucloud-telemetry
ExecStartPre=-/usr/bin/docker rm -f ucloud-telemetry-grafana
ExecStart=/usr/bin/docker run --name ucloud-telemetry-grafana --network ucloud-telemetry --memory 384m --cpus 0.50 --cpu-shares 256 --log-driver journald --env-file $grafana_env -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer -e GF_USERS_ALLOW_SIGN_UP=false -e GF_ANALYTICS_REPORTING_ENABLED=false -e GF_ANALYTICS_CHECK_FOR_UPDATES=false -e GF_SERVER_DOMAIN=127.0.0.1 -p 127.0.0.1:3000:3000 -v $data_root/grafana:/var/lib/grafana -v $grafana_provisioning:/etc/grafana/provisioning:ro -v $config_root/grafana/dashboards:/var/lib/grafana/dashboards:ro grafana/grafana:12.3.1
ExecStop=/usr/bin/docker stop --time 20 ucloud-telemetry-grafana
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF

docker pull grafana/tempo:2.10.7
docker pull victoriametrics/victoria-metrics:v1.148.0
docker pull otel/opentelemetry-collector-contrib:0.153.0
docker pull grafana/grafana:12.3.1
systemctl daemon-reload
systemctl enable \
  ucloud-telemetry-tempo.service \
  ucloud-telemetry-victoria.service \
  ucloud-telemetry-collector.service \
  ucloud-telemetry-grafana.service
systemctl restart \
  ucloud-telemetry-tempo.service \
  ucloud-telemetry-victoria.service
systemctl restart \
  ucloud-telemetry-collector.service \
  ucloud-telemetry-grafana.service

for url in \
  http://127.0.0.1:3200/ready \
  http://127.0.0.1:8428/-/healthy \
  http://127.0.0.1:13133/ \
  http://127.0.0.1:3000/api/health; do
  for attempt in $(seq 1 90); do
    if curl -fsS "$url" >/dev/null; then break; fi
    if [[ "$attempt" == 90 ]]; then
      systemctl --no-pager --full status \
        ucloud-telemetry-tempo.service \
        ucloud-telemetry-victoria.service \
        ucloud-telemetry-collector.service \
        ucloud-telemetry-grafana.service
      exit 1
    fi
    sleep 1
  done
done

printf 'otlp_endpoint=http://%s:4318\n' "$private_bind_ip"
printf 'grafana_tunnel=ssh -L 3000:127.0.0.1:3000 <gateway>\n'
printf 'grafana_url=http://127.0.0.1:3000/d/ucloud-platform-hot-paths\n'
printf 'grafana_admin_credentials=%s\n' "$grafana_env"
printf 'tempo_retention=72h\nmetrics_retention=14d\n'
