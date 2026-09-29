#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: configure_hetzner_sdk_ingress.sh --public-host <dns-name-or-ip> [options]

Options:
  --gateway-port <port>  Loopback gateway port (default: 8090)
  --relay-port <port>    Loopback model relay port, published under /relay/
                         (default: 8092)
  --email <address>      ACME account email (recommended)
  --staging              Use Let's Encrypt staging for a non-trusted test cert
EOF
}

public_host=""
gateway_port=8090
relay_port=8092
email=""
staging=false
while (($#)); do
  case "$1" in
    --public-host)
      if (($# < 2)); then
        usage
        exit 2
      fi
      public_host="${2:-}"
      shift 2
      ;;
    --gateway-port)
      if (($# < 2)); then
        usage
        exit 2
      fi
      gateway_port="${2:-}"
      shift 2
      ;;
    --relay-port)
      if (($# < 2)); then
        usage
        exit 2
      fi
      relay_port="${2:-}"
      shift 2
      ;;
    --email)
      if (($# < 2)); then
        usage
        exit 2
      fi
      email="${2:-}"
      shift 2
      ;;
    --staging)
      staging=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

if [[ "$EUID" -ne 0 ]]; then
  echo "SDK ingress configuration must run as root" >&2
  exit 2
fi
if [[ ! "$gateway_port" =~ ^[0-9]+$ ]] \
  || ((gateway_port < 1 || gateway_port > 65535)); then
  echo "gateway port must be between 1 and 65535" >&2
  exit 2
fi
if [[ ! "$relay_port" =~ ^[0-9]+$ ]] \
  || ((relay_port < 1 || relay_port > 65535)); then
  echo "relay port must be between 1 and 65535" >&2
  exit 2
fi
if [[ -z "$public_host" || "$public_host" == *:* || "$public_host" == */* ]]; then
  echo "public host must be one DNS name or IPv4 address without a scheme or port" >&2
  exit 2
fi

host_kind="$({
  python3 - "$public_host" <<'PY'
import ipaddress
import re
import sys

value = sys.argv[1]
try:
    address = ipaddress.ip_address(value)
except ValueError:
    address = None
if address is not None:
    if address.version != 4 or not address.is_global:
        raise SystemExit("the public IP must be a globally routable IPv4 address")
    print("ip")
elif (
    len(value) <= 253
    and "." in value
    and all(
        re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
        for label in value.rstrip(".").split(".")
    )
):
    print("dns")
else:
    raise SystemExit("public host is not a valid DNS name or IPv4 address")
PY
} 2>&1)" || {
  echo "$host_kind" >&2
  exit 2
}
if [[ "$host_kind" == dns ]]; then
  public_host="${public_host%.}"
fi

export DEBIAN_FRONTEND=noninteractive
apt-get -o DPkg::Lock::Timeout=600 update
apt-get -o DPkg::Lock::Timeout=600 install -y --no-install-recommends \
  ca-certificates nginx python3-venv

certbot_root=/opt/ucloud-sandboxes-certbot
if [[ ! -x "$certbot_root/bin/python" ]]; then
  python3 -m venv "$certbot_root"
fi
"$certbot_root/bin/pip" install \
  --disable-pip-version-check \
  'certbot>=5.4,<6'

acme_webroot=/var/lib/ucloud-sandboxes-acme
install -d -m 0755 "$acme_webroot/.well-known/acme-challenge"
rm -f /etc/nginx/sites-enabled/default

nginx_site=/etc/nginx/sites-available/ucloud-sandbox-gateway
nginx_enabled=/etc/nginx/sites-enabled/ucloud-sandbox-gateway
nginx_main=/etc/nginx/nginx.conf
temporary_site="$(mktemp)"
temporary_main="$(mktemp /etc/nginx/nginx.conf.ucloud.XXXXXX)"
previous_site="$(mktemp)"
trap 'rm -f "$temporary_site" "$temporary_main" "$previous_site"' EXIT

install_nginx_site() {
  # These limits apply per worker. Shrinking worker_processes=auto must not
  # shrink ingress below the sockets needed by 500 agents and their proxies.
  # Preserve the rest of the administrator's nginx configuration verbatim.
  python3 - "$nginx_main" "$temporary_main" <<'PY_NGINX_CAPACITY'
from pathlib import Path
import re
import sys

source = Path(sys.argv[1]).read_text()
tokens = list(re.finditer(r'''"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|\#[^\n]*|[{};]|[^\s{};\#]+''', source))
contexts, statement, edits = [], [], []
found = set()
events_open = None
for token in tokens:
    word = token.group()
    if word.startswith("#"):
        continue
    if word == "{":
        name = statement[0].group() if statement else ""
        if not contexts and name == "events":
            if events_open is not None:
                raise SystemExit("nginx configuration has multiple events blocks")
            events_open = token.end()
        contexts.append(name)
        statement = []
    elif word == "}":
        if not contexts:
            raise SystemExit("nginx configuration has an unmatched closing brace")
        contexts.pop()
        statement = []
    elif word == ";":
        name = statement[0].group() if statement else ""
        target = ((not contexts and name == "worker_rlimit_nofile")
                  or (contexts == ["events"] and name == "worker_connections"))
        if target:
            if name in found or len(statement) != 2:
                raise SystemExit("nginx capacity directive is ambiguous: " + name)
            found.add(name)
            if not statement[1].group().isdigit():
                raise SystemExit("nginx capacity directive must be numeric: " + name)
            minimum = 65536 if name == "worker_rlimit_nofile" else 4096
            value = str(max(minimum, int(statement[1].group())))
            edits.append((statement[1].start(), statement[1].end(), value))
        statement = []
    else:
        statement.append(token)
if contexts or events_open is None:
    raise SystemExit("nginx configuration must contain one complete events block")
if "worker_connections" not in found:
    edits.append((events_open, events_open, "\n    worker_connections 4096;"))
if "worker_rlimit_nofile" not in found:
    edits.append((len(source), len(source), "\nworker_rlimit_nofile 65536;\n"))
for start, end, replacement in sorted(edits, reverse=True):
    source = source[:start] + replacement + source[end:]
Path(sys.argv[2]).write_text(source)
PY_NGINX_CAPACITY

  local site_existed=false
  if [[ -f "$nginx_site" ]]; then
    cp -p "$nginx_site" "$previous_site"
    site_existed=true
    if ! cmp -s "$nginx_site" "$temporary_site"; then
      local site_backup
      site_backup="$(mktemp "${nginx_site}.ucloud-backup.XXXXXX")"
      cp -p "$nginx_site" "$site_backup"
    fi
  fi
  install -m 0644 "$temporary_site" "$nginx_site"
  ln -sfn "$nginx_site" "$nginx_enabled"
  # The running nginx keeps its old configuration throughout validation.
  # Validate the new site and limits together before replacing nginx.conf.
  if ! nginx -t -c "$temporary_main"; then
    if [[ "$site_existed" == true ]]; then
      cp -p "$previous_site" "$nginx_site"
    else
      rm -f "$nginx_site" "$nginx_enabled"
    fi
    return 1
  fi
  if ! cmp -s "$nginx_main" "$temporary_main"; then
    local main_backup
    main_backup="$(mktemp "${nginx_main}.ucloud-backup.XXXXXX")"
    cp -p "$nginx_main" "$main_backup"
    install -m 0644 "$temporary_main" "$nginx_main"
  fi
}

cat >"$temporary_site" <<EOF
server {
    listen 80;
    listen [::]:80;
    server_name $public_host;
    server_tokens off;

    location ^~ /.well-known/acme-challenge/ {
        root $acme_webroot;
        default_type text/plain;
    }

    location / {
        return 308 https://\$host\$request_uri;
    }
}
EOF
install_nginx_site
systemctl enable --now nginx.service
systemctl reload nginx.service

host_digest="$(printf '%s' "$host_kind|$public_host|$staging" | sha256sum | cut -c1-16)"
cert_name="ucloud-sandbox-gateway-$host_digest"
certificate_dir="/etc/letsencrypt/live/$cert_name"
if [[ ! -s "$certificate_dir/fullchain.pem" || ! -s "$certificate_dir/privkey.pem" ]]; then
  certbot_args=(
    certonly
    --non-interactive
    --agree-tos
    --cert-name "$cert_name"
    --webroot
    --webroot-path "$acme_webroot"
  )
  if [[ -n "$email" ]]; then
    certbot_args+=(--email "$email")
  else
    certbot_args+=(--register-unsafely-without-email)
  fi
  if [[ "$staging" == true ]]; then
    certbot_args+=(--staging)
  fi
  if [[ "$host_kind" == ip ]]; then
    certbot_args+=(--preferred-profile shortlived --ip-address "$public_host")
  else
    certbot_args+=(-d "$public_host")
  fi
  "$certbot_root/bin/certbot" "${certbot_args[@]}"
fi

cat >"$temporary_site" <<EOF
# The aiohttp relay releases request admission between keep-alive requests.
# Cache only a small idle pool per nginx worker, below its five-second timeout.
# The threaded gateway deliberately closes responses and is not pooled here.
upstream ucloud_model_relay {
    server 127.0.0.1:$relay_port;
    keepalive 8;
    keepalive_requests 100;
    keepalive_timeout 1s;
}

# The model relay is published under /relay/ for inference workers outside
# the private network. The raw request URI is forwarded without the prefix:
# nginx must not decode percent-encoded rollout ids in tunnel paths.
map \$request_uri \$ucloud_relay_uri {
    ~^/relay(?<rest>/.*)\$ \$rest;
    default /;
}

server {
    listen 80;
    listen [::]:80;
    server_name $public_host;
    server_tokens off;

    location ^~ /.well-known/acme-challenge/ {
        root $acme_webroot;
        default_type text/plain;
    }

    location / {
        return 308 https://\$host\$request_uri;
    }
}

server {
    listen 443 ssl;
    listen [::]:443 ssl;
    server_name $public_host;
    server_tokens off;

    ssl_certificate $certificate_dir/fullchain.pem;
    ssl_certificate_key $certificate_dir/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_session_timeout 1d;
    ssl_session_cache shared:UCloudSandboxTLS:10m;
    ssl_session_tickets off;

    client_max_body_size 256m;
    proxy_request_buffering off;
    proxy_buffering off;
    proxy_connect_timeout 10s;
    proxy_send_timeout 3600s;
    proxy_read_timeout 3600s;

    location /relay/ {
        proxy_pass http://ucloud_model_relay\$ucloud_relay_uri;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header Proxy-Authorization "";
    }

    location / {
        proxy_pass http://127.0.0.1:$gateway_port;
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header Proxy-Authorization "";
    }
}
EOF
install_nginx_site

install -d -m 0755 /etc/systemd/system
cat >/etc/systemd/system/ucloud-sandbox-certbot-renew.service <<EOF
[Unit]
Description=Renew UCloud sandbox gateway TLS certificate
After=network-online.target nginx.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=$certbot_root/bin/certbot renew --quiet --deploy-hook "/usr/bin/systemctl reload nginx.service"
EOF
cat >/etc/systemd/system/ucloud-sandbox-certbot-renew.timer <<'EOF'
[Unit]
Description=Renew UCloud sandbox gateway TLS certificate twice daily

[Timer]
OnCalendar=*-*-* 00,12:00:00
RandomizedDelaySec=1h
Persistent=true

[Install]
WantedBy=timers.target
EOF

nginx -t
systemctl daemon-reload
systemctl enable --now nginx.service ucloud-sandbox-certbot-renew.timer
systemctl reload nginx.service

curl --fail --silent --show-error "https://$public_host/healthz" >/dev/null
curl --fail --silent --show-error "https://$public_host/relay/healthz" >/dev/null
printf 'sdk_url=https://%s\n' "$public_host"
printf 'relay_url=https://%s/relay\n' "$public_host"
printf 'tls_certificate=%s\n' "$certificate_dir/fullchain.pem"
printf 'gateway_upstream=http://127.0.0.1:%s\n' "$gateway_port"
