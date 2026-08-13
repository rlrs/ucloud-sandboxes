#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'EOF'
usage: configure_hetzner_sdk_ingress.sh --public-host <dns-name-or-ip> [options]

Options:
  --gateway-port <port>  Loopback gateway port (default: 8090)
  --email <address>      ACME account email (recommended)
  --staging              Use Let's Encrypt staging for a non-trusted test cert
EOF
}

public_host=""
gateway_port=8090
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
apt-get update
apt-get install -y --no-install-recommends \
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
temporary_site="$(mktemp)"
trap 'rm -f "$temporary_site"' EXIT
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
install -m 0644 "$temporary_site" "$nginx_site"
ln -sfn "$nginx_site" /etc/nginx/sites-enabled/ucloud-sandbox-gateway
nginx -t
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
install -m 0644 "$temporary_site" "$nginx_site"

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
printf 'sdk_url=https://%s\n' "$public_host"
printf 'tls_certificate=%s\n' "$certificate_dir/fullchain.pem"
printf 'gateway_upstream=http://127.0.0.1:%s\n' "$gateway_port"
