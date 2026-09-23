#!/usr/bin/env bash
# Build a tiny, owned immutable input, then use the ordinary signed publisher.
set -euo pipefail
: "${UCLOUD_AGENT:?Set the qualified installed ucloud-sandboxes launcher}"
: "${CANARY_IMAGE_REF:?Set a unique owned managed-registry repository:tag}"
: "${ENVIRONMENT_REGISTRY_URL:?Set the managed OCI registry HTTP(S) URL}"
: "${ENVIRONMENT_TRUST_FILE:?Set the provisioned producers.json path}"
: "${ENVIRONMENT_SIGNING_KEY:?Set the private producer.pem path}"
: "${ENVIRONMENT_BUILD_ROOT:?Set an owned builder scratch directory}"
BUSYBOX_STATIC=${BUSYBOX_STATIC:-/bin/busybox}
command -v mkfs.erofs >/dev/null
# busybox-static is only a canary fixture dependency, not a product runtime dependency.
file "$BUSYBOX_STATIC" | grep -q 'statically linked'
canary_context=$(mktemp -d)
trap 'rm -rf -- "$canary_context"' EXIT
mkdir -p "$canary_context/bin" "$canary_context/etc"
cp "$BUSYBOX_STATIC" "$canary_context/bin/busybox"
ln -s busybox "$canary_context/bin/sh"
printf 'immutable-environment-canary\n' > "$canary_context/etc/canary"
cat > "$canary_context/Dockerfile" <<'DOCKERFILE'
FROM scratch
COPY bin /bin
COPY etc /etc
ENV PATH=/bin
ENTRYPOINT ["/bin/busybox", "sh"]
CMD ["-ec", "test \"$(/bin/busybox cat /etc/canary)\" = immutable-environment-canary; echo CANARY_OK"]
DOCKERFILE
docker build -t "$CANARY_IMAGE_REF" "$canary_context"
docker push "$CANARY_IMAGE_REF"
"$UCLOUD_AGENT" publish-environment --image-ref "$CANARY_IMAGE_REF" \
  --state-root "$ENVIRONMENT_BUILD_ROOT" \
  --environment-registry-url "$ENVIRONMENT_REGISTRY_URL" \
  --environment-registry-repository environments \
  --environment-trusted-keys "$ENVIRONMENT_TRUST_FILE" \
  --environment-signing-key "$ENVIRONMENT_SIGNING_KEY" \
  --environment-allow-path bin --environment-allow-path etc
