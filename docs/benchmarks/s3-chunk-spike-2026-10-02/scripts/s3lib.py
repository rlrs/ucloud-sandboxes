"""Stdlib-only S3 client for the S12 spike (SigV4 header auth and presigned query auth).

Credentials come from a root-only env file (HETZNER_S3_ACCESS_KEY / HETZNER_S3_SECRET_KEY);
they are never printed. Every write is confined to the spike prefix by `S3._check_key`.
"""
import datetime
import hashlib
import hmac
import http.client
import os
import ssl
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET

EMPTY_SHA = hashlib.sha256(b"").hexdigest()
SPIKE_PREFIX = "spike-s12/"


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def signing_key(secret, date, region, service):
    k = _sign(("AWS4" + secret).encode(), date)
    k = _sign(k, region)
    k = _sign(k, service)
    return _sign(k, "aws4_request")


def _q(s, safe="-_.~"):
    return urllib.parse.quote(s, safe=safe)


def canonical_query(params):
    return "&".join(f"{_q(k)}={_q(str(v))}" for k, v in sorted(params.items()))


def sigv4(method, host, path, params, headers, payload_hash, ak, sk, region, service, now):
    """Returns (authorization header value, amz date). `headers` must include host."""
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    date = amz[:8]
    hs = {k.lower(): " ".join(str(v).strip().split()) for k, v in headers.items()}
    signed = ";".join(sorted(hs))
    creq = "\n".join([method, path, canonical_query(params),
                      "".join(f"{k}:{hs[k]}\n" for k in sorted(hs)), signed, payload_hash])
    scope = f"{date}/{region}/{service}/aws4_request"
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
    sig = hmac.new(signing_key(sk, date, region, service), sts.encode(), hashlib.sha256).hexdigest()
    return f"AWS4-HMAC-SHA256 Credential={ak}/{scope}, SignedHeaders={signed}, Signature={sig}", amz


def presign_query(method, host, path, ak, sk, region, service, now, expires):
    amz = now.strftime("%Y%m%dT%H%M%SZ")
    date = amz[:8]
    scope = f"{date}/{region}/{service}/aws4_request"
    params = {"X-Amz-Algorithm": "AWS4-HMAC-SHA256", "X-Amz-Credential": f"{ak}/{scope}",
              "X-Amz-Date": amz, "X-Amz-Expires": str(expires), "X-Amz-SignedHeaders": "host"}
    cq = canonical_query(params)
    creq = "\n".join([method, path, cq, f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"])
    sts = "\n".join(["AWS4-HMAC-SHA256", amz, scope, hashlib.sha256(creq.encode()).hexdigest()])
    sig = hmac.new(signing_key(sk, date, region, service), sts.encode(), hashlib.sha256).hexdigest()
    return f"{path}?{cq}&X-Amz-Signature={sig}"


def load_env(path):
    out = {}
    for line in open(path):
        k, _, v = line.strip().partition("=")
        if k:
            out[k.strip()] = v.strip().strip("'\"")
    return out


class S3:
    def __init__(self, endpoint, bucket, region, env_file, virtual_host=True, writable_prefix=SPIKE_PREFIX):
        env = load_env(env_file)
        self._ak, self._sk = env["HETZNER_S3_ACCESS_KEY"], env["HETZNER_S3_SECRET_KEY"]
        u = urllib.parse.urlparse(endpoint)
        self.bucket, self.region, self.prefix = bucket, region, writable_prefix
        self.virtual_host = virtual_host
        self.host = f"{bucket}.{u.netloc}" if virtual_host else u.netloc
        self._tls = threading.local()
        self._ctx = ssl.create_default_context()

    # ------------------------------------------------------------- plumbing
    def path(self, key):
        p = "/" + _q(key, safe="-_.~/")
        return p if self.virtual_host else f"/{self.bucket}{p}"

    def conn(self, fresh=False):
        c = getattr(self._tls, "c", None)
        if c is None or fresh:
            if c is not None:
                c.close()
            c = self._tls.c = http.client.HTTPSConnection(self.host, 443, timeout=60, context=self._ctx)
        return c

    def _check_key(self, method, key):
        if method in ("PUT", "DELETE", "POST") and key and not key.startswith(self.prefix):
            raise PermissionError(f"refusing {method} outside {self.prefix}: {key}")

    def request(self, method, key, params=None, body=b"", headers=None, retries=3):
        self._check_key(method, key)
        params = params or {}
        path = self.path(key) if key else ("/" if self.virtual_host else f"/{self.bucket}")
        ph = hashlib.sha256(body).hexdigest() if body else EMPTY_SHA
        for attempt in range(retries + 1):
            h = {"host": self.host, "x-amz-content-sha256": ph, **(headers or {})}
            now = datetime.datetime.now(datetime.timezone.utc)
            h["x-amz-date"] = now.strftime("%Y%m%dT%H%M%SZ")
            auth, _ = sigv4(method, self.host, path, params, h, ph, self._ak, self._sk, self.region, "s3", now)
            h["Authorization"] = auth
            url = path + ("?" + canonical_query(params) if params else "")
            try:
                c = self.conn()
                c.request(method, url, body=body or None, headers=h)
                r = c.getresponse()
                data = r.read()
            except (OSError, http.client.HTTPException):
                self.conn(fresh=True)
                if attempt == retries:
                    raise
                time.sleep(0.2 * 2 ** attempt)
                continue
            if r.status in (500, 502, 503, 504) and attempt < retries:
                time.sleep(0.2 * 2 ** attempt)
                continue
            return r.status, dict(r.getheaders()), data
        raise RuntimeError("unreachable")

    # ------------------------------------------------------------- operations
    def put(self, key, data):
        st, _, body = self.request("PUT", key, body=data, headers={"content-length": str(len(data))})
        if st != 200:
            raise RuntimeError(f"PUT {key}: {st} {body[:300]!r}")

    def head(self, key):
        st, h, _ = self.request("HEAD", key)
        return h if st == 200 else None

    def get(self, key, rng=None):
        st, h, body = self.request("GET", key, headers={"range": rng} if rng else None)
        if st not in (200, 206):
            raise RuntimeError(f"GET {key}: {st} {body[:300]!r}")
        return body

    def list(self, prefix):
        keys, token = [], None
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
            if token:
                params["continuation-token"] = token
            st, _, body = self.request("GET", "", params=params)
            if st != 200:
                raise RuntimeError(f"LIST {prefix}: {st} {body[:300]!r}")
            root = ET.fromstring(body)
            ns = root.tag.split("}")[0] + "}" if root.tag.startswith("{") else ""
            for c in root.findall(f"{ns}Contents"):
                keys.append((c.find(f"{ns}Key").text, int(c.find(f"{ns}Size").text)))
            if root.findtext(f"{ns}IsTruncated") == "true":
                token = root.findtext(f"{ns}NextContinuationToken")
            else:
                return keys

    def delete_many(self, keys):
        """Multi-object delete, 1000 per request. Returns the number reported deleted."""
        import base64
        deleted = 0
        for i in range(0, len(keys), 1000):
            batch = keys[i:i + 1000]
            for k in batch:
                self._check_key("DELETE", k)
            xml = "<Delete><Quiet>false</Quiet>" + "".join(
                f"<Object><Key>{k.replace('&', '&amp;').replace('<', '&lt;')}</Key></Object>" for k in batch) + "</Delete>"
            body = xml.encode()
            md5 = base64.b64encode(hashlib.md5(body).digest()).decode()
            st, _, resp = self._post_delete(body, md5)
            if st != 200:
                raise RuntimeError(f"DELETE batch: {st} {resp[:300]!r}")
            deleted += resp.count(b"<Deleted>")
        return deleted

    def _post_delete(self, body, md5):
        # Bucket-level POST ?delete; keys were checked against the prefix by the caller.
        params = {"delete": ""}
        path = "/" if self.virtual_host else f"/{self.bucket}"
        ph = hashlib.sha256(body).hexdigest()
        h = {"host": self.host, "x-amz-content-sha256": ph, "content-md5": md5, "content-type": "application/xml"}
        now = datetime.datetime.now(datetime.timezone.utc)
        h["x-amz-date"] = now.strftime("%Y%m%dT%H%M%SZ")
        h["Authorization"], _ = sigv4("POST", self.host, path, params, h, ph, self._ak, self._sk, self.region, "s3", now)
        c = self.conn()
        c.request("POST", path + "?delete=", body=body, headers=h)
        r = c.getresponse()
        return r.status, None, r.read()

    def presign(self, key, expires=86400, method="GET"):
        now = datetime.datetime.now(datetime.timezone.utc)
        return presign_query(method, self.host, self.path(key), self._ak, self._sk, self.region, "s3", now, expires)


class PresignedReader:
    """What a worker does: ranged GETs on presigned URLs over a per-thread keep-alive connection.
    Holds no key. Returns (status, body, ttfb_s, total_s)."""
    def __init__(self, host):
        self.host = host
        self._tls = threading.local()
        self._ctx = ssl.create_default_context()
        self.connects = 0

    def _conn(self, fresh=False):
        c = getattr(self._tls, "c", None)
        if c is None or fresh:
            if c is not None:
                c.close()
            c = self._tls.c = http.client.HTTPSConnection(self.host, 443, timeout=30, context=self._ctx)
            self.connects += 1
        return c

    def get(self, url, start, length):
        for attempt in range(2):
            c = self._conn(fresh=attempt > 0)
            t0 = time.perf_counter()
            try:
                c.request("GET", url, headers={"Range": f"bytes={start}-{start + length - 1}"})
                r = c.getresponse()
                t1 = time.perf_counter()
                body = r.read()
                t2 = time.perf_counter()
                return r.status, body, t1 - t0, t2 - t0
            except (OSError, http.client.HTTPException):
                if attempt:
                    raise
        raise RuntimeError("unreachable")
