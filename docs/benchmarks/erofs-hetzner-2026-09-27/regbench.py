"""Registry blob upload/read latency for small chunk blobs (monolithic PUT)."""
import concurrent.futures as cf, hashlib, os, statistics, sys, time, urllib.request
R, REPO = "http://127.0.0.1:5000", "environments-bench"
def upload(size):
    data = os.urandom(size); d = "sha256:" + hashlib.sha256(data).hexdigest()
    t = time.monotonic()
    loc = urllib.request.urlopen(urllib.request.Request(f"{R}/v2/{REPO}/blobs/uploads/", method="POST", data=b"")).headers["Location"]
    loc = loc if loc.startswith("http") else R + loc
    sep = "&" if "?" in loc else "?"
    urllib.request.urlopen(urllib.request.Request(f"{loc}{sep}digest={d}", method="PUT", data=data,
        headers={"Content-Type": "application/octet-stream"}), timeout=120).read()
    return time.monotonic() - t, d
def read(d):
    t = time.monotonic(); urllib.request.urlopen(f"{R}/v2/{REPO}/blobs/{d}", timeout=60).read(); return time.monotonic() - t
for size in (262144, 4 * 1024 * 1024):
    seq = [upload(size) for _ in range(8)]
    with cf.ThreadPoolExecutor(32) as pool:
        t = time.monotonic(); par = list(pool.map(lambda _: upload(size), range(64))); wall = time.monotonic() - t
    reads = [read(d) for _, d in par[:16]]
    print(f"{size//1024} KiB: seq upload p50 {statistics.median(x for x,_ in seq):.2f}s; 64 uploads x32 parallel {wall:.1f}s "
          f"({64/wall:.1f}/s, {64*size/wall/1e6:.1f} MB/s); read p50 {statistics.median(reads)*1000:.0f} ms max {max(reads)*1000:.0f} ms")
