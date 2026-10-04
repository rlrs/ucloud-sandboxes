package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// maxResponseBytes bounds one request, as ucloud-chunk-store does.
const maxResponseBytes = 256 << 20

var (
	// Only the chunk store's content-addressed objects (chunk_store_node._KEY).
	objectKey = regexp.MustCompile(`^packs/([0-9a-f]{2})/([0-9a-f]{64})\.pack$|^meta/([0-9a-f]{64})\.(boot\.zst|map|tail|layout)$`)
	// nydusd's registry backend: GET/HEAD /v2/<repository>/blobs/sha256:<blob id>.
	virtualBlob = regexp.MustCompile(`^/v2/virtual/([0-9a-f]{64})/blobs/sha256:([0-9a-f]{64})$`)
	byteRange   = regexp.MustCompile(`^bytes=([0-9]{0,15})-([0-9]{0,15})$`)
	metaKinds   = map[string]string{"boot.zst": "boot", "map": "map", "tail": "tail", "layout": "layout"}
)

type server struct {
	extents           *extents
	layouts           *layoutCache
	readToken         []byte
	writeToken        []byte
	upstream          string
	proxy             *httputil.ReverseProxy
	client            *http.Client
	virtual           bool
	served, bytesSent atomic.Int64
	proxied           map[string]*atomic.Int64
	started           time.Time
}

func newServer(x *extents, readToken, writeToken []byte, upstream string, virtual bool) (*server, error) {
	target, err := url.Parse(upstream)
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{MaxIdleConns: 1024, MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport, proxy.FlushInterval = transport, -1
	proxied := map[string]*atomic.Int64{}
	for _, reason := range []string{"miss", "token", "request", "control"} {
		proxied[reason] = &atomic.Int64{}
	}
	return &server{extents: x, layouts: newLayoutCache(), readToken: readToken, writeToken: writeToken,
		upstream: upstream, proxy: proxy, client: &http.Client{Transport: transport, Timeout: 30 * time.Second},
		virtual: virtual, proxied: proxied, started: time.Now()}, nil
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case r.URL.RawPath != "":
		s.pass(w, r, "request") // Escaped paths: ucloud-chunk-store matches the raw path.
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/v1/objects/"):
		s.object(w, r, strings.TrimPrefix(path, "/v1/objects/"))
	case (r.Method == http.MethodGet || r.Method == http.MethodHead) && s.virtual && virtualBlob.MatchString(path):
		s.blob(w, r, virtualBlob.FindStringSubmatch(path)[2])
	case r.Method == http.MethodGet && path == "/v1/metrics":
		s.metrics(w, r)
	default:
		s.pass(w, r, "control")
	}
}

func (s *server) pass(w http.ResponseWriter, r *http.Request, reason string) {
	s.proxied[reason].Add(1)
	s.proxy.ServeHTTP(w, r)
}

// authorized accepts either token, as ucloud-chunk-store does for reads.
func (s *server) authorized(r *http.Request) bool {
	supplied := []byte(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	return subtle.ConstantTimeCompare(supplied, s.readToken) == 1 || subtle.ConstantTimeCompare(supplied, s.writeToken) == 1
}

// parsedRange is one "bytes=" range (chunk_store_node.parse_range); none
// means the whole object.
type parsedRange struct {
	present, suffix bool
	first, last     int64 // last < 0: open-ended
}

func parseRange(header string) (parsedRange, bool) {
	if header == "" {
		return parsedRange{}, true
	}
	match := byteRange.FindStringSubmatch(strings.TrimSpace(header))
	if match == nil || match[1] == "" && match[2] == "" {
		return parsedRange{}, false
	}
	if match[1] == "" {
		suffix, _ := strconv.ParseInt(match[2], 10, 64)
		return parsedRange{present: true, suffix: true, first: suffix}, suffix > 0
	}
	first, _ := strconv.ParseInt(match[1], 10, 64)
	last := int64(-1)
	if match[2] != "" {
		last, _ = strconv.ParseInt(match[2], 10, 64)
	}
	return parsedRange{present: true, first: first, last: last}, true
}

// resolve clamps a range to an object of total bytes; false when it is not
// satisfiable (ucloud-chunk-store then answers 416).
func (p parsedRange) resolve(total int64) (int64, int64, bool) {
	first, last := p.first, p.last
	switch {
	case p.suffix:
		first, last = max(0, total-p.first), total-1
	case !p.present:
		first, last = 0, total-1
	case last < 0:
		last = total - 1
	default:
		last = min(last, total-1)
	}
	return first, last, first < total && last >= first && last-first+1 <= maxResponseBytes
}

func (s *server) object(w http.ResponseWriter, r *http.Request, key string) {
	if !s.authorized(r) {
		s.pass(w, r, "token")
		return
	}
	match := objectKey.FindStringSubmatch(key)
	spec := r.Header.Get("Range")
	requested, ok := parseRange(spec)
	if match == nil || !ok || match[1] != "" && match[1] != match[2][:2] {
		s.pass(w, r, "request")
		return
	}
	digest, kind := match[2], "pack"
	if digest == "" {
		digest, kind = match[3], metaKinds[match[4]]
	}
	total, ok := s.extents.total(digest, kind)
	if !ok {
		s.pass(w, r, "miss")
		return
	}
	first, last, ok := requested.resolve(total)
	if !ok {
		s.pass(w, r, "request")
		return
	}
	pieces, ok := s.extents.read(digest, kind, first, last)
	if !ok {
		s.pass(w, r, "miss")
		return
	}
	s.send(w, r, spec != "", total, first, last-first+1, pieces)
}

func (s *server) blob(w http.ResponseWriter, r *http.Request, blob string) {
	if !s.authorized(r) {
		s.pass(w, r, "token")
		return
	}
	spec := r.Header.Get("Range")
	requested, ok := parseRange(spec)
	if !ok {
		s.pass(w, r, "request")
		return
	}
	l := s.layouts.get(blob)
	if l == nil {
		if l, ok = buildLayout(s.extents, blob); !ok {
			s.pass(w, r, "miss")
			return
		}
		s.layouts.put(blob, l)
	}
	if r.Method == http.MethodHead { // A blob's size, whatever its size.
		s.send(w, r, spec != "", l.size, 0, l.size, nil)
		return
	}
	first, last, ok := requested.resolve(l.size)
	if !ok {
		s.pass(w, r, "request")
		return
	}
	var pieces []piece
	for i := l.find(first); i < len(l.segments) && l.segments[i].offset <= last; i++ {
		seg := l.segments[i]
		low, high := max(first, seg.offset), min(last+1, seg.offset+seg.length)
		part, ok := s.extents.read(seg.digest, seg.kind, seg.objectOffset+low-seg.offset,
			seg.objectOffset+high-seg.offset-1)
		if !ok {
			closeAll(pieces)
			s.pass(w, r, "miss")
			return
		}
		pieces = append(pieces, part...)
	}
	s.send(w, r, spec != "", l.size, first, last-first+1, pieces)
}

// send writes the pieces with sendfile(2): io.Copy from a limited *os.File
// into the response reaches the TCP connection's ReadFrom. A cold page blocks
// only this goroutine's thread, never another request.
func (s *server) send(w http.ResponseWriter, r *http.Request, partial bool, total, first, length int64,
	pieces []piece) {
	defer closeAll(pieces)
	header := w.Header()
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Length", strconv.FormatInt(length, 10))
	status := http.StatusOK
	if partial {
		status = http.StatusPartialContent
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, first+length-1, total))
	}
	w.WriteHeader(status)
	s.served.Add(1)
	if r.Method == http.MethodHead {
		return
	}
	for _, p := range pieces {
		if _, err := p.file.Seek(p.offset, io.SeekStart); err != nil {
			return
		}
		sent, err := io.Copy(w, io.LimitReader(p.file, p.count))
		s.bytesSent.Add(sent)
		if err != nil {
			return
		}
	}
}

// metrics answers ucloud-chunk-store's metrics with this server's beside them.
func (s *server) metrics(w http.ResponseWriter, r *http.Request) {
	request, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, s.upstream+"/v1/metrics", nil)
	request.Header.Set("Authorization", r.Header.Get("Authorization"))
	response, err := s.client.Do(request)
	if err != nil {
		http.Error(w, `{"error": "ucloud-chunk-store is unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 16<<20))
	var document map[string]any
	if err != nil || response.StatusCode != http.StatusOK || json.Unmarshal(body, &document) != nil {
		w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
		w.WriteHeader(response.StatusCode)
		w.Write(body)
		return
	}
	proxied := map[string]int64{}
	for reason, count := range s.proxied {
		proxied[reason] = count.Load()
	}
	document["native"] = map[string]any{
		"served": s.served.Load(), "bytes_sent": s.bytesSent.Load(), "proxied": proxied,
		"extents": s.extents.count(), "verifications": s.extents.verifications.Load(),
		"verify_failures": s.extents.verifyFailures.Load(), "uptime_seconds": int64(time.Since(s.started).Seconds()),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(document)
}
