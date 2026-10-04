package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testExtent = 1 << 20

var (
	testRead  = []byte(strings.Repeat("r", 32))
	testWrite = []byte(strings.Repeat("w", 32))
)

// putObject writes an object's extents as ucloud-chunk-store names them.
func putObject(t *testing.T, root, digest, kind string, data []byte) {
	t.Helper()
	shard := filepath.Join(root, digest[:2])
	if err := os.MkdirAll(shard, 0o700); err != nil {
		t.Fatal(err)
	}
	for index := 0; index*testExtent < len(data); index++ {
		part := data[index*testExtent : min(len(data), (index+1)*testExtent)]
		sum := sha256.Sum256(part)
		name := fmt.Sprintf("%s.%s.%d.%d.%s", digest, kind, index, len(data), hex.EncodeToString(sum[:]))
		if err := os.WriteFile(filepath.Join(shard, name), part, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type fixture struct {
	root, blob string
	blobBytes  []byte
	pack       []byte
	packDigest string
}

// newFixture stores one pack, and a blob whose chunks are in it (with two
// adjacent chunks, so segments merge) plus its tail and layout objects.
func newFixture(t *testing.T) *fixture {
	random := rand.New(rand.NewSource(1))
	pack := make([]byte, 3*testExtent+12345)
	random.Read(pack)
	f := &fixture{root: t.TempDir(), pack: pack, packDigest: digestOf(pack)}
	putObject(t, f.root, f.packDigest, "pack", pack)
	// Chunks: (pack offset, size, zstd); 1 and 2 are adjacent in the pack.
	chunks := [][3]int64{{16, 70000, 1}, {900000, 262144, 0}, {1162144, 200000, 1}, {2500000, 5000, 0}}
	var table, blob bytes.Buffer
	table.Write([]byte("UCTAIL\x00\x01"))
	binary.Write(&table, binary.LittleEndian, uint32(len(chunks)))
	var entries bytes.Buffer
	for _, chunk := range chunks {
		offset := int64(blob.Len())
		blob.Write(pack[chunk[0] : chunk[0]+chunk[1]])
		binary.Write(&table, binary.LittleEndian, uint64(offset))
		binary.Write(&table, binary.LittleEndian, uint32(chunk[1]))
		table.Write(make([]byte, 32))
		table.WriteByte(byte(chunk[2]))
		binary.Write(&entries, binary.LittleEndian, [3]uint32{0, uint32(chunk[0]), uint32(chunk[1])})
		entries.WriteByte(byte(chunk[2]))
	}
	trailer := []byte("nydus toc and chunk digests")
	tail := append(table.Bytes(), trailer...)
	blob.Write(trailer)
	f.blobBytes, f.blob = blob.Bytes(), digestOf([]byte("blob id"))
	document, _ := json.Marshal(map[string]any{"packs": [][2]string{{f.packDigest, "http://store/p"}},
		"meta": map[string]string{}})
	var locator bytes.Buffer
	locator.Write([]byte("UCLOC\x00\x00\x01"))
	binary.Write(&locator, binary.LittleEndian, uint64(7))
	binary.Write(&locator, binary.LittleEndian, [2]uint32{uint32(len(chunks)), uint32(len(document))})
	locator.Write(document)
	locator.Write(entries.Bytes())
	putObject(t, f.root, f.blob, "tail", tail)
	putObject(t, f.root, f.blob, "layout", locator.Bytes())
	return f
}

type upstreamLog struct{ requests atomic.Int64 }

// serve starts the server in front of a stand-in ucloud-chunk-store that
// answers every request "upstream" with status 299.
func serve(t *testing.T, f *fixture) (*httptest.Server, *upstreamLog) {
	log := &upstreamLog{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.requests.Add(1)
		if r.URL.Path == "/v1/metrics" {
			w.Write([]byte(`{"requests": 3}`))
			return
		}
		w.WriteHeader(299)
		w.Write([]byte("upstream"))
	}))
	t.Cleanup(upstream.Close)
	x, err := openExtents(f.root, testExtent)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newServer(x, testRead, testWrite, upstream.URL, true)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	return front, log
}

func get(t *testing.T, method, url, token, spec string) (int, http.Header, []byte) {
	t.Helper()
	request, _ := http.NewRequest(method, url, nil)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if spec != "" {
		request.Header.Set("Range", spec)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, response.Header, body
}

func TestRangesFollowTheStoreNode(t *testing.T) {
	for _, c := range []struct {
		header      string
		ok          bool
		first, last int64
		satisfiable bool
		total       int64
	}{
		{"", true, 0, 99, true, 100},
		{"bytes=10-19", true, 10, 19, true, 100},
		{" bytes=10-19 ", true, 10, 19, true, 100},
		{"bytes=90-200", true, 90, 99, true, 100},
		{"bytes=90-", true, 90, 99, true, 100},
		{"bytes=-30", true, 70, 99, true, 100},
		{"bytes=-300", true, 0, 99, true, 100},
		{"bytes=100-", true, 100, 99, false, 100},
		{"bytes=20-10", true, 20, 10, false, 100},
		{"bytes=-0", false, 0, 0, false, 100},
		{"bytes=-", false, 0, 0, false, 100},
		{"bytes=1-2,4-5", false, 0, 0, false, 100},
		{"items=1-2", false, 0, 0, false, 100},
		{"bytes=0-", true, 0, maxResponseBytes, false, maxResponseBytes + 1},
	} {
		parsed, ok := parseRange(c.header)
		if ok != c.ok {
			t.Fatalf("%q parsed %v", c.header, ok)
		}
		if !ok {
			continue
		}
		first, last, satisfiable := parsed.resolve(c.total)
		if satisfiable != c.satisfiable || satisfiable && (first != c.first || last != c.last) {
			t.Fatalf("%q: %d-%d %v", c.header, first, last, satisfiable)
		}
	}
}

func TestBlobsAndObjectsAreServedFromResidentExtents(t *testing.T) {
	f := newFixture(t)
	front, upstream := serve(t, f)
	blobURL := front.URL + "/v2/virtual/" + strings.Repeat("c", 64) + "/blobs/sha256:" + f.blob
	status, header, body := get(t, http.MethodHead, blobURL, string(testRead), "")
	if status != 200 || header.Get("Content-Length") != fmt.Sprint(len(f.blobBytes)) || len(body) != 0 {
		t.Fatalf("HEAD: %d %v", status, header)
	}
	status, _, body = get(t, http.MethodGet, blobURL, string(testWrite), "")
	if status != 200 || !bytes.Equal(body, f.blobBytes) {
		t.Fatalf("whole blob: %d, %d bytes", status, len(body))
	}
	random := rand.New(rand.NewSource(2))
	for i := 0; i < 200; i++ {
		first := random.Int63n(int64(len(f.blobBytes)))
		last := first + random.Int63n(int64(len(f.blobBytes))-first)
		status, header, body = get(t, http.MethodGet, blobURL, string(testRead), fmt.Sprintf("bytes=%d-%d", first, last))
		want := fmt.Sprintf("bytes %d-%d/%d", first, last, len(f.blobBytes))
		if status != 206 || header.Get("Content-Range") != want || !bytes.Equal(body, f.blobBytes[first:last+1]) {
			t.Fatalf("blob %d-%d: %d %q", first, last, status, header.Get("Content-Range"))
		}
	}
	packURL := front.URL + "/v1/objects/packs/" + f.packDigest[:2] + "/" + f.packDigest + ".pack"
	status, _, body = get(t, http.MethodGet, packURL, string(testRead), "bytes=-5000")
	if status != 206 || !bytes.Equal(body, f.pack[len(f.pack)-5000:]) {
		t.Fatalf("pack suffix: %d", status)
	}
	l, _ := buildLayout(mustExtents(t, f.root), f.blob)
	if len(l.segments) != 4 { // Chunks 1 and 2 merged, then the tail.
		t.Fatalf("segments %d", len(l.segments))
	}
	if upstream.requests.Load() != 0 {
		t.Fatalf("resident reads went upstream %d times", upstream.requests.Load())
	}
}

func mustExtents(t *testing.T, root string) *extents {
	x, err := openExtents(root, testExtent)
	if err != nil {
		t.Fatal(err)
	}
	return x
}

func TestEverythingElseGoesToTheStoreNode(t *testing.T) {
	f := newFixture(t)
	front, upstream := serve(t, f)
	blobURL := front.URL + "/v2/virtual/" + strings.Repeat("c", 64) + "/blobs/sha256:" + f.blob
	missing := strings.Repeat("e", 64)
	for _, c := range []struct{ method, url, token, spec string }{
		{http.MethodGet, blobURL, "x" + string(testRead[1:]), ""},                               // bad token
		{http.MethodGet, blobURL, "", ""},                                                       // no token
		{http.MethodGet, blobURL, string(testRead), "bytes=1-2,4-5"},                            // malformed range
		{http.MethodGet, blobURL, string(testRead), fmt.Sprintf("bytes=%d-", len(f.blobBytes))}, // 416
		{http.MethodGet, front.URL + "/v2/virtual/" + missing + "/blobs/sha256:" + missing, string(testRead), ""},
		{http.MethodGet, front.URL + "/v1/objects/packs/" + missing[:2] + "/" + missing + ".pack", string(testRead), ""},
		{http.MethodGet, front.URL + "/v1/objects/packs/00/" + f.packDigest + ".pack", string(testRead), ""},
		{http.MethodGet, front.URL + "/v1/objects/meta/secret.env", string(testRead), ""},
		{http.MethodGet, front.URL + "/v1/objects/packs%2F" + f.packDigest, string(testRead), ""},
		{http.MethodPost, front.URL + "/v1/warm", string(testWrite), ""},
		{http.MethodGet, front.URL + "/healthz", "", ""},
	} {
		before := upstream.requests.Load()
		status, _, body := get(t, c.method, c.url, c.token, c.spec)
		if status != 299 || string(body) != "upstream" || upstream.requests.Load() != before+1 {
			t.Fatalf("%s %s %q: %d %q", c.method, c.url, c.spec, status, body)
		}
	}
	status, _, body := get(t, http.MethodGet, front.URL+"/v1/metrics", string(testRead), "")
	var metrics map[string]any
	if status != 200 || json.Unmarshal(body, &metrics) != nil || metrics["requests"] != 3.0 || metrics["native"] == nil {
		t.Fatalf("metrics: %d %s", status, body)
	}
}

func TestATornExtentIsNeverServed(t *testing.T) {
	f := newFixture(t)
	matches, _ := filepath.Glob(filepath.Join(f.root, f.packDigest[:2], f.packDigest+".pack.1.*"))
	data, _ := os.ReadFile(matches[0])
	data[100] ^= 0xff
	os.WriteFile(matches[0], data, 0o600)
	front, upstream := serve(t, f)
	packURL := front.URL + "/v1/objects/packs/" + f.packDigest[:2] + "/" + f.packDigest + ".pack"
	for i := 0; i < 2; i++ { // Rejected once, then never offered again.
		if status, _, _ := get(t, http.MethodGet, packURL, string(testRead), "bytes=1048576-1048700"); status != 299 {
			t.Fatalf("torn extent served: %d", status)
		}
	}
	if status, _, body := get(t, http.MethodGet, packURL, string(testRead), "bytes=0-99"); status != 206 ||
		!bytes.Equal(body, f.pack[:100]) {
		t.Fatalf("an intact extent of the same pack: %d", status)
	}
	if upstream.requests.Load() != 2 {
		t.Fatalf("upstream %d", upstream.requests.Load())
	}
}

func TestANewExtentIsFoundOnTheNextMiss(t *testing.T) {
	f := newFixture(t)
	front, _ := serve(t, f)
	data := []byte("arrived after start")
	digest := digestOf(data)
	url := front.URL + "/v1/objects/meta/" + digest + ".map"
	if status, _, _ := get(t, http.MethodGet, url, string(testRead), ""); status != 299 {
		t.Fatalf("not yet resident: %d", status)
	}
	putObject(t, f.root, digest, "map", data)
	for i := 0; ; i++ { // Within rescanAfter.
		status, _, body := get(t, http.MethodGet, url, string(testRead), "")
		if status == 200 && bytes.Equal(body, data) {
			break
		}
		if i > 100 {
			t.Fatal("a new extent was never found")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConcurrentFirstReadsHashEachExtentOnce(t *testing.T) {
	f := newFixture(t)
	x := mustExtents(t, f.root)
	s, _ := newServer(x, testRead, testWrite, "http://127.0.0.1:1", true)
	front := httptest.NewServer(s)
	t.Cleanup(front.Close)
	blobURL := front.URL + "/v2/virtual/" + strings.Repeat("c", 64) + "/blobs/sha256:" + f.blob
	done := make(chan error, 64)
	for g := 0; g < 64; g++ {
		go func(seed int64) {
			random := rand.New(rand.NewSource(seed))
			for i := 0; i < 20; i++ {
				first := random.Int63n(int64(len(f.blobBytes)))
				request, _ := http.NewRequest(http.MethodGet, blobURL, nil)
				request.Header.Set("Authorization", "Bearer "+string(testRead))
				request.Header.Set("Range", fmt.Sprintf("bytes=%d-", first))
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					done <- err
					return
				}
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if !bytes.Equal(body, f.blobBytes[first:]) {
					done <- fmt.Errorf("bytes %d-: %d", first, response.StatusCode)
					return
				}
			}
			done <- nil
		}(int64(g))
	}
	for g := 0; g < 64; g++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	// The blob reads pack extents 0-2, the tail and the layout: each hashed once.
	if x.verifications.Load() != 5 {
		t.Fatalf("%d hashes", x.verifications.Load())
	}
}
