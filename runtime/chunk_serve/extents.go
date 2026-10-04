package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// An extent file as ucloud-chunk-store names it under <cache>/<digest[:2]>/:
// <digest>.<kind>.<index>.<object size>.<sha256 of the file>. It appears under
// that name only once complete; nothing is fsynced, so every extent is hashed
// on its first open by this process, and a torn one is never served.
var extentName = regexp.MustCompile(`^([0-9a-f]{64})\.(pack|boot|map|tail|layout)\.(\d{1,6})\.(\d{1,12})\.([0-9a-f]{64})$`)

// rescanAfter bounds how often a miss re-lists one shard directory: new
// extents (fills, the replica mirror) appear between listings.
const rescanAfter = 200 * time.Millisecond

type ident struct {
	digest, kind string
	index        int64
}

type object struct{ digest, kind string }

type extent struct {
	path, sha        string
	size, total      int64
	device, inode    uint64
	mu               sync.Mutex
	verified, broken bool
}

type extents struct {
	root           string
	extentBytes    int64
	mu             sync.RWMutex
	byIdent        map[ident]*extent
	totals         map[object]int64
	scanned        map[string]time.Time
	rejected       map[[2]uint64]bool // (device, inode) of files that failed their hash
	verifyFailures atomic.Int64
	verifications  atomic.Int64
}

func openExtents(root string, extentBytes int64) (*extents, error) {
	x := &extents{root: root, extentBytes: extentBytes, byIdent: map[ident]*extent{},
		totals: map[object]int64{}, scanned: map[string]time.Time{}, rejected: map[[2]uint64]bool{}}
	shards, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, shard := range shards {
		if shard.IsDir() && len(shard.Name()) == 2 {
			x.scan(shard.Name())
		}
	}
	return x, nil
}

func (x *extents) count() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.byIdent)
}

// scan lists one shard directory and records what it finds; files that
// vanished are dropped when an open fails.
func (x *extents) scan(shard string) {
	entries, err := os.ReadDir(filepath.Join(x.root, shard))
	found := map[ident]*extent{}
	if err == nil {
		for _, entry := range entries {
			match := extentName.FindStringSubmatch(entry.Name())
			if match == nil || match[1][:2] != shard || !entry.Type().IsRegular() {
				continue
			}
			info, err := entry.Info()
			if err != nil || info.Size() <= 0 {
				continue
			}
			stat := info.Sys().(*syscall.Stat_t)
			index, _ := strconv.ParseInt(match[3], 10, 64)
			total, _ := strconv.ParseInt(match[4], 10, 64)
			found[ident{match[1], match[2], index}] = &extent{path: filepath.Join(x.root, shard, entry.Name()),
				sha: match[5], size: info.Size(), total: total, device: stat.Dev, inode: stat.Ino}
		}
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	x.scanned[shard] = time.Now()
	for id, fresh := range found {
		if x.rejected[[2]uint64{fresh.device, fresh.inode}] {
			continue
		}
		if known := x.byIdent[id]; known == nil || known.device != fresh.device || known.inode != fresh.inode {
			x.byIdent[id] = fresh
		}
		x.totals[object{id.digest, id.kind}] = fresh.total
	}
}

func (x *extents) lookup(id ident) *extent {
	x.mu.RLock()
	e := x.byIdent[id]
	last := x.scanned[id.digest[:2]]
	x.mu.RUnlock()
	if e != nil || time.Since(last) < rescanAfter {
		return e
	}
	x.scan(id.digest[:2])
	x.mu.RLock()
	defer x.mu.RUnlock()
	return x.byIdent[id]
}

func (x *extents) total(digest, kind string) (int64, bool) {
	key := object{digest, kind}
	x.mu.RLock()
	total, ok := x.totals[key]
	last := x.scanned[digest[:2]]
	x.mu.RUnlock()
	if ok || time.Since(last) < rescanAfter {
		return total, ok
	}
	x.scan(digest[:2])
	x.mu.RLock()
	defer x.mu.RUnlock()
	total, ok = x.totals[key]
	return total, ok
}

func (x *extents) forget(id ident, e *extent, reject bool) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.byIdent[id] == e {
		delete(x.byIdent, id)
	}
	if reject {
		x.rejected[[2]uint64{e.device, e.inode}] = true
	}
}

// open returns a verified extent's file, or nil when it is absent, gone or
// torn (the caller then proxies, and ucloud-chunk-store refetches it).
func (x *extents) open(id ident) (*os.File, *extent) {
	e := x.lookup(id)
	if e == nil {
		return nil, nil
	}
	file, err := os.Open(e.path)
	if err != nil {
		x.forget(id, e, false)
		return nil, nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.broken {
		file.Close()
		return nil, nil
	}
	if !e.verified {
		x.verifications.Add(1)
		digest := sha256.New()
		size, err := io.Copy(digest, io.NewSectionReader(file, 0, 1<<62))
		if err != nil || size != e.size || hex.EncodeToString(digest.Sum(nil)) != e.sha {
			e.broken = true
			x.verifyFailures.Add(1)
			x.forget(id, e, true)
			file.Close()
			return nil, nil
		}
		e.verified = true
	}
	return file, e
}

type piece struct {
	file          *os.File
	offset, count int64
}

func closeAll(pieces []piece) {
	for _, p := range pieces {
		p.file.Close()
	}
}

// read returns the open pieces of one object's [first, last], or false when
// any extent is not here and verified.
func (x *extents) read(digest, kind string, first, last int64) ([]piece, bool) {
	var pieces []piece
	var covered int64
	for index := first / x.extentBytes; index <= last/x.extentBytes; index++ {
		file, e := x.open(ident{digest, kind, index})
		if file == nil {
			closeAll(pieces)
			return nil, false
		}
		low, high := max(first, index*x.extentBytes), min(last+1, index*x.extentBytes+e.size)
		if high <= low {
			file.Close()
			closeAll(pieces)
			return nil, false
		}
		pieces = append(pieces, piece{file, low - index*x.extentBytes, high - low})
		covered += high - low
	}
	if covered != last-first+1 { // A short extent: never send fewer bytes than promised.
		closeAll(pieces)
		return nil, false
	}
	return pieces, true
}

// bytes reads one object's [first, last] into memory (layouts and tail tables).
func (x *extents) bytes(digest, kind string, first, last int64) ([]byte, bool) {
	pieces, ok := x.read(digest, kind, first, last)
	if !ok {
		return nil, false
	}
	defer closeAll(pieces)
	out := make([]byte, 0, last-first+1)
	for _, p := range pieces {
		chunk := make([]byte, p.count)
		if _, err := p.file.ReadAt(chunk, p.offset); err != nil {
			return nil, false
		}
		out = append(out, chunk...)
	}
	return out, true
}
