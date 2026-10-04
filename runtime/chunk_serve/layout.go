package main

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"sort"
	"sync"
)

// The formats of ucloud_sandboxes/chunk_store.py: a blob's tail object (its
// chunk table, then the bytes after its last chunk) and its layout (a locator
// over the same chunks, built at registration).
const (
	tailHeaderSize    = 12 // <8sI: magic, chunk count
	tailEntrySize     = 45 // <QI32s?: blob offset, size, chunk id, zstd
	locatorHeaderSize = 24 // <8sQII: magic, epoch, entries, JSON size
	locatorEntrySize  = 13 // <IIIB: pack, offset, clen, flags
	maxMapEntries     = 1 << 20
	maxLocatorJSON    = 16 << 20
	packHeaderSize    = 16
	maxClen           = 256<<10 + 4096
	maxPackBytes      = 64 << 20
	layoutsCached     = 4096
)

var (
	tailMagic    = []byte("UCTAIL\x00\x01")
	locatorMagic = []byte("UCLOC\x00\x00\x01")
)

// A segment maps blob bytes [offset, offset+length) to one object's bytes
// from objectOffset; segments cover a blob exactly, in order.
type segment struct {
	offset, length, objectOffset int64
	digest, kind                 string
}

type layout struct {
	size     int64
	segments []segment
}

// find returns the index of the first segment that ends after offset.
func (l *layout) find(offset int64) int {
	return sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].offset+l.segments[i].length > offset
	})
}

type layoutCache struct {
	mu    sync.Mutex
	order *list.List
	byID  map[string]*list.Element
}

type cachedLayout struct {
	blob   string
	layout *layout
}

func newLayoutCache() *layoutCache {
	return &layoutCache{order: list.New(), byID: map[string]*list.Element{}}
}

func (c *layoutCache) get(blob string) *layout {
	c.mu.Lock()
	defer c.mu.Unlock()
	if element := c.byID[blob]; element != nil {
		c.order.MoveToFront(element)
		return element.Value.(*cachedLayout).layout
	}
	return nil
}

func (c *layoutCache) put(blob string, l *layout) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byID[blob] != nil {
		return
	}
	c.byID[blob] = c.order.PushFront(&cachedLayout{blob, l})
	for c.order.Len() > layoutsCached {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.byID, oldest.Value.(*cachedLayout).blob)
	}
}

// buildLayout rebuilds a blob's layout as VirtualBlobs.layout does, from the
// resident tail and layout objects; false when either is not here or the two
// do not agree (ucloud-chunk-store then answers, from the index if need be).
func buildLayout(x *extents, blob string) (*layout, bool) {
	tailTotal, ok := x.total(blob, "tail")
	if !ok || tailTotal < tailHeaderSize {
		return nil, false
	}
	header, ok := x.bytes(blob, "tail", 0, tailHeaderSize-1)
	if !ok || !bytes.Equal(header[:8], tailMagic) {
		return nil, false
	}
	count := int64(binary.LittleEndian.Uint32(header[8:]))
	table := tailHeaderSize + count*tailEntrySize
	if count == 0 || count > maxMapEntries || table > tailTotal {
		return nil, false
	}
	entries, ok := x.bytes(blob, "tail", tailHeaderSize, table-1)
	if !ok {
		return nil, false
	}
	layoutTotal, ok := x.total(blob, "layout")
	if !ok || layoutTotal < locatorHeaderSize {
		return nil, false
	}
	encoded, ok := x.bytes(blob, "layout", 0, layoutTotal-1)
	if !ok {
		return nil, false
	}
	packs, located, ok := decodeLocator(encoded)
	if !ok || int64(len(located)) != count {
		return nil, false
	}
	var segments []segment
	var end int64
	for i := int64(0); i < count; i++ {
		entry := entries[i*tailEntrySize : (i+1)*tailEntrySize]
		offset := int64(binary.LittleEndian.Uint64(entry))
		size := int64(binary.LittleEndian.Uint32(entry[8:]))
		compressed := entry[44] != 0
		if offset != end { // The chunks fill the blob from 0.
			return nil, false
		}
		end = offset + size
		loc := located[i]
		flags := uint8(0)
		if compressed {
			flags = 1
		}
		if loc.clen != size || loc.flags != flags || int(loc.pack) >= len(packs) {
			return nil, false // Re-encoded chunks: ucloud-chunk-store says why.
		}
		digest := packs[loc.pack]
		if last := len(segments) - 1; last >= 0 && segments[last].digest == digest &&
			segments[last].objectOffset+segments[last].length == loc.offset {
			segments[last].length += size
		} else {
			segments = append(segments, segment{offset, size, loc.offset, digest, "pack"})
		}
	}
	segments = append(segments, segment{end, tailTotal - table, table, blob, "tail"})
	return &layout{end + tailTotal - table, segments}, true
}

type locatedChunk struct {
	pack         uint32
	offset, clen int64
	flags        uint8
}

// decodeLocator reads a locator's pack digests and entries (chunk_store.Locator).
func decodeLocator(payload []byte) ([]string, []locatedChunk, bool) {
	if len(payload) < locatorHeaderSize || !bytes.Equal(payload[:8], locatorMagic) {
		return nil, nil, false
	}
	count := int64(binary.LittleEndian.Uint32(payload[16:]))
	size := int64(binary.LittleEndian.Uint32(payload[20:]))
	body := locatorHeaderSize + size
	if size > maxLocatorJSON || count > maxMapEntries || int64(len(payload)) != body+count*locatorEntrySize {
		return nil, nil, false
	}
	var document struct {
		Packs [][2]string `json:"packs"`
	}
	if err := json.Unmarshal(payload[locatorHeaderSize:body], &document); err != nil {
		return nil, nil, false
	}
	packs, seen := make([]string, len(document.Packs)), map[string]bool{}
	for i, pack := range document.Packs {
		decoded, err := hex.DecodeString(pack[0])
		if err != nil || len(decoded) != 32 || pack[0] != hex.EncodeToString(decoded) || seen[pack[0]] {
			return nil, nil, false
		}
		packs[i], seen[pack[0]] = pack[0], true
	}
	chunks := make([]locatedChunk, count)
	for i := int64(0); i < count; i++ { // Locator's own bounds: anything else is ucloud-chunk-store's error.
		entry := payload[body+i*locatorEntrySize:]
		chunk := locatedChunk{binary.LittleEndian.Uint32(entry), int64(binary.LittleEndian.Uint32(entry[4:])),
			int64(binary.LittleEndian.Uint32(entry[8:])), entry[12]}
		if int(chunk.pack) >= len(packs) || chunk.offset < packHeaderSize || chunk.clen <= 0 ||
			chunk.clen > maxClen || chunk.offset+chunk.clen > maxPackBytes || chunk.flags > 1 {
			return nil, nil, false
		}
		chunks[i] = chunk
	}
	return packs, chunks, true
}
