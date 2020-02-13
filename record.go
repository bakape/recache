package recache

import (
	"compress/flate"
	"crypto/sha1"
	"encoding/json"
	"io"
	"time"
)

// Describes record location in a cache
type recordLocation struct {
	frontend int
	key      Key
}

// Describes record location across all caches
type intercacheRecordLocation struct {
	cache int
	recordLocation
}

// Kept separate from the record to localize locking regions
type recordWithMeta struct {
	// Memory used by the record, not counting any contained references or
	// storage infrastructure metadata.
	memoryUsed int

	// Time of most recent use of record
	lastUsed time.Time

	// Keep pointer to node in LRU list, so we can modify the list without
	// itterating it to find this record's node.
	node *node

	// Records that include this record and should be evicted on this record's
	// eviction
	includedIn []intercacheRecordLocation

	// The record itself. Has a separate lock and can be modified without the
	// lock on the cache mutex held.
	//
	// Record must be a pointer, because it contains mutexes.
	rec *record
}

// Data storage unit in the cache. Linked to a single Key on a Frontend.
type record struct {
	semaphore semaphore

	// Contained data and metainformation
	data componentNode
	frameDescriptor
	hash [sha1.Size]byte
	eTag string // generated from hash

	// Error that occurred during initial data population. This will also be
	// returned on any readers that are concurrent with population.
	// Might cause error duplication, but better than returning nothing on
	// concurrent reads.
	populationError error
}

// Linked list node for storing components. This is optimal, as most of the time
// a record will only have one component.
type componentNode struct {
	component
	next *componentNode
}

func (r *record) WriteTo(w io.Writer) (n int64, err error) {
	for c, m := &r.data, int64(0); c != nil; c = c.next {
		m, err = c.WriteTo(w)
		if err != nil {
			return
		}
		n += m
	}
	return
}

func (r *record) NewReader() io.Reader {
	return &recordReader{
		next:    r.data.next,
		current: r.data.NewReader(),
		record:  r,
	}
}

// Adapter for reading data from record w/o mutating it
type recordReader struct {
	current io.Reader
	next    *componentNode
	*record
}

func (r *recordReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	if r.current == nil {
		if r.next == nil {
			return 0, io.EOF
		}
		r.current = r.next.NewReader()
		r.next = r.next.next
	}

	n, err = r.current.Read(p)
	if err == io.EOF {
		// Fully consumed current reader
		err = nil
		r.current = nil
	}
	return
}

// Adapter, that enables decoding the record as JSON
type recordDecoder struct {
	*record
}

func (r recordDecoder) DecodeJSON(dst interface{}) (err error) {
	return json.NewDecoder(r.Decompress()).Decode(dst)
}

func (r recordDecoder) Decompress() io.Reader {
	return flate.NewReader(r.NewReader())
}

func (r recordDecoder) SHA1() [sha1.Size]byte {
	return r.record.hash
}

func (r recordDecoder) ETag() string {
	return r.eTag
}
