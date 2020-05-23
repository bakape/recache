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
	rec *Record
}

// Data storage unit in the cache. Linked to a single Key on a Frontend.
type Record struct {
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
// a record will only have one component and will never have zero components.
type componentNode struct {
	component
	next *componentNode
}

// Implements io.WrWriteTo
func (r *Record) WriteTo(w io.Writer) (n int64, err error) {
	for c, m := &r.data, int64(0); c != nil; c = c.next {
		m, err = c.WriteTo(w)
		if err != nil {
			return
		}
		n += m
	}
	return
}

// Create a new io.Reader for this stream.
// Multiple instances of such an io.Reader can exist and be read
// concurrently.
func (r *Record) NewReader() io.Reader {
	return &recordReader{
		current: r.data.NewReader(),
		next:    r.data.next,
	}
}

// Create a new io.ReadCloser for the Decompressped content of this stream.
//
// It is the caller's responsibility to call Close on the io.ReadCloser
// when finished reading.
func (r *Record) Decompress() io.Reader {
	return eofCaster{flate.NewReader(r.NewReader())}
}

// Convenience method for efficiently decoding stream contents as JSON into
// the destination variable.
//
// dst: pointer to destination variable
func (r *Record) DecodeJSON(dst interface{}) (err error) {
	return json.NewDecoder(r.Decompress()).Decode(dst)
}

// Return SHA1 hash of the content
func (r *Record) SHA1() [sha1.Size]byte {
	return r.hash
}

// Return strong ETag of content, if served as a compressed stream
func (r *Record) ETag() string {
	return r.eTag
}

// Return strong ETag of content, if served as a decompressed stream
func (r *Record) ETagDecompressed() string {
	return r.eTag[:len(r.eTag)-1] + `-uc"`
}

// Adapter for reading data from record w/o mutating it
type recordReader struct {
	current io.Reader
	next    *componentNode
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

// Suppresses unexpected EOF errors resulting as a consequence of flate using
// bufio
type eofCaster struct {
	io.Reader
}

func (e eofCaster) Read(p []byte) (n int, err error) {
	n, err = e.Reader.Read(p)
	if err == io.ErrUnexpectedEOF {
		err = io.EOF
	}
	return
}
