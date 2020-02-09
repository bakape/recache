package recache

import (
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
)

var (
	// Indicates no components have been written and no error has been returned
	// in a call to Getter. This is not allowed.
	ErrEmptyRecord = errors.New("empty record created")
)

// Value used to store entries in the cache. Must be a type suitable for being a
// key in a Go map.
type Key interface{}

// Generates fresh cache records for the given key by writing to RecordWriter.
// Getter must be thread-safe.
type Getter func(Key, *RecordWriter) error

// Readable stream with support for io.WriterTo and conversion to
// io.Reader interfaces
type Streamer interface {
	// Can be called safely from multiple goroutines
	io.WriterTo

	// Create a new io.Reader for this stream.
	// Multiple instances of such an io.Reader can exist and be read
	// concurrently.
	NewReader() io.Reader

	// Convenience method for efficiently decoding stream contents as JSON into
	// the destination variable.
	//
	// dst: pointer to destination variable
	DecodeJSON(dst interface{}) error

	// Create a new io.ReadCloser for the unzipped content of this stream.
	//
	// It is the caller's responsibility to call Close on the io.ReadCloser
	// when finished reading.
	Unzip() io.ReadCloser

	// Return SHA1 hash of the content
	SHA1() [sha1.Size]byte

	// Return strong etag of content
	ETag() string
}

// A frontend for accessing the cache contents
type Frontend struct {
	id, level int
	cache     *Cache
	getter    Getter
}

// Populates a record using the registered Getter
func (f *Frontend) populate(k Key, rec *record) (err error) {
	rw := RecordWriter{
		cache:    f.cache.id,
		frontend: f.id,
		key:      k,
		level:    f.level,
	}
	err = f.getter(k, &rw)
	if err != nil {
		return
	}

	// Flush any unclosed gzip buffers
	err = rw.flushPending(true)
	if err != nil {
		return
	}

	// Much of our code assumes there is at least oner component in a record.
	// 0 component records don't have any use anyway.
	if rw.data.component == nil {
		return ErrEmptyRecord
	}

	rec.data = rw.data
	memoryUsed := 0
	if rec.data.next == nil {
		// Most records will have only one component, so this is a hotpath
		memoryUsed = rec.data.Size()
		rec.hash = rec.data.Hash()
	} else {
		h := sha1.New()
		for c := &rec.data; c != nil; c = c.next {
			memoryUsed += c.Size()

			// Hash the child hash to better propagate changes
			arr := c.Hash()
			h.Write(arr[:])
		}
		copy(rec.hash[:], h.Sum(nil))
	}

	// Known size, so using array on the stack instead of heap allocation
	var b [27 + 2]byte
	b[0] = '"'
	base64.RawStdEncoding.Encode(b[1:], rec.hash[:])
	b[28] = '"'
	rec.eTag = string(b[:])

	f.cache.setUsedMemory(rec, recordLocation{f.id, k}, memoryUsed)

	return
}

// Get a record by key and block until it has been generated
func (f *Frontend) getGeneratedRecord(k Key) (rec *record, err error) {
	loc := recordLocation{f.id, k}
	rec, fresh := f.cache.getRecord(loc)
	if fresh {
		err = f.populate(k, rec)
		if err != nil {
			// Propagate error to any concurrent readers
			rec.populationError = err

			f.cache.evict(loc, 0)
		}

		// Also unblock any concurrent readers, even on error.
		// Having it here also protects from data races on rec.populationError.
		rec.semaphore.Unblock()
	}

	// Prevents a record being read concurrently before it is populated.
	// A record is immutable after initial population and this will not block
	// after it.
	rec.semaphore.Wait()
	err = rec.populationError

	return
}

// Retrieve or generate data by key and return a consumable result Stream
func (f *Frontend) Get(k Key) (s Streamer, err error) {
	rec, err := f.getGeneratedRecord(k)
	if err != nil {
		return
	}
	s = recordDecoder{rec}
	return
}

// Retrieve or generate data by key and write it to w.
// Writes ETag to w and returns 304 on ETag match without writing data.
// Sets "Content-Encoding" header to "gzip".
func (f *Frontend) WriteHTTP(k Key, w http.ResponseWriter, r *http.Request,
) (n int64, err error) {
	rec, err := f.getGeneratedRecord(k)
	if err != nil {
		return
	}
	if r.Header.Get("If-None-Match") == rec.eTag {
		w.WriteHeader(304)
		return
	}

	h := w.Header()
	h.Set("Content-Encoding", "gzip")
	h.Set("ETag", rec.eTag)
	return rec.WriteTo(w)
}
