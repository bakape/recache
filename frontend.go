package recache

import (
	"crypto/sha1"
	"encoding/base64"
	"io"
	"net/http"
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
}

// A frontend for accessing the cache contents
type Frontend struct {
	id     uint
	cache  *Cache
	getter Getter
}

// Populates a record using the registered Getter
func (f *Frontend) populate(k Key, rec *record) (err error) {
	rw := RecordWriter{
		cache:    f.cache.id,
		frontend: f.id,
		key:      k,
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

	rec.data = rw.data
	memoryUsed := 0
	if len(rec.data) == 1 {
		d := rec.data[0]
		memoryUsed = d.Size()
		rec.hash = d.Hash()
	} else {
		h := sha1.New()
		for _, d := range rec.data {
			memoryUsed += d.Size()

			// Hash the child hash to better propagate changes
			arr := d.Hash()
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
	rec, fresh := f.cache.getRecord(recordLocation{f.id, k})
	if fresh {
		err = f.populate(k, rec)
		if err != nil {
			// Propagate error to any concurrent readers
			rec.populationError = err

			f.cache.evict(recordLocation{f.id, k})
		}

		// Also unblock any concurrent readers, even on error.
		// Having it here also protects from data race on rec.populationError.
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

// Evict a record by key, if any
func (f *Frontend) Evict(k Key) {
	f.cache.evict(recordLocation{f.id, k})
}

// Evict all records from frontend
func (f *Frontend) EvictAll() {
	f.cache.evictFrontend(f.id)
}

// Evict keys from frontend using matcher function fn.
// fn returns true, if a key must be evicted.
func (f *Frontend) EvictByFunc(fn func(Key) (bool, error)) error {
	return f.cache.evictByFunc(f.id, fn)
}
