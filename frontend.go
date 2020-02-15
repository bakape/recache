package recache

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
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

	// Create a new io.ReadCloser for the Decompressped content of this stream.
	//
	// It is the caller's responsibility to call Close on the io.ReadCloser
	// when finished reading.
	Decompress() io.Reader

	// Return SHA1 hash of the content
	SHA1() [sha1.Size]byte

	// Return strong etag of content
	ETag() string
}

// A frontend for accessing the cache contents
type Frontend struct {
	id     int
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

	// Flush any unclosed deflate streams
	err = rw.flush(true)
	if err != nil {
		return
	}

	// Much of our code assumes there is at least one component in a record.
	// 0 component records don't have any use anyway.
	if rw.data.component == nil {
		return ErrEmptyRecord
	}

	rec.data = rw.data
	rec.frameDescriptor = rw.data.GetFrameDescriptor()
	memoryUsed := 0
	if rec.data.next == nil {
		// Most records will have only one component, so this is a hotpath
		memoryUsed = rec.data.Size()
		rec.hash = rec.data.Hash()
	} else {
		h := sha1.New()
		first := true
		for c := &rec.data; c != nil; c = c.next {
			memoryUsed += c.Size()
			if !first {
				rec.frameDescriptor.Append(c.GetFrameDescriptor())
			} else {
				first = false
			}

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
// Sets "Content-Encoding" header to "deflate".
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
	h.Set("ETag", rec.eTag)
	h.Set("Content-Encoding", "deflate")

	// Deflate compression, as specified by the HTTP spec, actually expects the
	// zlib file format. Write the zlib header and footer here.

	header := [2]byte{
		0: 0x78, // Deflate compression with default window size,
	}

	// Writes compression level into first 2 bits of byte 2
	switch CompressionLevel {
	case -2, 0, 1:
		header[1] = 0 << 6 // fastest
	case 2, 3, 4, 5:
		header[1] = 1 << 6 // fast
	case 6, -1:
		header[1] = 2 << 6 // default
	case 7, 8, 9:
		header[1] = 3 << 6 // best
	default:
		err = fmt.Errorf("unknown compression level: %d", CompressionLevel)
		return
	}

	// Writes mod-31 checksum into last 5 bytes of header
	header[1] += uint8(31 - (uint16(header[0])<<8+uint16(header[1]))%31)

	_, err = w.Write(header[:])
	if err != nil {
		return
	}
	n = 2

	m, err := rec.WriteTo(w)
	if err != nil {
		return
	}
	n += m

	// Final empty deflate block and adler32 checksum
	footer := [6]byte{
		0: 0x03,
	}
	binary.BigEndian.PutUint32(footer[2:], rec.checksum)
	_, err = w.Write(footer[:])
	if err != nil {
		return
	}
	n += 6

	return
}
