package recache

import (
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"io"
	"sync"
)

var (
	buffPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 4<<10)
		},
	}
)

// Provides utility methods for building record buffers and recursive record
// trees
//
// TODO: Composition with automatic eviction through channels
type RecordWriter struct {
	compressing bool // Currently compressing data into a buffer
	gzWriter    *gzip.Writer
	pending     bytes.Buffer
	data        []component
}

// Write non-gzipped data to the record for storage
func (rw *RecordWriter) Write(p []byte) (n int, err error) {
	if !rw.compressing {
		// Initialize or reset pipeline state.
		// Reuse allocated resources, if possible
		rw.pending.Reset()
		if rw.gzWriter == nil {
			rw.gzWriter, err = gzip.NewWriterLevel(&rw.pending,
				gzip.BestCompression)
			if err != nil {
				return
			}
		} else {
			rw.gzWriter.Reset(&rw.pending)
		}
	}
	_, err = rw.pending.Write(p)
	return
}

// Read non-gzipped data from r and write it to the record for storage
func (rw *RecordWriter) ReadFrom(r io.Reader) (n int64, err error) {
	var (
		m   int
		buf = buffPool.Get().([]byte)
	)
	defer buffPool.Put(buf)
	for {
		m, err = r.Read(buf)
		n += int64(m)
		if err != nil {
			return
		}
		_, err = rw.Write(buf[:m])
		if err != nil {
			return
		}
	}
	return
}

// Flush any pending buffer.
// final: this is the final flush and copy of buffer is not required
func (rw *RecordWriter) flushPending(final bool) (err error) {
	if rw.compressing {
		err = rw.gzWriter.Close()
		if err != nil {
			return
		}

		var buf buffer
		if final {
			buf.data = rw.pending.Bytes()
		} else {
			buf.data = make([]byte, rw.pending.Len())
			copy(buf.data, rw.pending.Bytes())
		}
		buf.hash = sha1.Sum(buf.data)

		rw.compressing = false
	}
	return
}
