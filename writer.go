package recache

import (
	"bytes"
	"compress/flate"
	"crypto/sha1"
	"hash"
	"hash/adler32"
	"io"
)

// Provides utility methods for building record buffers and recursive record
// trees
type RecordWriter struct {
	compressing            bool // Currently compressing data into a buffer
	cache, frontend, level int
	key                    Key

	compressor *flate.Writer
	current    struct { // Deflate frame currently being compressed
		bytes.Buffer
		frameDescriptor
	}
	hasher hash.Hash32 // Adler32 checksum builder

	data componentNode
	last *componentNode
}

// Write non-compressed data to the record for storage
func (rw *RecordWriter) Write(p []byte) (n int, err error) {
	if !rw.compressing {
		// Initialize or reset pipeline state.
		// Reuse allocated resources, if possible.
		if rw.compressor == nil {
			rw.compressor, err = flate.NewWriter(&rw.current, rw.level)
			if err != nil {
				return
			}
			rw.hasher = adler32.New()
		} else {
			rw.current.Reset()
			rw.current.frameDescriptor = frameDescriptor{}
			rw.hasher.Reset()
			rw.compressor.Reset(&rw.current)
		}
		rw.compressing = true
	}

	n, err = rw.compressor.Write(p)
	if err != nil {
		return
	}
	rw.current.size += uint32(n)
	_, err = rw.hasher.Write(p)
	return
}

// Read non-compressed data from r and write it to the record for storage
func (rw *RecordWriter) ReadFrom(r io.Reader) (n int64, err error) {
	var (
		m   int
		arr [4 << 10]byte
	)
	for {
		buf := arr[:]
		m, err = r.Read(buf)
		n += int64(m)
		switch err {
		case nil:
			_, err = rw.Write(buf[:m])
			if err != nil {
				return
			}
		case io.EOF:
			err = nil
			return
		default:
			return
		}
	}
}

// Include data from passed frontend by key and bind it to rw.
// The record generated by rw will automatically be evicted from its parent
// cache on eviction of the included record.
func (rw *RecordWriter) Include(f *Frontend, k Key) (err error) {
	rec, err := rw.bind(f, k)
	if err != nil {
		return
	}

	rw.append(recordReference{
		componentCommon: componentCommon{
			hash: rec.hash,
		},
		record: rec,
	})

	return
}

func (rw *RecordWriter) bind(f *Frontend, k Key) (rec *record, err error) {
	// Finish any previous buffer writes
	err = rw.flush(false)
	if err != nil {
		return
	}

	rec, err = f.getGeneratedRecord(k)
	if err != nil {
		return
	}

	registerDependance(
		intercacheRecordLocation{
			cache: rw.cache,
			recordLocation: recordLocation{
				frontend: rw.frontend,
				key:      rw.key,
			},
		},
		intercacheRecordLocation{
			cache: f.cache.id,
			recordLocation: recordLocation{
				frontend: f.id,
				key:      k,
			},
		},
	)

	return
}

// Bind to record from passed frontend by key and return a consumable stream
// of the retrieved record.
// The record generated by rw will automatically be evicted from its parent
// cache on eviction of the included record.
func (rw *RecordWriter) Bind(f *Frontend, k Key) (Streamer, error) {
	rec, err := rw.bind(f, k)
	if err != nil {
		return nil, err
	}
	return recordDecoder{rec}, nil
}

// Bind to record from passed frontend by key and decode it as JSON into dst.
// The record generated by rw will automatically be evicted from its parent
// cache on eviction of the included record.
func (rw *RecordWriter) BindJSON(
	f *Frontend,
	k Key,
	dst interface{},
) (err error) {
	s, err := rw.Bind(f, k)
	if err != nil {
		return
	}
	return s.DecodeJSON(dst)
}

// Flush the current deflate stream, if any.
//
// final: this is the final flush and copying of buffer is not required
func (rw *RecordWriter) flush(final bool) (err error) {
	if rw.compressing {
		err = rw.compressor.Close()
		if err != nil {
			return
		}

		var buf buffer
		if final {
			buf.data = rw.current.Bytes()
		} else {
			buf.data = make([]byte, rw.current.Len())
			copy(buf.data, rw.current.Bytes())
		}
		buf.hash = sha1.Sum(buf.data)
		buf.frameDescriptor = rw.current.frameDescriptor
		buf.frameDescriptor.checksum = rw.hasher.Sum32()

		rw.append(buf)
		rw.compressing = false
	}
	return
}

// Append new component to linked list
func (rw *RecordWriter) append(c component) {
	if rw.last == nil {
		// First component
		rw.data = componentNode{
			component: c,
		}
		rw.last = &rw.data
	} else {
		n := &componentNode{
			component: c,
		}
		rw.last.next = n
		rw.last = n
	}
}
