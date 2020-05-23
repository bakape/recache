package recache

import (
	"bytes"
	"compress/flate"
	"crypto/sha1"
	"hash"
	"hash/adler32"
	"io"
)

// Describes a single constituent deflate-compressed frame of a record
type frameDescriptor struct {
	checksum uint32 // Adler32 checksum
	size     uint32 // Uncompressed size
}

// Appending another frameDescriptor onto f
func (f *frameDescriptor) Append(rhs frameDescriptor) {
	f.size += rhs.size // Allowed to overflow

	// Merge Adler32 checksums. Based on adler32_combine() from zlib.
	// Copyright (C) 1995-2011, 2016 Mark Adler
	const base = uint32(65521) // largest prime smaller than 65536
	rem := rhs.size % base
	sum1 := f.checksum & 0xffff
	sum2 := (rem * sum1) % base
	sum1 += (rhs.checksum & 0xffff) + base - 1
	sum2 += ((f.checksum >> 16) & 0xffff) +
		((rhs.checksum >> 16) & 0xffff) +
		base -
		rem
	if sum1 >= base {
		sum1 -= base
	}
	if sum1 >= base {
		sum1 -= base
	}
	if sum2 >= (base << 1) {
		sum2 -= base << 1
	}
	if sum2 >= base {
		sum2 -= base
	}
	f.checksum = sum1 | (sum2 << 16)
}

// Provides utility methods for building record buffers and recursive record
// trees
type RecordWriter struct {
	compressing     bool // Currently compressing data into a buffer
	cache, frontend int
	key             Key

	compressor *flate.Writer
	current    struct { // Deflate frame currently being compressed
		bytes.Buffer
		size uint32
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
			rw.compressor, err = flate.NewWriter(&rw.current, CompressionLevel)
			if err != nil {
				return
			}
			rw.hasher = adler32.New()
		} else {
			rw.current.Reset()
			rw.current.size = 0
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
		arr [512]byte
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
		Record: rec,
	})

	return
}

func (rw *RecordWriter) bind(f *Frontend, k Key) (rec *Record, err error) {
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
func (rw *RecordWriter) Bind(f *Frontend, k Key) (*Record, error) {
	return rw.bind(f, k)
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
		err = rw.compressor.Flush()
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
		buf.size = rw.current.size
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
