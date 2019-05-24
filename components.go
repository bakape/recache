package recache

import (
	"crypto/sha1"
	"io"
)

// Contains either a buffer or a reference to another record
type component interface {
	io.WriterTo
	NewReader() io.Reader
	Size() int
	Hash() [sha1.Size]byte
}

// Common part of both buffer and reference components
type componentCommon struct {
	hash [sha1.Size]byte
}

func (c componentCommon) Hash() [sha1.Size]byte {
	return c.hash
}

// Contains a gzipped buffer
type buffer struct {
	componentCommon
	data []byte
}

func (b buffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.data)
	return int64(n), err
}

func (b buffer) NewReader() io.Reader {
	return &bufferReader{
		buffer: b,
	}
}

func (b buffer) Size() int {
	return len(b.data)
}

// Adapter for reading data from component w/o mutating it
type bufferReader struct {
	off int
	buffer
}

func (r *bufferReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.off:])
	r.off += n
	return
}

// Reference to another record
type recordReference struct {
	componentCommon
	*record
}

func (r recordReference) Size() int {
	// A record reference is considered to not store any data itself
	return 0
}
