package recache

import (
	"crypto/sha1"
	"io"
)

// Contains either a buffer or a reference to another record
type component interface {
	io.WriterTo
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

func (b buffer) Size() int {
	return len(b.data)
}

// Reference to another record
type recordReference struct {
	componentCommon
	ref io.WriterTo
}

func (r recordReference) WriteTo(w io.Writer) (int64, error) {
	return r.ref.WriteTo(w)
}

func (r recordReference) Size() int {
	// A record reference is considered to not store any data itself
	return 0
}
