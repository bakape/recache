package recache

import (
	"bytes"
	"compress/flate"
	"crypto/sha1"
	"io"
)

// Contains either a buffer or a reference to another record
type component interface {
	io.WriterTo
	NewReader() io.Reader
	Size() int
	Hash() [sha1.Size]byte
	GetFrameDescriptor() frameDescriptor
	Decompress() io.Reader
}

// Common part of both buffer and reference components
type componentCommon struct {
	hash [sha1.Size]byte
}

func (c componentCommon) Hash() [sha1.Size]byte {
	return c.hash
}

// Contains a deflate-compressed buffer
type buffer struct {
	componentCommon
	frameDescriptor
	data []byte
}

func (b buffer) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(b.data)
	return int64(n), err
}

func (b buffer) NewReader() io.Reader {
	return bytes.NewReader(b.data)
}

func (b buffer) Size() int {
	return len(b.data)
}

func (b buffer) GetFrameDescriptor() frameDescriptor {
	return b.frameDescriptor
}

// Read component as decompressed stream
func (b buffer) Decompress() io.Reader {
	return flate.NewReader(b.NewReader())
}

// Reference to another record
type recordReference struct {
	componentCommon
	*Record
}

func (r recordReference) Size() int {
	// A record reference is considered to not store any data itself
	return 0
}

func (r recordReference) GetFrameDescriptor() frameDescriptor {
	return r.frameDescriptor
}
