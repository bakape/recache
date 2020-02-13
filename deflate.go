package recache

import "encoding/binary"

var (
	// Shorthand
	le = binary.LittleEndian
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
