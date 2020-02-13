package recache

import (
	"hash/adler32"
	"testing"
)

func TestAdlerAppend(t *testing.T) {
	t.Parallel()

	buf1 := []byte("WAI")
	buf2 := []byte("ZUMA")

	genDesc := func(b []byte) frameDescriptor {
		return frameDescriptor{
			size:     uint32(len(b)),
			checksum: adler32.Checksum(b),
		}
	}

	fd1 := genDesc(buf1)
	fd2 := genDesc(buf2)
	appended := fd1
	appended.Append(fd2)
	combined := genDesc(append(buf1, buf2...))
	if appended != combined {
		t.Fatalf("descriptors don't match: %+v != %+v", appended, combined)
	}
}
