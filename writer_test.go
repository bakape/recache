package recache

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"hash/adler32"
	"testing"
)

func TestBindJSON(t *testing.T) {
	cache := NewCache(CacheOptions{})
	var f *Frontend
	f = cache.NewFrontend(func(k Key, rw *RecordWriter) (err error) {
		switch k.(int) {
		case 0:
			var data string
			err = rw.BindJSON(f, 1, &data)
			if err != nil {
				return
			}
			return json.NewEncoder(rw).Encode(data)
		case 1:
			return json.NewEncoder(rw).Encode("foo")
		default:
			return fmt.Errorf("unknown key: %d", k.(int))
		}
	})

	run := func() {
		s, err := f.Get(0)
		if err != nil {
			t.Fatal(err)
		}
		assertJsonStringEquals(t, s, "foo")
	}

	// Initial population
	run()

	// Read after the data has been populated and made immutable
	run()
}

func TestAdlerAppend(t *testing.T) {
	t.Parallel()

	buf1 := make([]byte, 128)
	buf2 := make([]byte, 128)
	for _, b := range [...][]byte{buf1, buf2} {
		n, err := rand.Read(b)
		if err != nil {
			t.Fatal(err)
		}
		if n != 128 {
			t.Fatalf("not enough bytes read: %d", n)
		}
	}

	genDesc := func(b []byte) frameDescriptor {
		return frameDescriptor{
			size:     uint32(len(b)),
			checksum: adler32.Checksum(b),
		}
	}

	fd1 := genDesc(buf1)
	fd2 := genDesc(buf2)
	appended := fd1
	appended.append(fd2)
	combined := genDesc(append(buf1, buf2...))
	if appended != combined {
		t.Fatalf("descriptors don't match: %+v != %+v", appended, combined)
	}
}
