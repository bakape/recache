package recache

import (
	"bytes"
	"compress/zlib"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// Simply writes the key to the record
var dummyGetter = func(k Key, rw *RecordWriter) error {
	return json.NewEncoder(rw).Encode(k)
}

func TestGetRecord(t *testing.T) {
	t.Parallel()

	var (
		cache = NewCache(CacheOptions{})
		f     = cache.NewFrontend(dummyGetter)
	)

	const key = "key1"

	run := func() {
		s, err := f.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		assertJsonStringEquals(t, s, key)
	}

	// Initial population
	run()

	// Read after the data has been populated and made immutable
	run()
}

func TestGetRecordConcurrentFetches(t *testing.T) {
	t.Parallel()

	var (
		cache = NewCache(CacheOptions{})
		f     = cache.NewFrontend(dummyGetter)
		wg    sync.WaitGroup
	)
	wg.Add(6)

	const key = "key1"

	run := func() {
		for k := 0; k < 3; k++ {
			// Concurrent key fetches
			go func(k int) {
				defer wg.Done()

				s, err := f.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				assertJsonStringEquals(t, s, key)
			}(k)
		}
	}

	// Initial population and 2 cache reads concurrently
	run()

	// 3 reads after the data has been populated and made immutable
	run()

	wg.Wait()
}

func TestGetRecordConcurentFrontends(t *testing.T) {
	t.Parallel()

	var (
		cache = NewCache(CacheOptions{})
		wg    sync.WaitGroup
	)
	wg.Add(9)

	test := func(t *testing.T, f *Frontend, j int) {
		key := "key" + strconv.Itoa(j)

		defer wg.Done()
		var keyWg sync.WaitGroup
		keyWg.Add(6)

		run := func() {
			// Concurrent key fetches
			for k := 0; k < 3; k++ {
				go func(k int) {
					defer keyWg.Done()

					s, err := f.Get(key)
					if err != nil {
						t.Fatal(err)
					}
					assertJsonStringEquals(t, s, key)
				}(k)
			}
		}

		// Initial population and 2 cache reads concurrently
		run()

		// 3 reads after the data has been populated and made
		// immutable
		run()

		keyWg.Wait()
	}

	for i := 0; i < 3; i++ {
		f := cache.NewFrontend(dummyGetter)
		for j := 0; j < 3; j++ {
			go test(t, f, j)
		}
	}

	wg.Wait()
}

func TestWriteHTTP(t *testing.T) {
	t.Parallel()

	var (
		etag         string
		_, frontends = prepareRecursion(0, 0)
		f            = frontends[0][1]
		key          = recursiveData{
			Frontend: 1,
			Key:      1,
		}
	)

	stdBuf, err := json.Marshal(recursiveStandard(0, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	std := string(stdBuf)

	cases := []struct {
		useDeflate bool
		etagMode   uint8
		name       string
	}{
		{
			name:       "first request",
			useDeflate: true,
		},
		{
			name:       "no etag match",
			etagMode:   1,
			useDeflate: true,
		},
		{
			name:       "etag match",
			etagMode:   2,
			useDeflate: true,
		},
	}
	for i := 0; i < 3; i++ {
		c := cases[i]
		c.name += " no deflate"
		c.useDeflate = false
		cases = append(cases, c)
	}

	for i := range cases {
		c := cases[i]
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/", nil)
			if c.useDeflate {
				req.Header.Set("Accept-Encoding", "gzip, deflate")
			}
			if c.etagMode == 2 {
				req.Header.Set("If-None-Match", etag)
			}

			_, err := f.WriteHTTP(key, rec, req)
			if err != nil {
				t.Fatal(err)
			}
			switch c.etagMode {
			case 0:
				etag = rec.Header().Get("ETag")
				if etag == "" {
					t.Fatal("no etag set")
				}

				s, err := f.Get(key)
				if err != nil {
					t.Fatal(err)
				}
				var stdETag string
				if c.useDeflate {
					stdETag = s.ETag()
				} else {
					stdETag = s.ETagDecompressed()
				}
				assertEquals(t, stdETag, etag)

				body := rec.Body
				if c.useDeflate {
					body = new(bytes.Buffer)
					zr, err := zlib.NewReader(rec.Body)
					if err != nil {
						t.Fatal(err)
					}
					defer zr.Close()
					_, err = io.Copy(body, zr)
					if err != nil {
						t.Fatal(err)
					}
				}
				t.Logf("got body: %s", rec.Body.String())
				assertEquals(
					t,
					strings.ReplaceAll(body.String(), "\n", ""),
					std,
				)

				h := s.SHA1()
				var pat string
				if c.useDeflate {
					pat = `"%s"`
				} else {
					pat = `"%s-uc"`
				}
				assertEquals(
					t,
					etag,
					fmt.Sprintf(
						pat,
						base64.RawStdEncoding.EncodeToString(h[:]),
					),
				)
			case 1:
				assertEquals(t, rec.Header().Get("ETag"), etag)
			case 2:
				assertEquals(t, rec.Code, 304)
			}
		})
	}
}
