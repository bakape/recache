package recache

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
)

// Simply writes the key to the record
func dummyGetter(k Key, rw *RecordWriter) error {
	return json.NewEncoder(rw).Encode(k)
}

func TestGetRecord(t *testing.T) {
	t.Parallel()

	var (
		cache = NewCache(Options{})
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
		cache = NewCache(Options{})
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
		cache = NewCache(Options{})
		wg    sync.WaitGroup
	)
	wg.Add(9)

	for i := 0; i < 3; i++ {
		go func() {
			f := cache.NewFrontend(dummyGetter)
			for j := 0; j < 3; j++ {
				go func(j int) {
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
				}(j)
			}
		}()
	}

	wg.Wait()
}

func TestWriteHTTP(t *testing.T) {
	t.Parallel()

	var etag string

	cache := NewCache(Options{})
	f := cache.NewFrontend(dummyGetter)

	cases := [...]struct {
		name string
	}{
		{"first request"},
		{"no etag match"},
		{"etag match"},
	}

	for i := range cases {
		c := cases[i]
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("GET", "/", nil)
			if i == 2 {
				req.Header.Set("If-None-Match", etag)
			}

			_, err := f.WriteHTTP(1, rec, req)
			if err != nil {
				t.Fatal(err)
			}
			switch i {
			case 0:
				etag = rec.Header().Get("ETag")
				if etag == "" {
					t.Fatal("no etag set")
				}
			case 1:
				assertEquals(t, rec.Header().Get("ETag"), etag)
			case 2:
				assertEquals(t, rec.Code, 304)
			}
		})
	}
}
