package recache

import (
	"compress/flate"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	errSample = errors.New("sample generation error")
)

func TestGetRecordConcurentCaches(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(27)

	test := func(t *testing.T, cache *Cache, f *Frontend, j int) {
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

		// Initial population and 2 caches reads
		// concurrently
		run()

		// 3 reads after the data has been populated and
		// made immutable
		run()

		keyWg.Wait()
	}

	for j := 0; j < 3; j++ {
		var cache = NewCache(CacheOptions{})
		for i := 0; i < 3; i++ {
			f := cache.NewFrontend(dummyFrontOpts)
			for j := 0; j < 3; j++ {
				go test(t, cache, f, j)
			}
		}
	}

	wg.Wait()
}

// Tests for overall correctness and data races with complicated intra-cache and
// inter-cache recursion
func TestWithRecursion(t *testing.T) {
	t.Parallel()

	cases := [...]struct {
		name        string
		memoryLimit uint
		lruLimit    time.Duration
	}{
		{
			name: "no limits",
		},
		{
			name:        "memory limit",
			memoryLimit: 1024,
		},
		{
			name:     "LRU limit",
			lruLimit: time.Nanosecond * 10,
		},
		{
			name:        "memory and LRU limit",
			memoryLimit: 1024,
			lruLimit:    time.Nanosecond * 10,
		},
	}

	for i := range cases {
		c := cases[i]
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			var wg sync.WaitGroup
			caches, frontends := prepareRecursion(c.memoryLimit, c.lruLimit)
			testRecursion(t, &wg, caches, frontends)
			wg.Wait()
			assertConsistency(t, caches[:]...)
		})
	}
}

type recursiveNode struct {
	Data     recursiveData   `json:"data"`
	Children []recursiveNode `json:"children"`
}

type recursiveData struct {
	Cache    int `json:"cache"`
	Frontend int `json:"frontend"`
	Key      int `json:"key"`
}

// Generate expected data for recursive queries
func recursiveStandard(cache, frontend, key int) (node recursiveNode) {
	node.Data = recursiveData{
		Cache:    cache,
		Frontend: frontend,
		Key:      key,
	}

	// Intra-frontend recursion
	if key > 0 {
		node.Children = append(node.Children,
			recursiveStandard(cache, frontend, key-1))
	}

	// Intra-cache recursion
	if frontend > 0 {
		node.Children = append(node.Children,
			recursiveStandard(cache, frontend-1, key))
	}

	// Inter-cache recursion
	if cache > 0 {
		node.Children = append(node.Children,
			recursiveStandard(cache-1, frontend, key))
	}

	// Ensure serialization as array
	if node.Children == nil {
		node.Children = []recursiveNode{}
	}

	return
}

// Generate caches and frontends for recursion tests
func prepareRecursion(memoryLimit uint, lruLimit time.Duration,
) ([3]*Cache, [3][3]*Frontend) {
	var (
		caches    [3]*Cache
		frontends [3][3]*Frontend
	)

	getter := func(k Key, rw *RecordWriter) (err error) {
		data := k.(recursiveData)

		if data.Key == 2 {
			return errSample
		}

		_, err = rw.Write([]byte(`{"data":`))
		if err != nil {
			return
		}

		err = json.NewEncoder(rw).Encode(data)
		if err != nil {
			return
		}

		// Also test reading from buffer
		_, err = rw.ReadFrom(strings.NewReader(`,"children":[`))
		if err != nil {
			return
		}

		first := true
		include := func(k recursiveData) (err error) {
			if !first {
				_, err = rw.Write([]byte{','})
				if err != nil {
					return
				}
			} else {
				first = false
			}

			err = rw.Include(frontends[k.Cache][k.Frontend], k)
			return
		}

		// Intra-frontend recursion
		if k := data; k.Key > 0 {
			k.Key--
			err = include(k)
			if err != nil {
				return
			}
		}

		// Intra-cache recursion
		if k := data; k.Frontend > 0 {
			k.Frontend--
			err = include(k)
			if err != nil {
				return
			}
		}

		// Inter-cache recursion
		if k := data; k.Cache > 0 {
			k.Cache--
			err = include(k)
			if err != nil {
				return
			}
		}

		_, err = rw.Write([]byte(`]}`))
		return
	}

	for i := 0; i < 3; i++ {
		caches[i] = NewCache(CacheOptions{
			MemoryLimit: memoryLimit,
			LRULimit:    lruLimit,
		})
		l := flate.BestCompression
		for j := 0; j < 3; j++ {
			frontends[i][j] = caches[i].NewFrontend(FrontendOptions{
				Get:   getter,
				Level: &l,
			})
		}
	}

	return caches, frontends
}

func testRecursion(t *testing.T, wg *sync.WaitGroup, caches [3]*Cache,
	frontends [3][3]*Frontend,
) {
	wg.Add(9 * 3)

	test := func(t *testing.T, cacheID, frontendID, keyID int) {
		defer wg.Done()
		var keyWg sync.WaitGroup
		keyWg.Add(6)

		key := recursiveData{
			Cache:    cacheID,
			Frontend: frontendID,
			Key:      keyID,
		}

		run := func() {
			// Concurrent key fetches
			for k := 0; k < 3; k++ {
				go func(k int) {
					defer keyWg.Done()

					s, err := frontends[cacheID][frontendID].
						Get(key)
					if keyID != 2 {
						// Normal generation
						if err != nil {
							t.Fatal(err)
						}
						std := recursiveStandard(
							cacheID,
							frontendID,
							keyID,
						)
						var res recursiveNode
						err = s.DecodeJSON(&res)
						if err != nil {
							t.Fatal(err)
						}
						assertEquals(t, res, std)
					} else {
						// Error passing
						assertEquals(t, err, errSample)
					}
				}(k)
			}
		}
		// Initial population and 2 cache reads concurrently
		run()

		// 3 reads after the data has been populated and
		// made immutable
		run()

		keyWg.Wait()
	}

	for cacheID := 0; cacheID < 3; cacheID++ {
		for frontendID := 0; frontendID < 3; frontendID++ {
			for keyID := 0; keyID < 3; keyID++ {
				go test(t, cacheID, frontendID, keyID)
			}
		}
	}

	wg.Wait()
}

// Assert cache metadata is still consistent with its data
func assertConsistency(t *testing.T, caches ...*Cache) {
	for i, c := range caches {
		c := c // Force heap allocation
		t.Run(fmt.Sprintf("cache_%d", i), func(t *testing.T) {
			c.mu.Lock()
			defer c.mu.Unlock()

			t.Run("linked list consistency", func(t *testing.T) {
				t.Parallel()

				var prev *node
				for n, i := c.lruList.front, 0; n != nil; n, i = n.next, i+1 {
					t.Run(fmt.Sprintf("node_%d", i), func(t *testing.T) {
						rec, ok := c.record(n.location)
						if !ok {
							t.Fatal("points to missing key")
						}
						if rec.node != n {
							t.Fatal("record does not point to node")
						}
						if i != 0 {
							prev, _ := c.record(prev.location)
							if prev.lastUsed.Before(rec.lastUsed) {
								t.Fatal("list not in LRU order")
							}
						}
						prev = n
					})
				}
			})

			t.Run("used memory consistency", func(t *testing.T) {
				t.Parallel()

				used := 0
				for _, b := range c.frontends {
					for _, rec := range b {
						recUsed := 0
						for c := &rec.rec.data; c != nil; c = c.next {
							recUsed += c.Size()
						}
						if recUsed != rec.memoryUsed {
							t.Fatal("record used memory mismatch")
						}
						used += recUsed
					}
				}
				if c.memoryUsed != used {
					t.Fatal("cache used memory mismatch")
				}
				if c.memoryUsed < 0 {
					t.Fatal("negative used memory")
				}
			})

			t.Run("records have nodes", func(t *testing.T) {
				t.Parallel()

				for _, b := range c.frontends {
					for _, rec := range b {
						has := false
						for n := c.lruList.front; n != nil; n = n.next {
							if n == rec.node {
								has = true
								break
							}
						}
						if !has {
							t.Fatal("no linked node for record")
						}
					}
				}
			})

			// The eviction of records over the LRU or memory limits in the
			// cache is eventual and thus not tested.
		})
	}
}
