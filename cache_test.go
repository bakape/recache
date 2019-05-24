package recache

import (
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
	wg.Add(9 * 3)

	for j := 0; j < 3; j++ {
		go func() {
			var cache = NewCache(0, 0)

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

							// Initial population and 2 caches reads
							// concurrently
							run()

							// 3 reads after the data has been populated and
							// made immutable
							run()

							keyWg.Wait()
						}(j)
					}
				}()
			}
		}()
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
		caches[i] = NewCache(memoryLimit, lruLimit)
		for j := 0; j < 3; j++ {
			frontends[i][j] = caches[i].NewFrontend(getter)
		}
	}

	return caches, frontends
}

func testRecursion(t *testing.T, wg *sync.WaitGroup, caches [3]*Cache,
	frontends [3][3]*Frontend,
) {
	wg.Add(9 * 3)

	for cacheID := 0; cacheID < 3; cacheID++ {
		go func(cacheID int) {
			for frontendID := 0; frontendID < 3; frontendID++ {
				go func(frontendID int) {
					for keyID := 0; keyID < 3; keyID++ {
						go func(keyID int) {
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
							// Initial population and 2 caches reads
							// concurrently
							run()

							// 3 reads after the data has been populated and
							// made immutable
							run()

							keyWg.Wait()
						}(keyID)
					}
				}(frontendID)
			}
		}(cacheID)
	}
}

// Assert cache metadata is still consistent with its data
func assertConsistency(t *testing.T, caches ...*Cache) {
	for i, c := range caches {
		c := c // Force heap allocation
		t.Run(fmt.Sprintf("cache_%d", i), func(t *testing.T) {
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
				for _, b := range c.buckets {
					for _, rec := range b {
						recUsed := 0
						for _, c := range rec.rec.data {
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

				for _, b := range c.buckets {
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

func TestEviction(t *testing.T) {
	t.Parallel()

	prepareCache := func() (*Cache, [3]*Frontend) {
		var wg sync.WaitGroup
		caches, frontends := prepareRecursion(0, 0)
		testRecursion(t, &wg, caches, frontends)
		wg.Wait()

		assertConsistency(t, caches[:]...)

		return caches[0], frontends[0]
	}

	t.Run("cache", func(t *testing.T) {
		t.Parallel()

		c, _ := prepareCache()

		c.EvictAll()
		assertConsistency(t, c)

		if c.memoryUsed != 0 {
			t.Fatal("memory used not 0")
		}
		for _, b := range c.buckets {
			if len(b) != 0 {
				t.Fatal("bucket not empty")
			}
		}
	})

	t.Run("frontend", func(t *testing.T) {
		t.Parallel()

		c, frontends := prepareCache()
		frontends[2].EvictAll()

		for i, b := range c.buckets {
			if i == 2 {
				if len(b) != 0 {
					t.Fatal("bucket not empty")
				}
			} else {
				if len(b) == 0 {
					t.Fatal("bucket empty")
				}
			}
		}

		assertConsistency(t, c)
	})

	t.Run("selective", func(t *testing.T) {
		t.Parallel()

		assertSingleEviction := func(t *testing.T, c *Cache, frontend int) {
			for i := 0; i < 2; i++ {
				_, ok := c.buckets[0][recursiveData{
					Cache:    0,
					Frontend: 0,
					Key:      i,
				}]
				if i == 1 {
					if ok {
						t.Fatal("key not evicted")
					}
				} else {
					if !ok {
						t.Fatal("key evicted")
					}
				}
			}
		}

		t.Run("by key", func(t *testing.T) {
			t.Parallel()

			c, frontends := prepareCache()

			frontends[0].Evict(recursiveData{
				Cache:    0,
				Frontend: 0,
				// Record 1 is not included in other records and won't cause
				// recursive evictions
				Key: 1,
			})

			assertSingleEviction(t, c, 0)
			assertConsistency(t, c)
		})

		t.Run("by function", func(t *testing.T) {
			t.Parallel()

			c, frontends := prepareCache()

			err := frontends[0].EvictByFunc(func(k Key) (bool, error) {
				// Record 1 is not included in other records and won't cause
				// recursive evictions
				return k.(recursiveData).Key == 1, nil
			})
			if err != nil {
				t.Fatal(err)
			}

			assertSingleEviction(t, c, 1)
			assertConsistency(t, c)
		})

		t.Run("by function with error", func(t *testing.T) {
			t.Parallel()

			c, frontends := prepareCache()

			err := frontends[0].EvictByFunc(func(k Key) (bool, error) {
				return false, errSample
			})
			if err != errSample {
				t.Fatal("error expected")
			}
			assertConsistency(t, c)
		})
	})

	t.Run("max-memory-based", func(t *testing.T) {
		t.Parallel()

		c, frontends := prepareCache()
		c.memoryLimit = 1

		_, err := frontends[2].Get(recursiveData{
			Key: 1,
		})
		if err != nil {
			t.Fatal(err)
		}

		assertConsistency(t, c)
	})

	t.Run("same cache recursion", func(t *testing.T) {
		t.Parallel()

		c, frontends := prepareCache()
		c.memoryLimit = 1

		_, err := frontends[2].Get(recursiveData{})
		if err != nil {
			t.Fatal(err)
		}

		assertConsistency(t, c)
	})
}
