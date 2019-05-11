package recache

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// TODO: Test with concurrent caches and evictions.
// Must assert state of caches and linked lists after it.

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

										var w bytes.Buffer
										_, err := f.WriteTo(key, &w)
										if err != nil {
											t.Fatal(err)
										}
										assertEquals(t, unzip(t, &w),
											fmt.Sprintf(`"%s"`, key))
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

// Tests for correctness and data races with complicated intra-cache and
// inter-cache recursion
func TestGetRecordConcurentCachesWithRecursion(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup
	wg.Add(9 * 3)

	var (
		caches    [3]*Cache
		frontends [3][3]*Frontend
	)

	getter := func(k Key, rw *RecordWriter) (err error) {
		data := k.(recursiveData)

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

			err = rw.Include(k, frontends[k.Cache][k.Frontend])
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
		caches[i] = NewCache(0, 0)
		for j := 0; j < 3; j++ {
			frontends[i][j] = caches[i].NewFrontend(getter)
		}
	}

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

										var w bytes.Buffer
										_, err := frontends[cacheID][frontendID].
											WriteTo(key, &w)
										if err != nil {
											t.Fatal(err)
										}
										assertJSON(t,
											unzip(t, &w),
											recursiveStandard(cacheID,
												frontendID, keyID))
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

	wg.Wait()
}
