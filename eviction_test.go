package recache

import (
	"sync"
	"testing"
	"time"
)

func TestEviction(t *testing.T) {
	t.Parallel()

	prepareCache := func(memoryLimit uint) (*Cache, [3]*Frontend) {
		var wg sync.WaitGroup
		caches, frontends := prepareRecursion(memoryLimit, 0)
		testRecursion(t, &wg, caches, frontends)
		wg.Wait()

		assertConsistency(t, caches[:]...)

		return caches[0], frontends[0]
	}

	t.Run("max-memory-based", func(t *testing.T) {
		t.Parallel()

		c, _ := prepareCache(1)
		assertConsistency(t, c)

		recs := 0
		for _, f := range c.frontends {
			recs += len(f)
		}
		// Because just inserted keys are never evicted, there will be one key
		// per frontend
		if recs > 3 {
			t.Fatalf("stored record count not minimal: %d", recs)
		}
	})

	cases := [...]struct {
		name  string
		timer time.Duration
	}{
		{"immediate", 0},
		{"scheduled", time.Millisecond * 10},
	}

	for i := range cases {
		opts := cases[i]
		t.Run(opts.name, func(t *testing.T) {
			t.Parallel()

			// Wait for cleanup, if eviction is not immediate
			await := func() {
				if opts.timer != 0 {
					time.Sleep(time.Second * 2)
				}
			}

			t.Run("cache", func(t *testing.T) {
				t.Parallel()

				c, _ := prepareCache(0)

				c.EvictAll(opts.timer)
				await()
				assertConsistency(t, c)

				if c.memoryUsed != 0 {
					t.Fatal("memory used not 0")
				}
				for _, b := range c.frontends {
					if len(b) != 0 {
						t.Fatal("bucket not empty")
					}
				}
			})

			t.Run("frontend", func(t *testing.T) {
				t.Parallel()

				c, frontends := prepareCache(0)
				frontends[2].EvictAll(opts.timer)
				await()

				t.Run("buckets", func(t *testing.T) {
					c.mu.Lock()
					defer c.mu.Unlock()

					for i, b := range c.frontends {
						if i == 2 {
							if len(b) != 0 {
								c.mu.Unlock()
								t.Fatal("bucket not empty")
							}
						} else {

							if len(b) == 0 {
								t.Fatal("bucket empty")
							}
						}
					}
				})

				t.Run("consistency", func(t *testing.T) {
					assertConsistency(t, c)
				})
			})

			t.Run("selective", func(t *testing.T) {
				t.Parallel()

				assertSingleEviction := func(
					t *testing.T,
					c *Cache,
					frontend int,
				) {
					c.mu.Lock()
					defer c.mu.Unlock()

					for i := 0; i < 2; i++ {
						_, ok := c.frontends[0][recursiveData{
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

					c, frontends := prepareCache(0)

					frontends[0].Evict(opts.timer, recursiveData{
						Cache:    0,
						Frontend: 0,
						// Record 1 is not included in other records and won't cause
						// recursive evictions
						Key: 1,
					})
					await()

					assertSingleEviction(t, c, 0)
					assertConsistency(t, c)
				})

				t.Run("by function", func(t *testing.T) {
					t.Parallel()

					c, frontends := prepareCache(0)

					err := frontends[0].EvictByFunc(
						opts.timer,
						func(k Key) (bool, error) {
							// Record 1 is not included in other records and
							// won't cause recursive evictions
							return k.(recursiveData).Key == 1, nil
						},
					)
					if err != nil {
						t.Fatal(err)
					}
					await()

					assertSingleEviction(t, c, 1)
					assertConsistency(t, c)
				})

				t.Run("by function with error", func(t *testing.T) {
					t.Parallel()

					c, frontends := prepareCache(0)

					err := frontends[0].EvictByFunc(
						opts.timer,
						func(k Key) (bool, error) {
							return false, errSample
						},
					)
					if err != errSample {
						t.Fatal("error expected")
					}
					await()
					assertConsistency(t, c)
				})
			})
		})
	}
}
