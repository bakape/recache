package benchmarks

import (
	"os"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
)

// Memcached cacher implementation that always caches the whole page
type memcachedWholePage struct {
	versionedCacher

	// Connection to memcached
	conn *memcache.Client
}

func (m *memcachedWholePage) init() (err error) {
	err = m.versionedCacher.init()
	if err != nil {
		return
	}

	addr := os.Getenv("MEMCACHED_ADDRESS")
	if addr == "" {
		addr = "localhost:11211"
	}
	m.conn = memcache.New(addr)
	return
}

func (m *memcachedWholePage) getPage() (out []byte, err error) {
	return m.getOrGen(m.getPageKey(), generatePage)
}

// Get record from cache using `key` or generate a fresh record on cache miss
// using `gen`
func (m *memcachedWholePage) getOrGen(key string, gen func() ([]byte, error)) (
	out []byte, err error,
) {
	item, err := m.conn.Get(key)
	switch err {
	case nil:
		out = item.Value
	case memcache.ErrCacheMiss:
		out, err = gen()
		if err != nil {
			return
		}
		err = m.conn.Set(&memcache.Item{
			Key:   key,
			Value: out,
		})
	}
	return
}

// Benchmark memcached with whole page caching
func BenchmarkMemcachedWholePage(b *testing.B) {
	runBenchmark(b, &memcachedWholePage{})
}

// Memcached cacher implementation that caches individual page parts
type memcachedPartitioned struct {
	memcachedWholePage
}

func (m *memcachedPartitioned) getPage() ([]byte, error) {
	return pageFromPartitionedCache(m)
}

// Benchmark memcached with individual page part caching
func BenchmarkMemcachedPartitioned(b *testing.B) {
	runBenchmark(b, &memcachedPartitioned{})
}
