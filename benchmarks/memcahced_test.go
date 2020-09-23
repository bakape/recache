package benchmarks

import (
	"os"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
)

// TODO: whole page
// TODO: part caching w/ concurrent requests

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
	k := m.getContentKey()
	item, err := m.conn.Get(k)
	switch err {
	case nil:
		out = item.Value
	case memcache.ErrCacheMiss:
		out, err = generatePage()
		if err != nil {
			return
		}
		err = m.conn.Set(&memcache.Item{
			Key:   k,
			Value: out,
		})
	}
	return
}

// Benchmark memcached with whole page caching
func BenchmarkMemcachedWholePage(b *testing.B) {
	runBenchmark(b, &memcachedWholePage{})
}
