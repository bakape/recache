package benchmarks

import (
	"context"
	"os"
	"testing"

	"github.com/go-redis/redis/v8"
)

// TODO: whole page
// TODO: part caching w/ concurrent requests

// Redis cacher implementation that always caches the whole page
type redisWholePage struct {
	versionedCacher

	// Connection to memcached
	conn *redis.Client
}

func (m *redisWholePage) init() (err error) {
	err = m.versionedCacher.init()
	if err != nil {
		return
	}

	addr := os.Getenv("REDIS_ADDRESS")
	if addr == "" {
		addr = "localhost:6379"
	}
	m.conn = redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return
}

func (m *redisWholePage) getPage() (out []byte, err error) {
	k := m.getContentKey()
	item, err := m.conn.Get(context.Background(), k).Result()
	switch err {
	case nil:
		out = []byte(item)
	case redis.Nil:
		out, err = generatePage()
		if err != nil {
			return
		}
		_, err = m.conn.Set(context.Background(), k, string(out), 0).Result()
	}
	return
}

// Benchmark Redis with whole page caching
func BenchmarkRedisWholePage(b *testing.B) {
	runBenchmark(b, &redisWholePage{})
}
