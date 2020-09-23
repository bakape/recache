package benchmarks

import (
	"bytes"
	"testing"

	"github.com/bakape/recache/v5"
)

// redache cacher implementation
type recacheCacher struct {
	cache                                               *recache.Cache
	configs, content, footer, header, pageContent, page *recache.Frontend
}

func (c *recacheCacher) init() error {
	c.cache = recache.NewCache(recache.CacheOptions{})

	c.configs = c.cache.NewFrontend(
		func(k recache.Key, rw *recache.RecordWriter) (err error) {
			b, err := generateConfiguration()
			if err != nil {
				return
			}
			_, err = rw.Write(b)
			if err != nil {
				return
			}
			return
		},
	)

	includeConfig := func(gen func([]byte) ([]byte, error),
	) func(k recache.Key, rw *recache.RecordWriter) error {
		return func(k recache.Key, rw *recache.RecordWriter) (err error) {
			err = rw.Include(c.configs, nil)
			if err != nil {
				return
			}
			b, err := gen(nil)
			if err != nil {
				return
			}
			_, err = rw.Write(b)
			return
		}
	}
	c.header = c.cache.NewFrontend(includeConfig(generateHeader))
	c.footer = c.cache.NewFrontend(includeConfig(generateFooter))
	c.content = c.cache.NewFrontend(includeConfig(generateContent))

	c.pageContent = c.cache.NewFrontend(
		func(k recache.Key, rw *recache.RecordWriter) (err error) {
			for _, f := range [...]*recache.Frontend{
				c.content,
				c.configs,
			} {
				err = rw.Include(f, nil)
				if err != nil {
					return
				}
			}

			b, err := generatePageMiddle(nil, nil)
			if err != nil {
				return
			}
			_, err = rw.Write(b)
			return
		},
	)

	c.page = c.cache.NewFrontend(
		func(k recache.Key, rw *recache.RecordWriter) (err error) {
			for _, f := range [...]*recache.Frontend{
				c.footer,
				c.pageContent,
				c.header,
			} {
				err = rw.Include(f, nil)
				if err != nil {
					return
				}
			}
			return
		},
	)

	return nil
}

func (c *recacheCacher) resetConfigurationCache() {
	c.configs.EvictAll(0)
}

func (c *recacheCacher) resetContentCache() {
	c.content.EvictAll(0)
}

func (c *recacheCacher) getPage() (out []byte, err error) {
	var w bytes.Buffer
	r, err := c.page.Get(nil)
	if err != nil {
		return
	}
	_, err = r.WriteTo(&w)
	if err != nil {
		return
	}
	return w.Bytes(), nil
}

// Benchmark Redis with whole page caching
func BenchmarkRecache(b *testing.B) {
	runBenchmark(b, &recacheCacher{})
}
