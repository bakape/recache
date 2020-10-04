package benchmarks

import (
	"bytes"
	"testing"

	"github.com/bakape/recache/v5"
)

// Common functionality of both whole page and partitioned recache benchmarks
type recacheCommon struct {
	cache *recache.Cache
	page  *recache.Frontend
}

func (c *recacheCommon) getPage() (out []byte, err error) {
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

// recache cacher implementation that caches individual page parts
type recachePartitioned struct {
	recacheCommon
	configs, content, footer, header, pageContent *recache.Frontend
}

func (c *recachePartitioned) init() error {
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

func (c *recachePartitioned) resetConfigurationCache() {
	c.configs.EvictAll(0)
}

func (c *recachePartitioned) resetContentCache() {
	c.content.EvictAll(0)
}

// Benchmark recache with whole page caching
func BenchmarkRecachePartititoned(b *testing.B) {
	runBenchmark(b, &recachePartitioned{})
}

// recache cacher implementation that caches the entire page
type recacheWholePage struct {
	recacheCommon
}

func (c *recacheWholePage) init() error {
	c.cache = recache.NewCache(recache.CacheOptions{})

	c.page = c.cache.NewFrontend(
		func(_ recache.Key, rw *recache.RecordWriter) (err error) {
			b, err := generatePage()
			if err != nil {
				return
			}
			_, err = rw.Write(b)
			return
		},
	)

	return nil
}

func (c *recacheWholePage) resetConfigurationCache() {
	c.page.EvictAll(0)
}

func (c *recacheWholePage) resetContentCache() {
	c.resetConfigurationCache()
}

func BenchmarkRecacheWholePage(b *testing.B) {
	runBenchmark(b, &recacheWholePage{})
}
