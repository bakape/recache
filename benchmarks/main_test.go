package benchmarks

import (
	"testing"
)

// Operations to be benchmarked for the cache
type cacher interface {
	// Run any required initialization procedures
	init() error

	// Force the next fetch of the configuration to not use a cached value
	resetConfigurationCache()

	// Force the next fetch of the content to not use a cached value
	resetContentCache()

	// Retrieve a fully generated page
	getPage() ([]byte, error)
}

// Runs benchmark suite on `cacher`
func runBenchmark(b *testing.B, c cacher) {
	err := c.init()
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i != 0 {
			if i%10 == 0 {
				c.resetContentCache()
			}
			if i%100 == 0 {
				c.resetConfigurationCache()
			}
		}
		_, err = c.getPage()
		if err != nil {
			b.Fatal(err)
		}
	}
}
