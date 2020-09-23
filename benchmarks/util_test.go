package benchmarks

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	uuid "github.com/satori/go.uuid"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Maintains version counters to handle eviction
// Common parts of memcached and redis cacher implementations.
type versionedCacher struct {
	// Version counters for the configuration and content to use as keys for
	// caching. Must only be accessed using atomic operations.
	configVersion, contentVersion uint64

	// Unique UUID each time to make sure caches don't overlap between
	// benchmarks
	benchmarkID uuid.UUID
}

func (v *versionedCacher) init() (err error) {
	_, err = rand.Read(v.benchmarkID[:])
	return
}

func (v *versionedCacher) resetConfigurationCache() {
	atomic.AddUint64(&v.configVersion, 1)
}

func (v *versionedCacher) resetContentCache() {
	atomic.AddUint64(&v.contentVersion, 1)
}

// Atomically retrieve full page fetch key as string
func (v *versionedCacher) getPageKey() string {
	return fmt.Sprintf(
		"page:%d:%d",
		atomic.LoadUint64(&v.contentVersion),
		atomic.LoadUint64(&v.configVersion),
	)
}

// Atomically retrieve configuration version key
func (v *versionedCacher) getConfigKey() string {
	return fmt.Sprintf(
		"config:%s:%d",
		v.benchmarkID,
		atomic.LoadUint64(&v.configVersion),
	)
}

// Atomically retrieve content version

func (v *versionedCacher) getContentKey() string {
	return fmt.Sprintf(
		"content:%s:%d",
		v.benchmarkID,
		atomic.LoadUint64(&v.contentVersion),
	)
}
