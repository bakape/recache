package benchmarks

import (
	"bytes"
	"fmt"
	"math/rand"
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
	// caching
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
	v.configVersion++
}

func (v *versionedCacher) resetContentCache() {
	v.contentVersion++
}

// Retrieve full page fetch key as string
func (v *versionedCacher) getPageKey() string {
	return fmt.Sprintf(
		"page:%s:%d:%d",
		v.benchmarkID,
		v.contentVersion,
		v.configVersion,
	)
}

func (v *versionedCacher) getVersionedCacher() *versionedCacher {
	return v
}

// cacher implementation capable of being partitioned into the header, middle
// and footer and have those cached separately
type partitionedCacher interface {
	// Return included versionedCacher instance
	getVersionedCacher() *versionedCacher

	// Get record from cache using `key` or generate a fresh record on cache
	// miss  using `gen`
	getOrGen(key string, gen func() ([]byte, error)) (out []byte, err error)
}

// Caches the individual page parts and builds the page from them
func pageFromPartitionedCache(c partitionedCacher) ([]byte, error) {
	vc := c.getVersionedCacher()

	return c.getOrGen(
		c.getVersionedCacher().getPageKey(),
		func() (out []byte, err error) {
			header, err := c.getOrGen(
				fmt.Sprintf("header:%s:%d", vc.benchmarkID, vc.configVersion),
				func() (out []byte, err error) {
					conf, err := generateConfiguration()
					if err != nil {
						return
					}
					return generateHeader(conf)
				},
			)
			if err != nil {
				return
			}

			middle, err := c.getOrGen(
				fmt.Sprintf(
					"middle:%s:%d:%d",
					vc.benchmarkID,
					vc.configVersion,
					vc.contentVersion,
				),
				func() (out []byte, err error) {
					conf, err := generateConfiguration()
					if err != nil {
						return
					}
					content, err := generateContent(conf)
					if err != nil {
						return
					}
					return generatePageMiddle(content, conf)
				},
			)
			if err != nil {
				return
			}

			footer, err := c.getOrGen(
				fmt.Sprintf("footer:%s:%d", vc.benchmarkID, vc.configVersion),
				func() (out []byte, err error) {
					conf, err := generateConfiguration()
					if err != nil {
						return
					}
					return generateFooter(conf)
				},
			)
			if err != nil {
				return
			}

			return bytes.Join([][]byte{header, footer, middle}, nil), nil
		},
	)
}
