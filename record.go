package recache

import (
	"crypto/sha1"
	"io"
	"sync"
	"time"
)

// Describes record location in a cache
type recordLocation struct {
	frontend uint
	key      Key
}

// Describes record location across all caches
type intercacheRecordLocation struct {
	cache uint
	recordLocation
}

// Kept separate from the record to localize locking regions
type recordWithMeta struct {
	// Memory used by the record, not counting any contained references or
	// storage infrastructure metadata.
	memoryUsed int

	// Time of most recent use of record
	lru time.Time

	// Keep pointer to node in LRU list, so we can modify the list without
	// itterating it to find this record's node.
	node *node

	// Records that include this record and should be evicted on this record's
	// eviction
	includedIn []intercacheRecordLocation

	// The record itself. Has a separate lock and can be modified without the
	// lock on the cache mutex held.
	//
	// Record must be a pointer, because it contains mutexes.
	rec *record
}

// Data storage unit in the cache. Linked to a single Key on a Frontend.
type record struct {
	wg sync.WaitGroup

	// Contained data and its SHA1 hash
	data []component
	hash [sha1.Size]byte

	// Error that occurred during initial data population. This will also be
	// returned on any readers that are concurrent with population.
	// Might cause error duplication, but better than returning nothing on
	// concurrent reads.
	populationError error
}

func (r record) WriteTo(w io.Writer) (n int64, err error) {
	var m int64
	for _, c := range r.data {
		m, err = c.WriteTo(w)
		if err != nil {
			return
		}
		n += m
	}
	return
}
