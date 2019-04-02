package recache

import (
	"crypto/sha1"
	"io"
	"sync"
	"time"
)

// Unified storage for cached records with specific eviction parameters
type Cache struct {
	mu          sync.Mutex
	lruLimit    time.Duration
	memoryLimit int
	idCounter   uint

	// record must be a pointer, because it contains a mutex
	buckets    map[uint]map[Key]*record
	memoryUsed int

	// TODO: Doubly linked list with first and last node hot access for keeping
	// LRU order
}

// Data storage unit in the cache. Linked to a single Key on a Frontend.
type record struct {
	wg   sync.WaitGroup
	hash [sha1.Size]byte
	data []component

	// Time of most recent use of record.
	//
	// Must only be accessed with a lock on the parent cache.
	lru time.Time

	// Memory used by the record, not counting any contained references or
	// storage infrastructure metadata.
	//
	// Must only be accessed with a lock on the parent cache.
	memoryUsed int

	// Error that occurred during initial data population. This will also be
	// returned on any readers that are concurrent with population.
	// Might cause error duplication, but better than returning nothing on
	// concurrent reads.
	populationError error
}

// Create new cache with specified memory and LRU eviction limits. After either
// of these are exceeded, the least recently used cache records will be evicted,
// until the requirements are satisfied again. Note that this eviction is
// eventual and not immediate for optimisation purposes.
//
// Pass in zero values to ignore either or both eviction limit.
func NewCache(memoryLimit uint, lruLimit time.Duration) *Cache {
	return &Cache{
		memoryLimit: int(memoryLimit),
		lruLimit:    lruLimit,
		buckets:     make(map[uint]map[Key]*record),
	}
}

// Value used to store entries in the cache. Must be a type suitable for being a
// key in a Go map.
type Key interface{}

// Generates fresh cache records for the given key. These records wil be stored
// by the cache engine and must not be modified after Getter returns.
//
// Getter must be thread-safe.
type Getter func(Key) (RecordWriter, error)

// A frontend for accessing the cache contents
type Frontend struct {
	id     uint
	cache  *Cache
	getter Getter
}

// Create new Frontend for accessing the cache.
//
// get() will be used for generating fresh cache records for the given key by
// the cache engine. These records wil be stored by the cache engine and must
// not be modified after get() returns. get() must be thread-safe.
func (c *Cache) NewFrontend(get Getter) *Frontend {
	f := &Frontend{
		id:     c.idCounter,
		cache:  c,
		getter: get,
	}
	c.buckets[c.idCounter] = make(map[Key]*record)
	c.idCounter++
	return f
}

// Retrieve or generate data by key and write it to w
func (f *Frontend) WriteTo(k Key, w io.Writer) (n int64, err error) {
	f.cache.mu.Lock()

	rec, ok := f.cache.buckets[f.id][k]
	if !ok {
		rec = &record{}
		rec.wg.Add(1) // Block all reads until population
		f.cache.buckets[f.id][k] = rec
		// TODO: Insert record to front of LRU linked list
	} else {
		// TODO: Move record to front of LRU linked list
	}
	rec.lru = time.Now()

	// TODO: Attempt to evict up to the last 2 records due to LRU or memory
	// constraints

	f.cache.mu.Unlock()

	if !ok {
		err = f.populate(k, rec)
		if err != nil {
			// Propagate error to any concurrent readers
			rec.populationError = err

			// TODO: Evict from cache. Can be eventual, if needed.
		}
		rec.wg.Done() // Also unblock any concurrent readers, even on error
		if err != nil {
			return
		}
	}

	// Prevents a record being read concurrently before it is populated.
	// A record is immutable after initial population and this will not block
	// after it.
	rec.wg.Wait()

	var m int64
	for _, c := range rec.data {
		m, err = c.WriteTo(w)
		if err != nil {
			return
		}
		n += m
	}
	return
}

// TODO: Method for writing to http.ResponseWriter with ETag handling and Gzip
// setting

// Populates a record using the registered Getter
func (f *Frontend) populate(k Key, rec *record) (err error) {
	rw, err := f.getter(k)
	if err != nil {
		return
	}

	// Flush any unclosed gzip buffers
	err = rw.flushPending(true)
	if err != nil {
		return
	}

	rec.data = rw.data
	for _, d := range rec.data {
		rec.memoryUsed += d.Size()
		for i, b := range d.Hash() {
			rec.hash[i] += b
		}
	}

	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()

	// Check record still in cache before size assignment. It might have been
	// evicted.
	_, ok := f.cache.buckets[f.id][k]
	if ok {
		f.cache.memoryUsed += rec.memoryUsed
	}

	return
}
