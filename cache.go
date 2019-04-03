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

	lruList linkedList
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

// Data storage unit in the cache. Linked to a single Key on a Frontend.
type record struct {
	wg sync.WaitGroup

	// Contained data and its SHA1 hash
	data []component
	hash [sha1.Size]byte

	// Location of record in cache. Needed for reverse lookup.
	frontend uint
	key      Key

	// Error that occurred during initial data population. This will also be
	// returned on any readers that are concurrent with population.
	// Might cause error duplication, but better than returning nothing on
	// concurrent reads.
	populationError error

	// Time of most recent use of record.
	//
	// Must only be accessed with a held lock on the parent cache.
	lru time.Time

	// Memory used by the record, not counting any contained references or
	// storage infrastructure metadata.
	//
	// Must only be accessed with a held lock on the parent cache.
	memoryUsed int

	// Keep pointer to node in LRU list, so we can modify the list without
	// itterating it to find the this record's node.
	//
	// Must only be accessed with a held lock on the parent cache.
	node *node
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

// TODO: Explicit exact key match and matcher function eviction

// Evict record from cache
func (c *Cache) evict(frontend uint, key Key) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictWithLock(frontend, key)
}

// Evict record from cache. Requires lock on c.mu.
func (c *Cache) evictWithLock(frontend uint, key Key) {
	rec, ok := c.buckets[frontend][key]
	if !ok {
		return
	}
	delete(c.buckets[frontend], key)
	c.lruList.Remove(rec.node)
	c.memoryUsed -= rec.memoryUsed
}

// Retrieve or generate data by key and write it to w
func (f *Frontend) WriteTo(k Key, w io.Writer) (n int64, err error) {
	rec, ok := f.getRecord(k)
	if !ok {
		err = f.populate(k, rec)
		if err != nil {
			// Propagate error to any concurrent readers
			rec.populationError = err

			f.cache.evict(rec.frontend, rec.key)
		}

		// Also unblock any concurrent readers, even on error.
		// Having it here also protects from data race on rec.populationError.
		rec.wg.Done()

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

// Retrieve existing record or create a new one and insert it into the cache
func (f *Frontend) getRecord(k Key) (rec *record, ok bool) {
	f.cache.mu.Lock()
	defer f.cache.mu.Unlock()

	rec, ok = f.cache.buckets[f.id][k]
	if !ok {
		rec = &record{
			frontend: f.id,
			key:      k,
		}
		rec.wg.Add(1) // Block all reads until population
		f.cache.buckets[f.id][k] = rec
		rec.node = f.cache.lruList.Prepend(rec)
	} else {
		f.cache.lruList.MoveToFront(rec.node)
	}
	now := time.Now()
	rec.lru = now

	// Attempt to evict up to the last 2 records due to LRU or memory
	// constraints. Doing this here simplifies locking patterns while retaining
	// good enough eviction eventuality.
	for i := 0; i < 2; i++ {
		last := f.cache.lruList.Last()
		if rec == nil || last == rec {
			break
		}
		if lim := f.cache.memoryLimit; lim != 0 && f.cache.memoryUsed > lim {
			f.cache.evictWithLock(last.frontend, last.key)
			continue
		}
		if lim := f.cache.lruLimit; lim != 0 && last.lru.Add(lim).After(now) {
			f.cache.evictWithLock(last.frontend, last.key)
		}
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
