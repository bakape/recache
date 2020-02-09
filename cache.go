package recache

import (
	"compress/gzip"
	"sync"
	"time"
)

var (
	// Registry of all created caches. Require cacheMu to be held for access.
	cacheMu sync.RWMutex
	caches  = make([]*Cache, 1)
)

// Get cache from registry by ID
func getCache(id int) *Cache {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return caches[id]
}

// Unified storage for cached records with specific eviction parameters
type Cache struct {
	// Locks for all cache access, excluding the contained records
	mu sync.Mutex

	// Global ID of cache
	id int

	// Total used memory and limit
	memoryLimit, memoryUsed int

	// Linked list and limit for quick LRU data order modifications and lookup
	lruLimit time.Duration
	lruList  linkedList

	// Storage for each individual frontend
	frontends []map[Key]recordWithMeta
}

// Options for new cache creation
type CacheOptions struct {
	// Maximum amount of memory the cache can consume without forcing eviction
	MemoryLimit uint

	// Maximum last use time of record without forcing eviction
	LRULimit time.Duration
}

// Create new cache with specified memory and LRU eviction limits. After either
// of these are exceeded, the least recently used cache records will be evicted,
// until the requirements are satisfied again. Note that this eviction is
// eventual and not immediate for optimisation purposes.
//
// Pass in zero values to ignore either or both eviction limits.
func NewCache(opts CacheOptions) (c *Cache) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	c = &Cache{
		id:          len(caches),
		memoryLimit: int(opts.MemoryLimit),
		lruLimit:    opts.LRULimit,
	}
	caches = append(caches, c)
	return c
}

// Options for creating a new cache frontend
type FrontendOptions struct {
	// Will be used for generating fresh cache records for the given key by
	// the cache engine. These records will be stored by the cache engine and
	// must not be modified after Get() returns. Get() must be thread-safe.
	Get Getter

	// Level of compression to use for storing records.
	// Defaults to gzip.DefaultCompression.
	Level *int
}

// Create new Frontend for accessing the cache.
// A Frontend must only be created using this method.
func (c *Cache) NewFrontend(opts FrontendOptions) *Frontend {
	c.mu.Lock()
	defer c.mu.Unlock()

	f := &Frontend{
		id:     len(c.frontends),
		cache:  c,
		getter: opts.Get,
	}
	if opts.Level != nil {
		f.level = *opts.Level
	} else {
		f.level = gzip.DefaultCompression
	}

	c.frontends = append(c.frontends, make(map[Key]recordWithMeta))
	return f
}

// Get or create a new record in the cache.
// fresh=true, if record is freshly created and requires population.
func (c *Cache) getRecord(loc recordLocation) (rec *record, fresh bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	recWithMeta, ok := c.record(loc)
	if !ok {
		recWithMeta = recordWithMeta{
			node: c.lruList.Prepend(loc),
			rec:  new(record),
		}
		recWithMeta.rec.semaphore.Init() // Block all reads until population
	} else {
		c.lruList.MoveToFront(recWithMeta.node)
	}
	now := time.Now()
	recWithMeta.lastUsed = now
	c.frontends[loc.frontend][loc.key] = recWithMeta

	// Attempt to evict up to the last 2 records due to LRU or memory
	// constraints. Doing this here simplifies locking patterns while retaining
	// good enough eviction eventuality.
	for i := 0; i < 2; i++ {
		last, ok := c.lruList.Last()
		if !ok {
			break
		}
		if c.memoryLimit != 0 && c.memoryUsed > c.memoryLimit {
			c.evictWithLock(last, 0)
			continue
		}
		if c.lruLimit != 0 {
			lruRec, ok := c.record(last)
			if !ok {
				panic("linked list points to evicted record")
			}
			if lruRec.lastUsed.Add(c.lruLimit).Before(now) {
				c.evictWithLock(last, 0)
				continue
			}
		}
		break
	}

	return recWithMeta.rec, !ok
}

// Shorthand for retrieving record by its location.
//
// Requires lock on c.mu.
func (c *Cache) record(loc recordLocation) (recordWithMeta, bool) {
	rec, ok := c.frontends[loc.frontend][loc.key]
	return rec, ok
}

// Set record used memory
func (c *Cache) setUsedMemory(src *record, loc recordLocation, memoryUsed int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// It is possible for the record to be evicted and possibly then a new
	// record to be inserted into the same spot while the current record is
	// being populated. Need to assert the former is still in the cache.
	//
	// This is needed here, because the used memory value of the record directly
	// effects the total used memory of the cache w/o recording what parts of
	// the cache contribute what amount to the total.
	//
	// All other cases of such possible concurrent evictions and overridden
	// inclusions will simply NOP on their respective operations.
	rec, ok := c.record(loc)
	if !ok || rec.rec != src {
		return
	}
	rec.memoryUsed = memoryUsed
	c.frontends[loc.frontend][loc.key] = rec
	c.memoryUsed += memoryUsed
}

// Register a record as being used in another record
func registerInclusion(parent, child intercacheRecordLocation) {
	c := getCache(child.cache)
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.record(child.recordLocation)
	if !ok {
		return // Already evicted
	}
	rec.includedIn = append(rec.includedIn, parent)
	c.frontends[child.frontend][child.key] = rec
}

// Make copy of frontend keys to prevent itterator invalidation.
// Requires lock on c.mu.
func (c *Cache) keys(frontend int) []Key {
	m := c.frontends[frontend]
	keys := make([]Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
