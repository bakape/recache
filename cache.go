package recache

import (
	"sync"
	"time"
)

var (
	// Registry of all created caches. Require cacheMu to be held for access.
	cacheMu        sync.RWMutex
	cacheIDCounter uint
	caches         = make(map[uint]*Cache)
)

// Get cache from registry by ID
func getCache(id uint) *Cache {
	cacheMu.RLock()
	defer cacheMu.RUnlock()
	return caches[id]
}

// Unified storage for cached records with specific eviction parameters
type Cache struct {
	// Locks for all cache access, excluding the contained records
	mu sync.Mutex

	// Global ID of cache
	id uint

	// Total used memory and limit
	memoryLimit, memoryUsed int

	// Linked list and limit for quick LRU data order modifications and lookup
	lruLimit time.Duration
	lruList  linkedList

	// Storage for each individual frontend
	frontendIDCounter uint
	buckets           map[uint]map[Key]recordWithMeta
}

// Create new cache with specified memory and LRU eviction limits. After either
// of these are exceeded, the least recently used cache records will be evicted,
// until the requirements are satisfied again. Note that this eviction is
// eventual and not immediate for optimisation purposes.
//
// Pass in zero values to ignore either or both eviction limits.
func NewCache(memoryLimit uint, lruLimit time.Duration) (c *Cache) {
	cacheMu.Lock()
	defer cacheMu.Unlock()

	c = &Cache{
		id:          cacheIDCounter,
		memoryLimit: int(memoryLimit),
		lruLimit:    lruLimit,
		buckets:     make(map[uint]map[Key]recordWithMeta),
	}
	caches[cacheIDCounter] = c
	cacheIDCounter++
	return c
}

// Create new Frontend for accessing the cache.
// A Frontend must only be created using this method.
//
// get() will be used for generating fresh cache records for the given key by
// the cache engine. These records will be stored by the cache engine and must
// not be modified after get() returns. get() must be thread-safe.
func (c *Cache) NewFrontend(get Getter) *Frontend {
	c.mu.Lock()
	defer c.mu.Unlock()

	f := &Frontend{
		id:     c.frontendIDCounter,
		cache:  c,
		getter: get,
	}
	c.buckets[c.frontendIDCounter] = make(map[Key]recordWithMeta)
	c.frontendIDCounter++
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
	c.buckets[loc.frontend][loc.key] = recWithMeta

	// Attempt to evict up to the last 2 records due to LRU or memory
	// constraints. Doing this here simplifies locking patterns while retaining
	// good enough eviction eventuality.
	for i := 0; i < 2; i++ {
		last, ok := c.lruList.Last()
		if !ok {
			break
		}
		if c.memoryLimit != 0 && c.memoryUsed > c.memoryLimit {
			c.evictWithLock(last)
			continue
		}
		if c.lruLimit != 0 {
			lruRec, ok := c.record(last)
			if !ok {
				panic("linked list points to evicted record")
			}
			if lruRec.lastUsed.Add(c.lruLimit).Before(now) {
				c.evictWithLock(last)
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
	rec, ok := c.buckets[loc.frontend][loc.key]
	return rec, ok
}

// Set record used memory
func (c *Cache) setUsedMemory(src *record, loc recordLocation, memoryUsed int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// It is possible for the record to evicted and possibly then a new record
	// to inserted into the same spot while the current is populated.
	// Need to assert the former is still in the cache.
	//
	// This is needed here, because the used memory value of the record directly
	// effects the total used memory of the cache w/o recording what parts of
	// the cache contribute what amount to the total.
	//
	// All other cases of such possible concurrent evictions and  override
	// inclusions will simply NOP on their respective operations.
	rec, ok := c.record(loc)
	if !ok || rec.rec != src {
		return
	}
	rec.memoryUsed = memoryUsed
	c.buckets[loc.frontend][loc.key] = rec
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
}

// Evict record from cache
func evict(loc intercacheRecordLocation) {
	getCache(loc.cache).evict(loc.recordLocation)
}

// Evict record from cache
func (c *Cache) evict(loc recordLocation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictWithLock(loc)
}

// Evict record from cache. Requires lock on c.mu.
func (c *Cache) evictWithLock(loc recordLocation) {
	rec, ok := c.record(loc)
	if !ok {
		return
	}
	delete(c.buckets[loc.frontend], loc.key)
	c.lruList.Remove(rec.node)
	c.memoryUsed -= rec.memoryUsed

	for _, ch := range rec.includedIn {
		if ch.cache == c.id {
			// Hot path to reduce lock contention
			c.evictWithLock(ch.recordLocation)
		} else {
			// Separate goroutine to prevent lock intersection
			go evict(ch)
		}
	}
}

// The all keys of specific frontend.
func (c *Cache) evictFrontend(frontend uint) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictFrontendWithLock(frontend)
}

// The all keys of specific frontend. Requires lock on c.mu.
func (c *Cache) evictFrontendWithLock(frontend uint) {
	for _, k := range c.keys(frontend) {
		c.evictWithLock(recordLocation{frontend, k})
	}
}

// Make copy of frontend keys to prevent itterator invalidation.
// Requires lock on c.mu.
func (c *Cache) keys(frontend uint) []Key {
	m := c.buckets[frontend]
	keys := make([]Key, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Evict all records from cache
func (c *Cache) EvictAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := uint(0); i < c.frontendIDCounter; i++ {
		c.evictFrontendWithLock(i)
	}
}

// Evict keys from frontend using matcher function fn.
// fn returns true, if a key must be evicted.
func (c *Cache) evictByFunc(frontend uint, fn func(Key) (bool, error),
) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		b     = c.buckets[frontend]
		evict bool
	)
	for _, k := range c.keys(frontend) {
		// Check, if key not already evicted by recursive eviction, to reduce
		// potentially expensive matcher function calls
		if _, ok := b[k]; !ok {
			continue
		}

		evict, err = fn(k)
		if err != nil {
			return
		}
		if evict {
			c.evictWithLock(recordLocation{frontend, k})
		}
	}

	return
}
