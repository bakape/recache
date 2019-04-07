package recache

import (
	"sync"
	"time"
)

var (
	// Registry of all created caches. Require cacheMU to be held for access.
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
func (c *Cache) getRecord(frontend uint, key Key) (rec *record, fresh bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	recWithMeta, ok := c.buckets[frontend][key]
	if !ok {
		recWithMeta = recordWithMeta{
			node: c.lruList.Prepend(recordLocation{
				frontend: frontend,
				key:      key,
			}),
			rec: new(record),
		}
		recWithMeta.rec.wg.Add(1) // Block all reads until population
	} else {
		c.lruList.MoveToFront(recWithMeta.node)
	}
	now := time.Now()
	recWithMeta.lru = now
	c.buckets[frontend][key] = recWithMeta

	// Attempt to evict up to the last 2 records due to LRU or memory
	// constraints. Doing this here simplifies locking patterns while retaining
	// good enough eviction eventuality.
	for i := 0; i < 2; i++ {
		last, ok := c.lruList.Last()
		if !ok {
			break
		}
		if c.memoryLimit != 0 && c.memoryUsed > c.memoryLimit {
			c.evictWithLock(last.frontend, last.key)
			continue
		}
		if c.lruLimit != 0 {
			lru := c.buckets[last.frontend][last.key].lru
			if lru.Add(c.lruLimit).After(now) {
				c.evictWithLock(last.frontend, last.key)
				continue
			}
		}
		break
	}

	return recWithMeta.rec, !ok
}

// Set record used memory
func (c *Cache) setUsedMemory(loc recordLocation, memoryUsed int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.buckets[loc.frontend][loc.key]
	if ok {
		return // Already evicted
	}
	rec.memoryUsed = memoryUsed
	c.memoryUsed += memoryUsed
}

// Register a record as being used in another record
func registerInclusion(parent, child intercacheRecordLocation) {
	c := getCache(child.cache)
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.buckets[child.frontend][child.key]
	if !ok {
		return // Already evicted
	}
	rec.includedIn = append(rec.includedIn, parent)
}

// Evict record from cache
func evict(cache, frontend uint, key Key) {
	getCache(cache).evict(frontend, key)
}

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

	for _, ch := range rec.includedIn {
		if ch.cache == c.id {
			// Hot path to reduce lock contention
			c.evictWithLock(ch.frontend, ch.key)
		} else {
			// Separate goroutine to prevent lock intersection
			go evict(ch.cache, ch.frontend, ch.key)
		}
	}
}

// TODO: Evict all method
