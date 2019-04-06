package recache

import (
	"sync"
	"time"
)

var (
	// Lock that must be held for any access to the Cache instances themselves.
	// Need not be held during record population.
	//
	// Stop the world garbage collection is needed to avoid conflicts with
	// child eviction propagating to their parents as those.
	//
	// This is even more crucial with multiple caches recursing into other
	// caches. Greatly simplifies locking patterns.
	cacheMu sync.RWMutex

	// Registry of all created caches. Require cacheMU to be held for access.
	cacheIDCounter uint
	caches         = make(map[uint]*Cache)
)

// Locks/unlocks both the global and the internal cache mutexes in one call
type jointLock struct {
	mu sync.Mutex
}

func (j *jointLock) lock() {
	cacheMu.RLock()
	j.mu.Lock()
}

func (j *jointLock) unlock() {
	cacheMu.RUnlock()
	j.mu.Unlock()
}

// Unified storage for cached records with specific eviction parameters
type Cache struct {
	// Locks for all cache access, excluding the contained records
	jointLock

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
	c.lock()
	defer c.unlock()

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
	c.lock()
	defer c.unlock()

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
	recWithMeta.lru = time.Now()
	c.buckets[frontend][key] = recWithMeta

	return recWithMeta.rec, !ok
}

// Set record used memory
func (c *Cache) setUsedMemory(loc recordLocation, memoryUsed int) {
	c.lock()
	defer c.unlock()

	rec, ok := c.buckets[loc.frontend][loc.key]
	if ok {
		return // Already evicted
	}
	rec.memoryUsed = memoryUsed
	c.memoryUsed += memoryUsed
}

// Register a record as being used in another record
func registerInclusion(parent, child intercacheRecordLocation) {
	cacheMu.RLock()
	defer cacheMu.RUnlock()

	c := caches[child.cache]
	c.mu.Lock()
	defer c.mu.Unlock()

	rec, ok := c.buckets[child.frontend][child.key]
	if !ok {
		return // Already evicted
	}
	rec.includedIn = append(rec.includedIn, parent)
}

// Evict record from cache on the next garbage collection
func (c *Cache) scheduleEviction(frontend uint, key Key) {
	c.lock()
	defer c.unlock()

	// TODO: Schedule eviction
}

// TODO: Evict all method
