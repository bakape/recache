package recache

import "time"

var (
	// Schedule and debounce eventual cache eviction of record.
	// Buffered to reduce lock contention on scheduling and deadlocks
	evictAfter = make(chan evictionReq, 1<<10)
)

type evictionReq struct {
	loc   intercacheRecordLocation
	timer time.Duration
}

func init() {
	go func() {
		pending := make(map[intercacheRecordLocation]time.Time)
		scan := time.Tick(time.Second)

		for {
			select {
			case req := <-evictAfter:
				existing, ok := pending[req.loc]
				deadline := time.Now().Add(req.timer)
				if !ok || deadline.Before(existing) {
					pending[req.loc] = deadline
				}
			case <-scan:
				now := time.Now()
				for loc, deadline := range pending {
					if deadline.Before(now) {
						delete(pending, loc)
						evict(loc, 0)
					}
				}
			}
		}
	}()
}

// Evict record from cache after t
func evict(loc intercacheRecordLocation, t time.Duration) {
	getCache(loc.cache).evict(loc.recordLocation, t)
}

// Evict record from cache after t
func (c *Cache) evict(loc recordLocation, t time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictWithLock(loc, t)
}

// Evict record from cache after t. Requires lock on c.mu.
func (c *Cache) evictWithLock(loc recordLocation, t time.Duration) {
	rec, ok := c.record(loc)
	if !ok {
		return
	}
	if t != 0 {
		evictAfter <- evictionReq{
			loc: intercacheRecordLocation{
				cache:          c.id,
				recordLocation: loc,
			},
			timer: t,
		}
		return
	}

	delete(c.frontends[loc.frontend], loc.key)
	c.lruList.Remove(rec.node)
	c.memoryUsed -= rec.memoryUsed

	for _, ch := range rec.includedIn {
		if ch.cache == c.id {
			// Hot path to reduce lock contention
			c.evictWithLock(ch.recordLocation, 0)
		} else {
			// Separate goroutine to prevent lock intersection
			go evict(ch, 0)
		}
	}
}

// Evict all keys of specific frontend after t
func (c *Cache) evictFrontend(frontend int, t time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictFrontendWithLock(frontend, t)
}

// Evict all keys of specific frontend after t. Requires lock on c.mu.
func (c *Cache) evictFrontendWithLock(frontend int, t time.Duration) {
	for _, k := range c.keys(frontend) {
		c.evictWithLock(recordLocation{frontend, k}, t)
	}
}

// Evict keys from frontend using matcher function fn after t.
//
// fn muts return true, if a key must be evicted.
func (c *Cache) evictByFunc(
	frontend int,
	t time.Duration,
	fn func(Key) (bool, error),
) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		b     = c.frontends[frontend]
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
			c.evictWithLock(recordLocation{frontend, k}, t)
		}
	}

	return
}

// Evict all records from cache after t amount of time, if the matched are still
// in the cache by then.
//
// If t = 0, any matched record(s) are evicted immediately.
//
// t can be used to decrease record turnover on often evicted records, thereby
// decreasing fresh data fetches and improving performance.
//
// Any subsequent scheduled eviction calls on matching records with a greater t
// value than is currently left from a previous scheduled eviction on the
// record will have no effect.
//
// A scheduled eviction with a smaller timer than currently left on the record
// will replace the existing timer.
func (c *Cache) EvictAll(t time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.frontends {
		c.evictFrontendWithLock(i, t)
	}
}

// Evict a record by key after t amount of time, if the matched are still in
// the cache by then.
//
// If t = 0, any matched record(s) are evicted immediately.
//
// t can be used to decrease record turnover on often evicted records, thereby
// decreasing fresh data fetches and improving performance.
//
// Any subsequent scheduled eviction calls on matching records with a greater t
// value than is currently left from a previous scheduled eviction on the
// record will have no effect.
//
// A scheduled eviction with a smaller timer than currently left on the record
// will replace the existing timer.
func (f *Frontend) Evict(t time.Duration, k Key) {
	f.cache.evict(recordLocation{f.id, k}, t)
}

// Evict all records from frontend after t amount of time, if the matched are
// still in the cache by then.
//
// If t = 0, any matched record(s) are evicted immediately.
//
// t can be used to decrease record turnover on often evicted records, thereby
// decreasing fresh data fetches and improving performance.
//
// Any subsequent scheduled eviction calls on matching records with a greater t
// value than is currently left from a previous scheduled eviction on the
// record will have no effect.
//
// A scheduled eviction with a smaller timer than currently left on the record
// will replace the existing timer.
func (f *Frontend) EvictAll(t time.Duration) {
	f.cache.evictFrontend(f.id, t)
}

// Evict records from frontend using matcher function fn after t amount of time,
//  if the matched are still in the cache by then.
//
// If t = 0, any matched record(s) are evicted immediately.
//
// t can be used to decrease record turnover on often evicted records, thereby
// decreasing fresh data fetches and improving performance.
//
// Any subsequent scheduled eviction calls on matching records with a greater t
// value than is currently left from a previous scheduled eviction on the
// record will have no effect.
//
// A scheduled eviction with a smaller timer than currently left on the record
// will replace the existing timer.
func (f *Frontend) EvictByFunc(t time.Duration, fn func(Key) (bool, error),
) error {
	return f.cache.evictByFunc(f.id, t, fn)
}
