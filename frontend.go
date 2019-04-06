package recache

import (
	"crypto/sha1"
	"io"
)

// Value used to store entries in the cache. Must be a type suitable for being a
// key in a Go map.
type Key interface{}

// Generates fresh cache records for the given key. These records wil be stored
// by the cache engine and must not be modified after Getter returns.
//
// Getter must be thread-safe.
type Getter func(Key, *RecordWriter) error

// A frontend for accessing the cache contents
type Frontend struct {
	id     uint
	cache  *Cache
	getter Getter
}

// TODO: Method for writing to http.ResponseWriter with ETag handling and Gzip
// encoding setting

// Populates a record using the registered Getter
func (f *Frontend) populate(k Key, rec *record) (err error) {
	rw := RecordWriter{
		frontend: f.id,
		key:      k,
	}
	err = f.getter(k, &rw)
	if err != nil {
		return
	}

	// Flush any unclosed gzip buffers
	err = rw.flushPending(true)
	if err != nil {
		return
	}

	rec.data = rw.data
	memoryUsed := 0
	if len(rec.data) == 1 {
		d := rec.data[0]
		memoryUsed = d.Size()
		rec.hash = d.Hash()
	} else {
		h := sha1.New()
		for _, d := range rec.data {
			memoryUsed += d.Size()

			// Hash the child hashed to better propagate changes
			arr := d.Hash()
			h.Write(arr[:])
		}
		copy(rec.hash[:], h.Sum(nil))
	}
	f.cache.setUsedMemory(recordLocation{f.id, k}, memoryUsed)

	return
}

// Get a record by key and block until it has been generated
func (f *Frontend) getGeneratedRecord(k Key) (rec *record, err error) {
	rec, fresh := f.cache.getRecord(f.id, k)
	if fresh {
		err = f.populate(k, rec)
		if err != nil {
			// Propagate error to any concurrent readers
			rec.populationError = err

			f.cache.scheduleEviction(f.id, k)
		}

		// Also unblock any concurrent readers, even on error.
		// Having it here also protects from data race on rec.populationError.
		rec.wg.Done()
	}

	// Prevents a record being read concurrently before it is populated.
	// A record is immutable after initial population and this will not block
	// after it.
	rec.wg.Wait()
	err = rec.populationError

	return
}

// Retrieve or generate data by key and write it to w
func (f *Frontend) WriteTo(k Key, w io.Writer) (n int64, err error) {
	rec, err := f.getGeneratedRecord(k)
	if err != nil {
		return
	}
	return rec.WriteTo(w)
}

// TODO: Explicit exact key match, all record and matcher function eviction
