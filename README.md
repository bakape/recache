[![GoDoc](https://godoc.org/github.com/bakape/recache?status.svg)](https://godoc.org/github.com/bakape/recache)
[![Build Status](https://travis-ci.org/bakape/recache.svg?branch=master)](https://travis-ci.org/bakape/recache)
[![codecov](https://codecov.io/gh/bakape/recache/branch/master/graph/badge.svg)](https://codecov.io/gh/bakape/recache)

# recache
recursive compressed cache library and server

recache is a library (standalone server implementation pending) that enables you
to easily construct caches with reusable components stored efficiently in
compressed buffers and efficiently streamed to any client consumer.

This is based on the fact that any compliant GZIP decoder decompresses a
concatenation of individually compressed component GZIP buffers to the
equivalent of the concatenation of the source component buffers. This allows
recache to generate a tree of components on request that can be sequentially
written to a consumer such as a HTTP request or buffer builder with zero extra
buffer copies or allocation.

Unlike more traditional caches, that provide a more or less CRUD-like interface,
recache abstracts cache hits and misses from the client. The client instead
provides a lookup key and lambda to generate a new cache record.
In case of cache miss (of the targeted record or any included record references)
recache calls the provided lambda, compresses the result, generates hashes and
ETags for versioning and registers any recursively looked up records from the
same or other cache instances.

On client request a component tree is generated for this specific request.
This allows recache to pass any errors, that occurred during generation, before
writing a single byte to the client, enabling simple error propagation.
Once a component tree has been generated it is immutable and safe to be streamed
to the client, even if a component is evicted concurrently during the streaming.

recache guarantees work deduplication with concurrent requests requesting a
missing record. In such a case the first client will proceed to generate the
cache record data and any subsequent clients will block until this generation
has completed. Once generation is completed, the record is immutable and any
subsequent clients will simply consume it after a cheap atomic flag check.

A single cache can contain multiple frontends. A frontend's stored data is
subject to the same memory and LRU limits as its parent cache, however each
frontend has it's own private key space and a possibly different new record
generation lambda.

recache provides configurable per-cache instance maximum used memory and last
record use time limits. In case of overflow the least recently used records are
evicted from the cache until the overflow is eventually mitigated.
recache also provides methods for evicting records by key, by matcher functions
or clearing the cache or frontend by evicting all records.

TODO: benchmarks
