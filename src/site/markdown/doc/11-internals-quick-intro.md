Quick intro to MapDB internals
==============================

This chapter gives 5 minutes introduction to MapDB internal architecture. Rest of this
manual assume that you have read this chapter.

TODO explain serialization lifecycle.

TODO design goals

DBMaker and DB
---------------

TODO named catalog

Layers
------

MapDB stack is little bit different from most DBs.
It integrates instance cache and serialization usually found in ORM frameworks.
On other side MapDB eliminated fixed-size page and disk cache.

From raw-files to `Map` interface it has following layers:

 1) **Volume** - an `ByteBuffer` like abstraction over raw store. There are implementations for
 in-memory buffers or files.

 2) **Store** - primitive key-value store. Key is offset on index table, value is variable length data.
    It has single transaction. Implementations are Direct, WAL, append-only and
    Heap (which does not use serialization). It performs serialization, encryption and compression.

 3) **AsyncWriterEngine** - is optional `Store` (or `Engine`) wrapper which performs all modifications
    on background thread.

 4) **Instance Cache** - is `Engine` wrapper which caches object instances. This minimises deserialization overhead.

 5) **TxMaker** - is `Engine` factory which creates fake `Engine` for each transaction or snapshot. Dirty
    data are stored on heap.

 6) **Collections** - such as TreeMap use `Engine` to store their data and state.

TODO fit serialization into this scheme

TODO explain Engine, Store and wrappers

Volume
---------

TODO 1GB chunks

TODO rename `Volume.buffer*` to chunk


Store
--------

TODO explain index and offsets

TODO append only store and its index

TODO WAL transactions

Engine Wrappers
----------------
TODO async write
TODO caches

TxMaker
-------


Collections
------


Serialization
------------


Concurrency patterns
---------------------






