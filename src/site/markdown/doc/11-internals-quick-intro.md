Quick intro to MapDB internals
==============================

This chapter gives 5 minutes introduction to MapDB internal architecture. Rest of this
manual assume that you have read this chapter.

MapDB originally evolved as store for astronomical desktop application. As such it has
a bit different design from most DBs. Major goal was to minimise overhead of any sort
(garbage collection, memory, CPU, stack trace length...).

Also serialization lifecycle is very different here. In most DB engines user has to serialize
data himself and pass binary data into db. API than looks similar to this:

        engine.update(long recid, byte[] data);

But MapDB serializes data itself by using user supplied serializer:

        engine.update(long recid, Person data, Serializer<Person> serializer);

So serialization lifecycle is driven by MapDB rather than by user. This small detail is reason
why MapDB is so flexible. For example `update` method could pass data-serializer pair to
[background-writer](http://www.mapdb.org/apidocs/org/mapdb/AsyncWriteEngine.html)
thread and return almost instantly. Or `Person` instance could be stored in
[instance cache](http://www.mapdb.org/apidocs/org/mapdb/Caches.html), to minimise
deserilization overhead on multiple reads. `Person` does not even have to be
serialized, but could be stored in `Map<Long,Person>` map
[on heap](http://www.mapdb.org/apidocs/org/mapdb/StoreHeap.html), in this case
MapDB has speed comparable to Java Collections.


DBMaker and DB
---------------

90% of users will only need two classes from MapDB. [DBMaker](http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html)
is builder style configurator which opens database.
[DB](http://www.mapdb.org/apidocs/org/mapdb/DB.html) represents store, it creates and opens collections, commits
or rollbacks data.

MapDB collections use `Engine` (simple key-value store) to persist its data and state.
Most of functionality comes from mixing `Engine` implementations and wrappers.
For example off-heap store with asynchronous writes
and instance cache could be instantiated by this pseudo-code:
```
    Engine engine = new Caches.HashTable(         //instance cache
                        new AsyncWriteEngine(     //asynchronous writes
                         new StoreWAL(            //actual store with WAL transactions
                          new Volume.MemoryVol()  //raw buffer used for storage
                    )))
```

Reality is even more complex since each wrapper takes extra parameters and there are more levels.
So `DBMaker` is a factory which takes settings and wires all MapDB classes together.

`DB` has similar role. It is too hard to load and instantiate collections manually (for example
`HTreeMap` constructor takes 14 parameters). So `DB` stores all settings in Named Catalog and
handles collections. Named Catalog is `Map<String,Object>` which is persisted in store at fixed
recid and contains parameters for all other named collections and named records. To rename collection
one just has to rename relevant keys in Named Catalog.


Layers
------

MapDB stack is little bit different from most DBs.
It integrates instance cache and serialization usually found in ORM frameworks.
On other side MapDB eliminated fixed-size page and disk cache.

From raw-files to `Map` interface it has following layers:

 1) **Volume** - an `ByteBuffer` like abstraction over raw store. There are implementations for
 in-memory buffers or files.

 2) **Store** - primitive key-value store (implementation of `Engine`).
    Key is offset on index table, value is variable length data.
    It has single transaction. Implementations are Direct, WAL, append-only and
    Heap (which does not use serialization). It performs serialization, encryption and compression.

 3) **AsyncWriterEngine** - is optional `Store` (or `Engine`) wrapper which performs all modifications
    on background thread.

 4) **Instance Cache** - is `Engine` wrapper which caches object instances. This minimises deserilization overhead.

 5) **TxMaker** - is `Engine` factory which creates fake `Engine` for each transaction or snapshot. Dirty
    data are stored on heap.

 6) **Collections** - such as TreeMap use `Engine` to store their data and state.


Volume
---------
`ByteBuffer` is best raw buffer abstraction Java has. However its size is limited by 31 bits addressing to 2GB.
For that purpose MapDB uses `Volume` as raw buffer abstraction. It takes multiple `ByteBuffer`s and uses them
together with 64bit addressing. Each `ByteBuffer` has 1GB size and represents *chunk*. IO operations which cross
chunk boundaries are not supported (`readLong(1GB-3)` will throw an exception). It is responsibility
of higher layer `Store` to ensure data do not overlap chunk boundaries.

MapDB provides some Volume implementations: heap buffers, direct (off-heap) buffers, memory mapped files and
random access file. Each implementation fits different situation. For example memory mapped files have
great performance, however 32bit desktop app will probably prefer random access files. All implementations share
the same format, so it is possible to copy data (and entire store) between implementations.

User can also supply their own `Volume` implementations. For example each 1Gb chunk can be stored in
separate file on multiple disks, to create software RAID. `Volume` could also handle duplication,
binary snapshots (MapDB snapshots are at different layer) or raw disks.

Store
--------

[Engine](http://www.mapdb.org/apidocs/org/mapdb/Engine.html) (and
[Store](http://www.mapdb.org/apidocs/org/mapdb/Store.html)) is primitive key-value store
which maps recid (8-byte long record id) to some data (record). It has 4 methods
for CRUD operations and 2 transaction methods:

```
    long put(A, Serializer<A>)
    A get(long, Serializer<A>)
    void update(long, A, Serializer<A>)
    void delete(long, Serializer<A>)

    void commit()
    void rollback()
```

By default MapDB stack supports only single transaction. However there is wrapper `TxMaker` which
stores un-commited data on heap and provides concurrent ACID transactions.

[DB](http://www.mapdb.org/apidocs/org/mapdb/DB.html) is low level implementation of `Engine` which stores data on raw `Volume`. It usually has two
files (or Volumes): index table and physical file. Recid (record ID) is usually fixed offset in index table,
which contains pointer to physical file.

MapDB has multiple `Store` implementations, which differ in speed and durability guarantees. User can
also supply their own implementation.

First (and default) is [StoreWAL](http://www.mapdb.org/apidocs/org/mapdb/StoreWAL.html).
In this case Index Table contains record size and offset in physical file.
Large records are stored as linked list. StoreWAL has free space management, so released space is reused.
However over time it may require compaction. StoreWAL stores modifications in *Write Ahead Log*, which
is sequence of simple instructions such as *write byte at this offset*. On commit (or reopen) WAL is
replayed into main store, and discarded after successful file sync. On rollback the WAL is discarded.

[StoreDirect](http://www.mapdb.org/apidocs/org/mapdb/StoreDirect.html)
shares the same file format with `StoreWAL`, however it does not use write ahead log.
Instead it writes data directly data into files and performs file sync on commit and close.
This implementation trades any sort of data protection for speed, so data are usually lost if
`StoreDirect` is not closed correctly (or synced after last write).
Because there is no WAL, this store does not support rollback. This store is used
if transactions are disabled.

Third implementation is `StoreAppend` which provides append-only file store. Because data are
never overwritten, it is very solid and stable. However space usage skyrockets, since it
stores all modifications ever made.
TODO This store is not finished yet, so for example advanced compaction is missing.
TODO Also all possibilities of this store are not explored (and documented yet).
This store reads all data in sequence, in order to build Index Table which points
to newest version of each record. The Index Table is stored on heap.

Engine Wrappers
----------------
Big part of features in MapDB is implemented as `Engine` wrappers. For example `update` method
does not have modify file directly, but it can forward modification into
[background-writer](http://www.mapdb.org/apidocs/org/mapdb/AsyncWriteEngine.html)

Also deserialized records can be stored in [instance cache](http://www.mapdb.org/apidocs/org/mapdb/Caches.html),
so it does not have to be deserialized on next read.

TODO expand Engine Wrappers section

TxMaker
-------
MapDB `Store`s support only single transaction. So concurrent transactions needs to be serialized
and commited one by one. For this there is
[TxMaker](http://www.mapdb.org/apidocs/org/mapdb/TxMaker.html).
It is factory which creates fake `Engine` for each transaction.
Dirty (uncommited) data are stored on heap.
Optimistic concurrency control is used to detect conflicts.
[TxRollbackException](http://www.mapdb.org/apidocs/org/mapdb/TxRollbackException.html)
is thrown on write or commit, if current transaction was rolled back thanks to an conflict.

TxMaker has Serializable Isolation level, this level supports highest guarantees.
Other isolation levels are not implemented, since author does not want to support (and explain)
isolation problems.

TODO Current TxMaker uses global lock, so concurrent performance sucks. It will be rewritten after 1.0 release.

Collections
------

MapDB collection uses `Engine` as its parameter. There are two basic indexes:

[BTreeMap](http://www.mapdb.org/apidocs/org/mapdb/BTreeMap.html) is ordered B-Linked-Tree.
It offers great concurrent performance. It is best for small sized keys.

[HTreeMap](http://www.mapdb.org/apidocs/org/mapdb/HTreeMap.html) is segmented Hash-Tree.
It is good for large keys and values. It also supports entry expiration based on maximal size
or time-to-live.

There also also [Queues](http://www.mapdb.org/apidocs/org/mapdb/Queues.html)
and [Atomic](http://www.mapdb.org/apidocs/org/mapdb/Atomic.html) variables

TODO explain collections.

Serialization
------------

MapDB contains its own serialization framework.
TODO explain serialization


Concurrency patterns
---------------------

TODO concurrency patterns.





