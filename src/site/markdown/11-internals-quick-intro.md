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
[background-writer](apidocs/org/mapdb/AsyncWriteEngine.html)
thread and return almost instantly. Or `Person` instance could be stored in
[instance cache](apidocs/org/mapdb/Caches.html), to minimise
deserilization overhead on multiple reads. `Person` does not even have to be
serialized, but could be stored in `Map<Long,Person>` map
[on heap](apidocs/org/mapdb/StoreHeap.html), in this case
MapDB has speed comparable to Java Collections.

Colons can be used to align columns.



Dictionary
-----------

| Term | Explanation |
| ------------- | ------------- |
| Record | Atomically stored value. Usually tree node or similar. Transaction conflicts and locking is usually per record. |
| Index Table | Table which translates recid into real offset and size in physical file. |
| [Engine](apidocs/org/mapdb/Engine.html) | Primitive key-value store used by collections for storage. Most features are [Engine Wrappers](apidocs/org/mapdb/EngineWrapper.html) |
| [Store](apidocs/org/mapdb/Store.html) | Engine implementation which actually persist data. Is wrapped by other Engines. |
| [Volume](apidocs/org/mapdb/Volume.html) | Abstraction over ByteBuffer or other raw data store. Used for files, memory, partition etc.. |
| Slice | Non overlapping pages used in Volume. Slice size is 1MB. Older name was 'chunk'|
| Direct mode | With disabled transactions, data are written directly into file. It is fast, but store is not protected from corruption during crasches. |
| WAL | Write Ahead Log, way to protect store from corruption if db crashes during write. |
| RAF | Random Access File, way to access data on disk. Safer but slower method. |
| MMap file | memory mapped file. On 32bit platform has size limit around 2GB. Faster than RAF. |
| index file | contains mapping between recid (index file offset) and record size and offset in physical file (index value). Is organized as sequence of 8-byte longs |
| index value | single 8-byte number from index file. Usually contains record offset in physical file. |
| recid |	record identificator, an unique 8-byte long number which identifies record. Recid is offset in index file. After record is deleted, its recid may be reused for newly inserted record. |
| record | atomical value stored in storage (Engine) identified by record identifier (recid). In collections Record corresponds to tree nodes. In Maps record mey not correspond to Key->Value pair, as multiple keys may be stored inside single node.|
| physical file |	Contains record binary data |
| cache (or instance cache) |	caches object instances (created with 'new' keyword). MapDB does not have traditional fixes-size-buffer cache for binary pages (it relies on OS to do this). Instead deserialized objects are cached on heap to minimise deserialization overhead. Instance cache is main reason why your keys/values must be immutable. |
| BTreeMap |	tree implementation behind TreeMap and TreeSet provided by MapDB |
| HTreeMap |	tree implementation behind HashMap and HashSet provided by MapDB |
| delta packing | compression method to minimalise space used by keys in BTreeMap. Keys are sorted, so only difference between keys needs to be stored. You need to privide specialized serializer to enable delta packing. |
| append file db |	alternative storage format. In this case no existing data are modified, but all changes are appended to end of file. This may improve write speed and durability, but introduces some tradeoffs. |
| temp map/set...	 | collection backed by file in temporary directory. Is usually configured to delete file after close or on JVM exit. Data written into temp collection are not persisted between JVM restarts. |
| async write | 	writes may be queued and written into file on background thread. This does not affect commit durability (it blocks until queue is empty). |
| TX |	equals to Concurrent Transaction. |
| LongMap	| specialized map which uses primitive long for keys. It minimises boxing overhead. |
| DB |	API class exposed by MapDB. It is an abstraction over Engine which manages MapDB collections and storage. |
| DMaker | Builder style factory class, which opens and configures DB instances. |
| Collection Binding |	MapDB mechanism to keep two collections synchronized. It provides secondary keys and values, aggregations etc.. known from SQL and other databases. All functions are provided as static methods in Bind class. |
| Data Pump | Tool to import and manipulate large collections and storages. |

DBMaker and DB
---------------

90% of users will only need two classes from MapDB. [DBMaker](apidocs/org/mapdb/DBMaker.html)
is builder style configurator which opens database.
[DB](apidocs/org/mapdb/DB.html) represents store, it creates and opens collections, commits
or rollbacks data.

MapDB collections use `Engine` (simple key-value store) to persist its data and state.
Most of functionality comes from mixing `Engine` implementations and wrappers.
For example off-heap store with asynchronous writes
and instance cache could be instantiated by this pseudo-code:

```java
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
together with 64bit addressing. Each `ByteBuffer` has 1GB size and represents *slice*. IO operations which cross
slice boundaries are not supported (`readLong(1GB-3)` will throw an exception). It is responsibility
of higher layer `Store` to ensure data do not overlap slice boundaries.

MapDB provides some Volume implementations: heap buffers, direct (off-heap) buffers, memory mapped files and
random access file. Each implementation fits different situation. For example memory mapped files have
great performance, however 32bit desktop app will probably prefer random access files. All implementations share
the same format, so it is possible to copy data (and entire store) between implementations.

User can also supply their own `Volume` implementations. For example each 1Gb slice can be stored in
separate file on multiple disks, to create software RAID. `Volume` could also handle duplication,
binary snapshots (MapDB snapshots are at different layer) or raw disks.

Store
--------

[Engine](apidocs/org/mapdb/Engine.html) (and
[Store](apidocs/org/mapdb/Store.html)) is primitive key-value store
which maps recid (8-byte long record id) to some data (record). It has 4 methods
for CRUD operations and 2 transaction methods:

```java
    long put(A, Serializer<A>)
    A get(long, Serializer<A>)
    void update(long, A, Serializer<A>)
    void delete(long, Serializer<A>)

    void commit()
    void rollback()
```

By default MapDB stack supports only single transaction. However there is wrapper `TxMaker` which
stores un-commited data on heap and provides concurrent ACID transactions.

[DB](apidocs/org/mapdb/DB.html) is low level implementation of `Engine` which stores data on raw `Volume`. It usually has two
files (or Volumes): index table and physical file. Recid (record ID) is usually fixed offset in index table,
which contains pointer to physical file.

MapDB has multiple `Store` implementations, which differ in speed and durability guarantees. User can
also supply their own implementation.

First (and default) is [StoreWAL](apidocs/org/mapdb/StoreWAL.html).
In this case Index Table contains record size and offset in physical file.
Large records are stored as linked list. StoreWAL has free space management, so released space is reused.
However over time it may require compaction. StoreWAL stores modifications in *Write Ahead Log*, which
is sequence of simple instructions such as *write byte at this offset*. On commit (or reopen) WAL is
replayed into main store, and discarded after successful file sync. On rollback the WAL is discarded.

[StoreDirect](apidocs/org/mapdb/StoreDirect.html)
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
[background-writer](apidocs/org/mapdb/AsyncWriteEngine.html)

Also deserialized records can be stored in [instance cache](apidocs/org/mapdb/Caches.html),
so it does not have to be deserialized on next read.

TODO expand Engine Wrappers section

TxMaker
-------
MapDB `Store`s support only single transaction. So concurrent transactions needs to be serialized
and commited one by one. For this there is
[TxMaker](apidocs/org/mapdb/TxMaker.html).
It is factory which creates fake `Engine` for each transaction.
Dirty (uncommited) data are stored on heap.
Optimistic concurrency control is used to detect conflicts.
[TxRollbackException](apidocs/org/mapdb/TxRollbackException.html)
is thrown on write or commit, if current transaction was rolled back thanks to an conflict.

TxMaker has Serializable Isolation level, this level supports highest guarantees.
Other isolation levels are not implemented, since author does not want to support (and explain)
isolation problems.

TODO Current TxMaker uses global lock, so concurrent performance sucks. It will be rewritten after 1.0 release.

Collections
------

MapDB collection uses `Engine` as its parameter. There are two basic indexes:

[BTreeMap](apidocs/org/mapdb/BTreeMap.html) is ordered B-Linked-Tree.
It offers great concurrent performance. It is best for small sized keys.

[HTreeMap](apidocs/org/mapdb/HTreeMap.html) is segmented Hash-Tree.
It is good for large keys and values. It also supports entry expiration based on maximal size
or time-to-live.

There also also [Queues](apidocs/org/mapdb/Queues.html)
and [Atomic](apidocs/org/mapdb/Atomic.html) variables

TODO explain collections.

Serialization
------------

MapDB contains its own serialization framework.
TODO explain serialization


Concurrency patterns
---------------------

TODO concurrency patterns.





