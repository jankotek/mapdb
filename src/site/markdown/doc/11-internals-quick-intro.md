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

Most of functionality comes from mixing wrappers. For example off-heap store with asynchronous writes
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

 2) **Store** - primitive key-value store. Key is offset on index table, value is variable length data.
    It has single transaction. Implementations are Direct, WAL, append-only and
    Heap (which does not use serialization). It performs serialization, encryption and compression.

 3) **AsyncWriterEngine** - is optional `Store` (or `Engine`) wrapper which performs all modifications
    on background thread.

 4) **Instance Cache** - is `Engine` wrapper which caches object instances. This minimises deserilization overhead.

 5) **TxMaker** - is `Engine` factory which creates fake `Engine` for each transaction or snapshot. Dirty
    data are stored on heap.

 6) **Collections** - such as TreeMap use `Engine` to store their data and state.

TODO fit serialization into this scheme


Volume
---------
`ByteBuffer` is best raw buffer abstraction Java has. However its size is limited by 31 bits addressing to 2GB.
For that purpose MapDB uses `Volume` as raw buffer abstraction. It takes multiple `ByteBuffer`s and uses them
together with 64bit addressing. Each `ButeBuffer` has 1GB size and represents *chunk*.
Map

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






