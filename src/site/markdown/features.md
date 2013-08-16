Features
========
Most DBs engines juggle with byte arrays and leave serialization and caching to user.
MapDB goes much further and it integrates instance cache, serialization and
storage into single library. This seamless integration is necessary to achieve
best user experience. In most cases MapDB 'just works'.

Tight integration also allows many performance optimization tricks.
MapDB blurs difference between POJO (Plain Old Java Object) and storage records.
So MapDB collections can have comparable performance to their
Java Collections Framework counterparts.


Highlights
----------

* Drop-in replacement for ConcurrentTreeMap and ConcurrentHashMap.

* Outstanding performance comparable to low-level native DBs (TokyoCabinet, LevelDB)

* Scales well on multi-core CPUs (fine grained locks, concurrent trees)

* Very easy configuration using builder class

* Space-efficient transparent serialization.

* Instance cache to minimise deserialization overhead

* Various write modes (transactional journal, direct, async or append)
to match various requirements.

* ACID transactions with full MVCC isolation

* Very fast snapshots

* Very flexible; works equally well on an Android phone and
a supercomputer with multi-terabyte storage.

* Modular & extensible design; most features (compression, cache,
async writes) are just `Engine` wrappers. Introducing
new functionality (such as network replication) is very easy.

* Highly embeddable; 200 KB no-deps jar (50KB packed), pure java,
low memory & CPU overhead.

* Transparent encryption, compression, checksums and other optional stuff.



Caching
-------
MapDB provides instance cache to minimize deserialization overhead. Instance cache
is tightly integrated with storage engine and is one of the reasons why MapDB is so fast.

MapDB does not use fixed size disk pages. It also does not have
settings like page size or disk cache. To cache binary data read from disk MapDB relies
on operating system. MapDB performance strongly depends on operating system selection and
we recommend using Linux with latest JVM for best performance

### HashTable instance cache
This is fixed size random removal cache. It has very small overhead and is very fast.
It uses fixed size hash table and entries are removed randomly on hash collisions.
Is activated by default with size 32768.

### LRU instance cache
Fixed size cache with Least Recently Used entry removal. It uses Cleanup daemon thread.
Has larger overhead compared to HashTable, but will have better results object deserialization
is expensive. Is activated by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#cacheLRUEnable">DBMaker.cacheLRUEnable()</a>

### Weak reference instance cache
Unbounded instance cache with entries removed after GC collection. It uses
Weak reference so cache entry can be Garbage Collected when no longer referenced.
It uses Cleanup daemon thread. Is activated by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#cacheWeakRefEnable()">DBMaker.cacheWeakRefEnable()</a>

### Soft reference instance cache
Unbounded instance cache with entries removed after GC collection. It uses
Soft reference so entry cache entry can be Garbage Collected when no longer referenced.
It uses Cleanup daemon thread. Is activated by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#cacheSoftRefEnable()">DBMaker.cacheSoftRefEnable()</a>

### Hard reference instance cache
Unbounded instance cache with all entries removed when heap memory runs low.
All cache entries are stored in HashMap with hard reference.
To prevent OutOfMemoryError free heap memory is monitored, when it runs bellow 25%
all entries are removed from cache.  This cache is often faster than Soft/Weak Cache,
it requires much less Garbage Collections. Use this cache if you have large heap and want
maximal performance. Is activated by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#cacheHardRefEnable()">DBMaker.cacheHardRefEnable()</a>

Serialization
-------------
MapDB has its own space efficient serialization tightly optimized for minimal CPU overhead.
In many cases it outperforms specialized serialization frameworks such as Kryo or Protobufs.

MapDB serialization tries to mimic Standard Java Serialization behavior. So you may
use Serializable marker interface, customize binary format with Externalizable methods
and so on.

MapDB serialization framework is completely transparent and activated by default.
In most cases you wont even realize it is there.
But for best performance you may also bypass it and use custom binary format by
implementing [Serializer](http://www.mapdb.org/apidocs/org/mapdb/Serializer.html).

### Hand Coded Serializers
MapDB hand coded serializers for most classes in `java.lang` and `java.util` packages.
Custom serializers have maximal space efficiency and very low CPU overhead.
For example:

* `j.l.Date`  requires only 9 bytes: 1-byte-long serialization header and 8-byte-long timestamp.

* For `true`, `String("")` or `Long(3)` MapDB writes only single-byte serialization header.

* For array list and other collections MapDB writes serialization header, packed size and than data itself.

### Generic POJO serialization
For other classes MapDB uses generic POJO serialization. It traverses and serializes object graph,
handling stuff like forward/backward references in process.
MapDB stores class structure (names, fields types) on centralized space,
this greatly reduces space overhead with larger number of records. For example take following class:

     class Person{ int age = 40; String nickname = "Agent Smith"; }

Standard Java Serialization stores this class in 104 bytes with many redundant information such as
field names:

      __srorg.apache.jdbm.geecon.Person O IageL __ __ nicknametLjava/lang/String;xp(tAgent Smith

MapDB serialization stores this record with  only 15 bytes!

### Asynchronous Writes
MapDB performs serialization and storage modifications in background thread. This behaviour is fully
ACID compliant by default. Thanks to Async Writes MapDB has high performance and low latency even under high load.
There is number of configuration option to fine-tune Async Writes for your usage pattern.


Other features
--------------
MapDB has many other small features which would not fit on this page. You may find more by reading
[DBMaker documentation](http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html) or MapDB source codes.

### Transparent compression
MapDB supports transparent compression using [LZF compressor](http://oldhome.schmorp.de/marc/liblzf.html).
It is very fast compression with typical rates around 40%. It is tightly optimized in does not require
much internal copying and object allocations. It can be enabled by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#compressionEnable()">DBMaker.compressionEnable()</a>

### Transparent encryption
MapDB allows storage encryption using [XTea algorithm](http://en.wikipedia.org/wiki/XTEA).
It is fast and sound algorithm with 256bit keys. It is highly optimized for MapDB to minimize internal copying.
Other encryption algorithms can be used with some extra coding.
Encryption can be enabled by <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#encryptionEnable(java.lang.String)">DBMaker.encryptionEnable(String password)</a>

