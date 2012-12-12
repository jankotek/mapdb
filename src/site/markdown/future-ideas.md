Future Ideas
============


### Increase maximal record size over 64KB
Serialized record size can not be currently bigger than 64KB. It is because record size is marked with 2-byte-long number.
MapDB should support record size up to 256MB. Plan is to chain small records into linked list.

### Engine wrappers to support Network Sockets
MapDB does not support client-server communication directly, but user may implement it themselfs.
So there should be two engine wrappers `SocketServerEngine` and `SocketClientEngine` to allow using MapDB remotely over network socket. It should rely on already established connection (users responsibility).

### Unsafe Memory Store
MapDB relies on mapped ByteBuffers and those use `sun.misc.Unsafe` class internaly. Using `Unsafe` class directly may increase performance. But this means bypassing JVM protections and could crash JVM. We should also investigate hybrid approach (for example only index Volume is unsafe).
Apache Memory Direct project is using this.

### Unsafe class in deserialization
`sun.misc.Unsafe` methods allows directly manipulating class structure. It bypasses JVM protections and should be faster than reflection. Lightnight serializer is using it.

### Internal Fork/Join framework
Scala Parallel Collections supports operations such as `map`, `reduce`, `filter`, `contains` executed in parallel. Fork/Join framework (used by Scala) needs to divide collection into parts to work efectively. So parallel operations
are usually performed on indexed array style collection, where splitting into parts is easy.
MapDB could use internal tree node structure to achive this split. My estimate is that internal Fork/Join framework would be 30% faster than similar parallel solution which would rely on iterators.

### Lirs Cache
MapDB does not currently have MRU (Most Recently Used) or LIRS (Low Inter-
reference Recency Set) cache. Main problem is concurrent scalability of such cache.
There is [project](http://code.google.com/p/concurrentlinkedhashmap/)
under development, we should integrate it into MapDB when it matures.

### Full Java Serialization compability
MapDB serialization tryes to mimic standard Java Serialization (for example Serializable marker interface).
However there are still many cases we do not support, such as Externalizable and writeExternal/readExternal methods.
This may cause some code to be incompatible with MapDB.

### Optimize Transaction Log instructions
Transaction log could be smaller (and transactions faster). We could reduce size by choosing right type of log replay
instructions. This applies mostly to LongStack updates (currently entire record is dumped even if only single slot changes).

### Raw Disk Partition support
[Pure Java is capable](http://stackoverflow.com/questions/2108313/how-to-access-specific-raw-data-on-disk-from-java) of using raw disk partition on most operating system. Raw Disk Partition means bypassing filesystem and probably better performance. MapDB should be capable of using it directly. We should document it, present examples and make sure
it works faster. Storage should be also capable of spreading over multiple partitions for best performance.

### SSD Trim support
We need to make sure that MapDB works well with SSD drives. It means that SSD needs to know about free space within storage file. This may be achieved by writing zeroes into unused space (just a guess). We could also use native library to issue trim instructions.

### Split storage file across multiple disks
MapDB should be capable of using multiple disks. This means splitting storage file into smaller chunks (1GB) and spreading it into multiple directories.

### Single file storage
MapDB currently uses two files (index, phys). There is also third file (trans log) to support transactions.
Index file needs to be in separate Volume to reduce fragmentation.
But storage could be stored in single file. For example virtual Index Volume could be created in first 1GB
of storage file. We could also create packed single file read-only storage.

### BTree delta serializers
Delta compression can be applied to any ordered set, it means storing first entry and than only differences. Currently it is applied only on Long[]. However we should provide BTree key serializers for other data types (String)
as well.

### Storage Engine Write Overlay
An mode where MapDB uses read-only primary storage and stores all modifications in secondary writable storage. This could be used when main (remote) db can not be modified, but we need to apply changes locally.


### Lock Free Storage
Current storage implementation uses `ReentrantReadWriteLock`. Write operation needs exclusive lock and blocks other reads. To workaround this limitation most writes are performed asynchronously in background thread. This does not scale well on large number of CPU cores (8+). 
To improve concurrency we need Storage without global locks. Exclusive locks should only apply to directly modified area of file. 

### Primitive Long BTree
MapDB uses `LongMap` as fast cache. We should try to modify current BTree implementation to support primitive long keys. This should improve performance and decrease memory consumption.

### Serialization - class and field rename
Serialized class structure information is stored on single place for all record. So renaming class or field for entire store requires very small modification. There should be utility to support basic refactoring on already created store.

### Map Entry Expiration Interval
It should be possible to use MapDB as cache with expiration period for each Map Entry. Expired entryes should be removed from store by background thread. 

### Master-Slave replication
It should be possible to setup MapDB into simple cluster similar way as MySQL. There would be single write-able master and multiple read-only slaves. Changes made on master would be distributed to slaves. 

### Concurrent transaction 
MapDB currently supports only single transaction. It should handle multiple concurreent transactions with record isolation, locking and all usuall stuff. There is solution which would require only minimal modificaiton and would have nearly zero impact on performance. 

### Java Transaction API support
With concurrrent transaction we should support JTA as well. It is widely used Java standard for various databases.

### Hash collision attack protection
Most libraries (including `j.u.HashMap`) are vulnerable to [hash collision attack](http://arstechnica.com/business/2011/12/huge-portions-of-web-vulnerable-to-hashing-denial-of-service-attack/). MapDB could be indirectly exposed to internet, so it should be immune to this attack. Solution is to add randomly generated hash salt into each store, and rehash all caches and HTreeMaps

### GUI store explorer
There should be an application which would allow to open and explore existing storage files. It should provide possibility to search modify serialized records.

### Administration GUI
There should be an GUI application for monitoring running MapDB. It should provide runtime informations such as cache-miss stats, memory consumption and fragmentation. 

### JMX support
Java Management Extension is standard API to monitor and manage applications deployed in enterprise environment. MapDB should provide statistics via this interface. 

### Cache miss statistics
Cache miss statistics are esential for performance tunning. MapDB should be able to provide it

### Store statistics
MapDB should provide basic store statistics (space usage, fragmentation,...)

### Load/Save DBMaker configuration
It should be possible to load 

### .NET port
It should be possible to port MapDB into CLI virtual machine. JVM and CLI are similar (threading, IO API).

### XML DOM Parser with MapDB backend
DOM parsers are probably most intuitive way to manipulate XML. However those do not work well on large files. We could implement DOM parser which would use MapDB to store parsed data. It could manipulate HUGE files without sweat. 





