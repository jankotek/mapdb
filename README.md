
MapDB provides concurrent Maps, Sets and Queues backed by disk storage or off-heap-memory.
It is a fast and easy to use embedded Java database engine. MapDB is free as speech and free as beer under 
[Apache License 2.0](https://github.com/jankotek/MapDB/blob/master/doc/license.txt).

Features
========
* **Concurrent** - MapDB has record level locking and state-of-art concurrent engine. Its performance scales nearly linearly with number of cores. Even single threaded app can take advantage of asynchronous writes on background thread.

* **ACID** - MapDB optionally supports ACID transactions with full MVCC isolation. MapDB uses write-ahead-log or append-only store for great write durability.

* **Flexible** - MapDB can be used anywhere from in-memory cache to multi-terabyte database. It also has number of options to trade durability for write performance. It is very easy to configure MapDB to exactly fit your needs.

* **Hackable** - MapDB is component based, most features (instance cache, async writes, compression) are just class wrappers. It is very easy to introduce new functionality or component into MapDB. 

* **SQL Like** - MapDB was developed as faster alternative to SQL engine. It has number of features which makes transition from rational database easier: secondary indexes/collections, autoincremental sequential ID, joints, triggers composite keys...

* **Low disk-space usage** - MapDB has number of features (serialization, delta key packing...) to minimize disk used by its store. It also has very fast compression and custom sterilizers. We take disk-usage seriously and do not waste single byte.

* **Familiar** - In some way MapDB is just an alternative memory management model to Java Heap. If you are know Java concurrent  programming, you will find MapDB familiar. There is compare-and-swap (at store level), atomic variables, concurrent collections... In fact we discourage using transactions and recommend traditional concurrent programming instead.




Intro
======
MapDB has very power-full API, but for 99% cases you need just two classes: [DBMaker](http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html) is builder style factory for configuring and opening a database. It has handful of static 'newXXX' methods for particular storage mode. [DB](http://www.mapdb.org/apidocs/org/mapdb/DB.html) represents storage. It has methods for accessing Maps and other collections. It also controls DB life-cycle with commit, rollback and close methods.

Best place to checkout various features of MapDB are [Examples](https://github.com/jankotek/MapDB/tree/master/src/test/java/examples). There is also [screencast](http://www.youtube.com/watch?v=FdZmyEHcWLI) which describes most aspects of MapDB.

MapDB is in Maven Central. Just add code bellow to your pom file to use it. You may also download jar files directly from [repo](http://search.maven.org/#artifactdetails%7Corg.mapdb%7Cmapdb%7C0.9.0%7Cjar).

```xml
<dependency>
    <groupId>org.mapdb</groupId>
    <artifactId>mapdb</artifactId>
    <version>0.9.0</version>
</dependency>
```

There is also repository with [daily builds](https://oss.sonatype.org/content/repositories/snapshots/org/mapdb/mapdb/):

```xml
<repositories>
    <repository>
        <id>sonatype-snapshots</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>org.mapdb</groupId>
        <artifactId>mapdb</artifactId>
        <version>0.9.1-SNAPSHOT</version>
    </dependency>
</dependencies>
```

Hereafter is a simple example. It opens TreeMap backed by file in temp directory, file is discarded after JVM exit:

```java
import org.mapdb.*;
ConcurrentNavigableMap treeMap = DBMaker.newTempTreeMap()

// and now use disk based Map as any other Map
treeMap.put(111,"some value")
```

More advanced example with configuration and write-ahead-log transaction.

```java
import org.mapdb.*;

// configure and open database using builder pattern.
// all options are available with code auto-completion.
DB db = DBMaker.newFileDB(new File("testdb"))
               .closeOnJvmShutdown()
               .encryptionEnable("password")
               .make();

// open existing an collection (or create new)
ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");

map.put(1, "one");
map.put(2, "two");
// map.keySet() is now [1,2]

db.commit();  //persist changes into disk

map.put(3, "three");
// map.keySet() is now [1,2,3]
db.rollback(); //revert recent changes
// map.keySet() is now [1,2]

db.close();
```

What you should know
====================
* Transactions (write-ahead-log) can be disabled, this will speedup writes. However without transactions
store gets corrupted easily when not closed correctly.

* Keys and values must be immutable. MapDB may serialize them on background thread, put them into instance cache... 
Modifying an object after it was stored is bad idea.

* MapDB relies on memory mapped files. On 32bit JVM you need `DBMaker.newRandomAccessFileDB()`
to access files larger than 2GB. This introduces some overhead compared to memory mapped files.

* MapDB does not run defrag on background. You need to call `DB.compact()` from time to time.

* MapDB uses unchecked exceptions. All `IOException` are wrapped into unchecked `IOError`.

Benchmark
=========
MapDB has outstanding performance rivaled only by native db engines. There is no proper benchmark yet, so here is just small example.

Insert/read/update 100 000 000 records into Map in single thread. Source code is [here](https://github.com/jankotek/MapDB/blob/master/src/test/java/benchmark/Basic_SingleThread.java)
* Sequential insert took 144 seconds. That is 695 000 inserts per second
* Randomly read 1e8 entries from previous set takes  264 seconds. That is 379 000 reads per second
* Randomly update 1e8 entries from previous set takes 620 seconds. That is 161 000 random updates per second.


Support
=======
There is [mail-group](mailto:mapdb@googlegroups.com) with [archive](http://groups.google.com/group/mapdb). Ask here 
support questions such as 'how do I open db'.

Anything with stack-trace should go to [new bug report](https://github.com/jankotek/MapDB/issues/new). 

Before creating report:

* check if it is not already fixed in snapshot repository

* try alternative configurations (disable cache, disable async write thread...) 

* try to use your own serializer, to make sure problem is not in serializer

MapDB is a hobby project and my time is limited.
Please always attach code to illustrate/reproduce your problem, so I can fix it efficiently. Push requests with failing unit test case would be appreciated. For hard to reproduce problems I would strongly suggest to record JVM execution with
[Chronon](http://www.chrononsystems.com/learn-more/products-overview) and submit the record together with a bug report. This will make sure your 
bug will get fixed promptly.

Last option is to [contact me directly](mailto:jan at kotek dot net).
I prefer public bug tracker and mail-group so others can find answers as well.
Unless you specify your question as confidential, I may forward it to public mail-group.
