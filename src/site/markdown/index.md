Are you running out of memory?
=============================

```
There is easy way to extend your data model to handle billions of objects.
It is so simple you won't even recognize you just went off-heap.
There is no catch, no clustering, no complexity, no extra layers...
... and it is free as a beer under Apache License.
```

**Warning:** benchmarks and text bellow may shock you***!!!***

It might be hard to believe technology like this exists. <br/>
But keep your mind open *.......* it took more than decade to develop it!<br/>
Please give us 10 minutes to convince you there are no boundaries.

It is still Data and Java, but the way it should be.. ..

Java beyond heap
----------------

Java Memory Model is great but has its limits. Create a few billions of objects and your CPU turns
to very expensive Garbage Collection truck.

What is the solution? Adding more memory? Adding more computers? Or just give in and use C++?

Scale-up or scale-out is not always best solution. Thanks to MapDB you can optimize, **scale-in and keep Java**.

MapDB keeps your data off-heap (or on disk). Only small recently used fraction is on heap.
Your code *sees* billions of objects, but only a few dozens are GCable. You can also optimize and compress, so
**off-heap takes smaller space**.


How does it work?
-------------------

MapDB provides Maps, Sets, Queues and Atomic Variables backed by state of the art **database engine**.
After more than decade of rewrites and optimizations it emerged as **alternative memory model to Java heap**.

TODO benchmark chart with 1E9 items, MapDB versus ConcurrentHashMap

Now it is so fast, it even outperforms Java collections.


Yet another NoSQL database?
----------------------------
MapDB existed long before 'NoSQL' term was born (under name JDBM). It was originally developed as an alternative
to flat binary files for astronomical desktop application and java applets. As such it had different
requirements for complexity, overhead and speed. It evolved separately from other DBs and has strange architecture as result.

It nicely combines high-level abstractions usual in Java, with tightly optimized assembly like code.
Over time it evolved into fully featured db. Now it has optional ACID transactions, snapshots, concurrency...
It also includes stuff from ORMs such as serialization or instance cache. All packed into 300KB jar with no other dependencies.


Features
-----------

* **Concurrent** - MapDB has record level locking and state-of-art concurrent engine. Its performance scales nearly linearly with number of cores. Data can be written by multiple parallel threads.

* **Fast** - MapDB has outstanding performance rivaled only by native DBs. It is result of more than a decade of optimizations and rewrites.

* **ACID** - MapDB optionally supports ACID transactions with full MVCC isolation. MapDB uses write-ahead-log or append-only store for great write durability.

* **Flexible** - MapDB can be used everywhere from in-memory cache to multi-terabyte database. It also has number of options to trade durability for write performance. This makes it very easy to configure MapDB to exactly fit your needs.

* **Hackable** - MapDB is component based, most features (instance cache, async writes, compression) are just class wrappers. It is very easy to introduce new functionality or component into MapDB.

* **SQL Like** - MapDB was developed as faster alternative to SQL engine. It has number of features which makes transition from relational database easier: secondary indexes/collections, autoincremental sequential ID, joints, triggers, composite keys...

* **Low disk-space usage** - MapDB has number of features (serialization, delta key packing...) to minimize disk used by its store. It also has very fast compression and custom serializers. We take disk-usage seriously and do not waste single byte.

Checkout MapDB overview. TODO link to overview page

Hello world
-----------

MapDB is in Maven Central, add this to your pom file.:

```xml
<dependency>
    <groupId>org.mapdb</groupId>
    <artifactId>mapdb</artifactId>
</dependency>
```

And run simple example:

```java
import org.mapdb.*;
ConcurrentNavigableMap treeMap = DBMaker.newTempTreeMap()

// and now use disk based Map as any other Map
treeMap.put(111,"some value")
```

Now you can follow [10 minutes introduction](intro.html)


What you should know
----------------------
* Transactions (write-ahead-log) can be disabled with <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#writeAheadLogDisable()">DBMaker.writeAheadLogDisable()</a>, this will speedup writes. However without transactions store gets corrupted easily when not closed correctly.

* Keys and values must be immutable. MapDB may serialize them on background thread, put them into instance cache. Modifying an object after it was stored is a bad idea.

* MapDB relies on memory mapped files. On 32bit JVM you will need <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#randomAccessFileEnable()">DBMaker.randomAccessFileEnable()</a> configuration option to access files larger than 2GB. This introduces overhead compared to memory mapped files.

* MapDB does not run defrag on background. You need to call `DB.compact()` from time to time.

* MapDB uses unchecked exceptions. All `IOException` are wrapped into unchecked `IOError`. MapDB has weak error handling and assumes disk failure can not be recovered at runtime. However this does not affects data safety, if you use durable commits.



Support
-------
There is [mail-group](mailto:mapdb@googlegroups.com) with [archive](http://groups.google.com/group/mapdb). Ask here
support questions such as 'how do I open db'.

Anything with stack-trace should go to [new bug report](https://github.com/jankotek/MapDB/issues/new).

Before creating report:

* check if it is not already fixed in snapshot repository

* try alternative configurations (disable cache, disable async write thread...)

* try to use your own serializer, to make sure problem is not in serializer

MapDB is a hobby project and my time is limited.Please always attach code to illustrate/reproduce your problem, so I can fix it efficiently. For hard to reproduce problems I would strongly suggest to record JVM execution with
[Chronon](http://www.chrononsystems.com/learn-more/products-overview) and submit the record together with a bug report. This will make sure your
bug will get fixed promptly.

Last option is to [contact me directly](mailto:jan at kotek dot net). I prefer public bug tracker and mail-group so others can find answers as well. Unless you specify your question as confidential, I may forward it to public mail-group.


![Image](https://www.google-analytics.com/__utm.gif?utmac=UA-42074659-3&raw=true)
