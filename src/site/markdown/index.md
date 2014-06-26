#### [Quick overview podcast][overview]

[![Quick overview podcast](images/car-overview.jpg)][overview]

Learn more about MapDB in quick video podcast.


#### [Learn more][intro]
[![Learn more](images/car-intro.png)][intro]

MapDB provides concurrent Maps, Sets and Queues backed by disk storage or off-heap memory.


#### [Benchmarks][benchmarks]

[![benchmarks](images/car-benchmarks.jpg)][benchmarks]

MapDB is probably the fastest pure java database. Checkout our benchmarks.


#### [Features][features]

[![features](images/car-features.png)][features]

CRUD, off-heap, append only store.. we have them all. MapDB is highly modular and flexible


[overview]: http://youtu.be/_KGDwwEP5js
[intro]: 02-getting-started.html
[benchmarks]: /benchmarks.html
[features]: /features.html


---


News
----

* 2014-06-26 [MapDB 1.0.4 released](http://www.mapdb.org/changelog.html#Version_104_2014-06-26). Fixed file locking on Windows.

* 2014-06-08 [MapDB 1.0.3 released](http://www.mapdb.org/changelog.html#Version_103_2014-06-08). Fixed new space allocation, updated copyrights.

* 2014-06-02 [MapDB 1.0.2 released](http://www.mapdb.org/changelog.html#Version_102_2014-06-02). Fixed Serializer.CompressionWrapper()

* 2014-05-14 [MapDB Roadmap and future plans](http://www.kotek.net/blog/MapDB_Roadmap_and_near_future) blog post.



Follow news:
[RSS](news.xml) |
[Mail-List](https://groups.google.com/forum/?fromgroups#!forum/mapdb-news) |
[Twitter](http://twitter.com/MapDBnews)

Features
--------
* **Concurrent** - MapDB has record level locking and state-of-art concurrent engine. Its performance scales nearly linearly with number of cores. Data can be written by multiple parallel threads.

* **Fast** - MapDB has outstanding performance rivaled only by native DBs. It is result of more than a decade of optimizations and rewrites.

* **ACID** - MapDB optionally supports ACID transactions with full MVCC isolation. MapDB uses write-ahead-log or append-only store for great write durability.

* **Flexible** - MapDB can be used everywhere from in-memory cache to multi-terabyte database. It also has number of options to trade durability for write performance. This makes it very easy to configure MapDB to exactly fit your needs.

* **Hackable** - MapDB is component based, most features (instance cache, async writes, compression) are just class wrappers. It is very easy to introduce new functionality or component into MapDB.

* **SQL Like** - MapDB was developed as faster alternative to SQL engine. It has number of features which makes transition from relational database easier: secondary indexes/collections, autoincremental sequential ID, joints, triggers, composite keys...

* **Low disk-space usage** - MapDB has number of features (serialization, delta key packing...) to minimize disk used by its store. It also has very fast compression and custom serializers. We take disk-usage seriously and do not waste single byte.
