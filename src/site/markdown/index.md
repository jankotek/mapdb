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


[overview]: http://www.youtube.com/watch?v=FdZmyEHcWLI
[intro]: /intro.html
[benchmarks]: /benchmarks.html
[features]: /features.html


---


News
----

* 2013-11-06 [MapDB 1.0 ](http://kotek.net/blog/MapDB_1_in_january) will be released in January.

* 2013-10-28 [MapDB 0.9.7 was released](http://www.mapdb.org/changelog.html#Version_097_2013-10-28). Fixed locking issues on Windows. Concurrent Transactions (TxMaker) reworked and finally fixed.

* 2013-09-27 [MapDB 0.9.6 was released](http://www.mapdb.org/changelog.html#Version_096_2013-09-27). Concurrent Transactions (TxMaker) almost fixed. Backward incompatible store format change. Snapshots are no longer enabled by default.

* 2013-09-25 [MapDB and the road ahead](http://kotek.net/blog/MapDB_and_the_road_ahead)

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
