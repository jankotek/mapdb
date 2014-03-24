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

* 2014-03-24 [MapDB 0.9.11 was released](http://www.mapdb.org/changelog.html#Version_0911_2014-03-24). Fixed various race conditions and data corruptions. Storage format completely changed.

* 2014-02-18 [MapDB 0.9.10 was released](http://www.mapdb.org/changelog.html#Version_0910_2014-02-18). Fixed data corruption with disabled transaction. In-memory cache now has memory size limit.

* 2014-01-29 [MapDB 0.9.9 was released](http://www.mapdb.org/changelog.html#Version_099_2014-01-29). TxMaker was fixed. TreeSet did not worked, fix has incompatibile format change.

* 2013-12-30 [MapDB 0.9.8 was released](http://www.mapdb.org/changelog.html#Version_098_2013-12-30). API and store format should be stable now. RAF on by default. Append-Only store postponed...

* 2013-11-06 [MapDB 1.0 ](http://kotek.net/blog/MapDB_1_in_january) will be released in January.


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
