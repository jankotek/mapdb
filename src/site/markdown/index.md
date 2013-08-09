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
* 2013-08-09 [MapDB 0.9.4 was released](http://www.mapdb.org/changelog.html#Version_094_2013-08-09). It is not backward compatible with previous versions. Many changes and improvements.

* 2013-06-18 Now I work on MapDB [full time](http://kotek.net/blog/MapDB_Reloaded).

* 2013-06-02 MapDB 0.9.3 was released. Fixed data corruption in Write Ahead Log and improved serialization [release notes](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-093-2013-06-02)

* 2013-05-19 MapDB 0.9.2 was released. It fixes bugs and introduces Data Pump, [release notes](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-092-2013-05-19)

* 2013-04-22 Blogpost outlining my future plans with MapDB [has been posted](http://www.kotek.net/blog/MapDB_Future)

* 2013-04-14 MapDB 0.9.1 was just released. It fixes dozen of critical bugs and upgrade is highly recommended.
  Release notes are [here](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-091-2013-04-14)

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
