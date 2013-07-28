MapDB provides concurrent Maps, Sets and Queues backed by disk storage or off-heap-memory. It is a fast and easy to use embedded Java database engine. This project is more than 12 years old, you may know it under name JDBM. MapDB is free as speech and free as beer under
[Apache License 2.0](https://github.com/jankotek/MapDB/blob/master/doc/license.txt).



News
====
* 2013-06-18 Now I work on MapDB [full time](http://kotek.net/blog/MapDB_Reloaded).
* 2013-06-02 MapDB 0.9.3 was released. Fixed data corruption in Write Ahead Log and improved serialization [release notes](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-093-2013-06-02)
* 2013-05-19 MapDB 0.9.2 was released. It fixes bugs and introduces Data Pump, [release notes](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-092-2013-05-19)
* 2013-04-22 Blogpost outlining my future plans with MapDB [has been posted](http://www.kotek.net/blog/MapDB_Future)
* 2013-04-14 MapDB 0.9.1 was just released. It fixes dozen of critical bugs and upgrade is highly recommended.
  Release notes are [here](https://github.com/jankotek/MapDB/blob/master/release_notes.md#version-091-2013-04-14)

Follow news:
[RSS](https://groups.google.com/group/mapdb-news/feed/rss_v2_0_msgs.xml?num=50) |
[Mail-List](https://groups.google.com/forum/?fromgroups#!forum/mapdb-news) |
[Twitter](http://twitter.com/MapDBnews)

Features
========
* **Concurrent** - MapDB has record level locking and state-of-art concurrent engine. Its performance scales nearly linearly with number of cores. Data can be written by multiple parallel threads.

* **Fast** - MapDB has outstanding performance rivaled only by native DBs. It is result of more than a decade of optimizations and rewrites.

* **ACID** - MapDB optionally supports ACID transactions with full MVCC isolation. MapDB uses write-ahead-log or append-only store for great write durability.

* **Flexible** - MapDB can be used everywhere from in-memory cache to multi-terabyte database. It also has number of options to trade durability for write performance. This makes it very easy to configure MapDB to exactly fit your needs.

* **Hackable** - MapDB is component based, most features (instance cache, async writes, compression) are just class wrappers. It is very easy to introduce new functionality or component into MapDB.

* **SQL Like** - MapDB was developed as faster alternative to SQL engine. It has number of features which makes transition from relational database easier: secondary indexes/collections, autoincremental sequential ID, joints, triggers, composite keys...

* **Low disk-space usage** - MapDB has number of features (serialization, delta key packing...) to minimize disk used by its store. It also has very fast compression and custom serializers. We take disk-usage seriously and do not waste single byte.
