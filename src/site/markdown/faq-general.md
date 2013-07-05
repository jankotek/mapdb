General questions
===================


Is MapDB free as a beer?
------------------------
Yes! MapDB is distributed under [Apache 2 license](https://github.com/jankotek/MapDB/blob/master/license.txt),
it can be modified, sold and used in commercial product. All documentation, unit tests and build scripts are
published as well. I believe that MapDB has good chance to become the de facto standard Java storage engine.
For that it needs permissive license and even LGPL could ruin its chances.

Is MapDB commercial project?
------------------------------
Yes and no. It started as a hobby project. Right now I work on MapDB full-time. I am trying to find
business model which would allow me to continue full-time work. You
[can hire me](http://kotek.net/contact) for commercial support and consultancy.


What sort of DB is this? (graph, sql, key-value...)
---------------------------------------------------
MapDB is *not* a database, it is db engine on top of which some database can be build,
similar way as MyISAM is engine for MySQL database. I try to keep MapDB as generic as possible.
For that reason MapDB does not have its own network protocol or query language.

On other side MapDB is very flexible, has many utilities and offers rich set if data structures (trees, queues...).
So it is easy to use MapDB API directly similar way as SQL or Graph DB would be used.

Are there any design goals?
---------------------------
MapDB started as storage engine for astronomical desktop application. It had two design goals: minimal overhead
and simplicity. It is a reason why it contains less usual stuff such as serialization, instance cache or data pump.
Those goals were reason for stripping-off several layers of abstractions (page blocks, query language...)
Over time third goal emerged: provide alternative to traditional java memory model. That is why I added
atomic variables or compare-and-swap primitives.
Today I see MapDB as a framework which enables Java to process big data easily.

Does it have ACID transactions?
---------------------------------
Yes. MapDB has several modes and one of them is Append-Only (or WAL) transactions with full MVCC isolation.
It also has several options to trade letters in ACID for performance. For example consistency
can be also achieved with compare-and-swap, heavy transactions might not be necessary.


Can MapDB be used as in-memory cache for other database?
-----------------------------------------------------
Absolutely. MapDB has very small overhead and low space usage.
*(TODO unfinished) HashMap/HashSet also has time expiration after which items is removed.*
And there is also *(TODO unfinished) cache provider for Hibernate and other enterprise frameworks.*


Does MapDB have network server?
--------------------------------
No. It is database engine, so network interface should be provided by other project which sits on top.
TODO: There is plan to implement Redis server protocol, so MapDB would be drop in replacement for REDIS

Does it have replication/clustering?
-------------------------------------------
Not yet. TODO: There is plan to implement master/slave replication similar to MySQL.






