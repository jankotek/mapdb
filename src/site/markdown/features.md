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

* ACID transactions with full MVCC isolation (not yet implemented)

* Very fast snapshots (not yet implemented)

* Very flexible; works equally well on an Android phone and
a supercomputer with multi-terabyte storage (Android not yet tested).

* Modular & extensible design; most features (compression, cache,
async writes) are just `Engine` wrappers. Introducing
new functionality (such as network replication) is very easy.

* Highly embeddable; 200 KB no-deps jar (50KB packed), pure java,
low memory & CPU overhead.

* Transparent encryption, compression, CRC32 checksums and other optional stuff.


