Secondary Collections
======================

Rational databases have very good system of primary and secondary indexes, tables and views. It has clear benefits for extensibility, clarity and robustness. On other side it has limitations for scalability and performance. MapDBs Secondary Collections are *poor man`s* SQL tables. It brings most benefits without sacrificing flexibility.

Secondary Collections are very simple and flexible way to access and expand primary collections. Primary collection is authoritative source of data, and is modified by user. Secondary collection contains records derived from primary. Secondary is bind to primary and updated by listener when primary collection is modified. Secondary should not be modified directly by user.

Secondary Collections are typically used in three ways:

 * Secondary Keys (indexes) to efficiently access records in primary collection.

 * Secondary Values to expand primary collection, while keeping primary small and fast.

 * Aggregation to groups. For example to list all high-income customers.

Primary Collection is typically stored in MapDB. The requirement is that it provides [modification listener](/apidocs/org/mapdb/Bind.MapWithModificationListener.html) triggered on entry update, insert or removal. There is no such requirement for Secondary Collection. Secondary may be stored in MapDB, but it can also be usual Java Collection such as java.util.TreeSet.

Primary and Secondary collection are bind together. There is [Bind](http://www.mapdb.org/apidocs/org/mapdb/Bind.html) class with static methods to establish binding. Binding adds modification listener to primary collection and changes secondary collection accordingly. It also removes old entries from secondary if primary entry gets deleted or modified.

Bind relation is not persistent, so binding needs to be restored every time store is reopened. If secondary collections is empty when binded, entire primary is traversed and secondary is filled accordingly.

Consistency
-------------

Consistency between primary and secondary collections is on 'best-effort' basis. Two concurrent threads might observe
 secondary contains old values while primary was already updated. Also if secondary is on heap, while primary is in transactional store which gets rolled back, secondary will become inconsistent with primary (its changes were not rolled back.

Secondary collection is updated in [serializable fashion](https://en.wikipedia.org/wiki/Serializability). This means that if two concurrent thread update primary, secondary collection is updated with 'winner'. TODO verify this is true, update this paragraph after [issue](https://github.com/jankotek/MapDB/issues/226) is closed.

There are some best practices for Secondary Collections to handle this:

 * Secondary Collections must be thread safe. Either use MapDB or `java.util.concurrent.*` collections. Other (but not optimal) option is to use `Collections.synchronized*()` wrappers.

 * When using concurrent transactions, do not mix collections from multiple transactions. If primary gets rollback, secondary will not be updated if its not within the same transaction.

 * Keep binding minimal. It should only transform one value into other, without dependency on third collections.

Performance
-----------

To import large dataset, you should not enable binding until primary collection has finished its import.
Also there might be more efficient way to pre-fill secondary collection (for example with data pump).
