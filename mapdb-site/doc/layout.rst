Collection Layout
==========================

Partitioning in databases is usually way to distribute data between multiple stores, tables etc...
MapDB has great flexibility and its partitioning is more complicated.
So in MapDB partitioning is when collection is using more than single Store, to contain its state.

For example ``HTreeMap`` can split key-value entries between multiple disks, while its Hash Table uses in-memory Store
and Expiration Queues are regular on-heap collections.

This chapter gives overview of most partitioning options in MapDB. Details are in separate chapters for each
collection or Store.


Hash Partitioning
~~~~~~~~~~~~~~~~~~~~
HP is well supported in HTreeMap *TODO link*. To achieve concurrency HashMap is split into
segments, each segment is separate HashMap with its own ReadWriteLock. Segment number is calculated from hash.
When expiration is enabled each segment has its own Expiration Queue.

Usually HTreeMap segments share single Store *TODO link*. But each segment can have its own Store, that improves
concurrency and allows to shard HTreeMap across multiple disks.

Range partitioning
~~~~~~~~~~~~~~~~~~~~
Is not currently supported in BTreeMap.

*TODO discus sharding based on Node Recid hash*

*TODO investigate feasibility in BTreeMap*


Time of Update Partitioning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Large BTrees usually has slow updates, due to write amplification *TODO chapter & link*.
In some cases (time series) it makes sense to shard data based on last modification.
Each day (or other interval) has its own store, old data can be removed just by deleting files.
There are various options to handle modifications, delete markers etc...
MapDB supports this with SortedTableMap and CopyOnWriteMap

*TODO this could also work on HTreeMap (sorted by hash)*

Partitioning by Durability
-----------------------------

Durable commits are much slower than non-durable. We have to move data to/from write-ahead-log, sync files,
calculate checksums... Durability Partitioning allows to minimize size of durable data, by moving non essential
data into non-durable store. Trade off is longer recovery time after crash.

Good example is BTree. We really only care about Leaf Nodes, which contains all key-value pairs. Directory nodes (index)
can be easily reconstructed from Leaf Nodes. BTreeMap can use two stores, one with durable commits for leafs,
second non-durable for directory nodes. Pump than reconstructs Directory Store in case of crash.

Name?
----------------------------

*TODO HTreeMap expiration queues onheap, in memory*

*TODO in-memory indexes for HTreeMap and BTreeMap*

*TODO HTreeMap IndexTree onheap*

Expiration Partitioning
---------------------------
*TODO HTreeMap disk overflow*

