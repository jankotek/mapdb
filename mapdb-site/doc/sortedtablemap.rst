Sorted Table Map
====================

``SortedTableMap`` is inspired by Sorted String Tables from Cassandra.
It stores keys in file (or memory store) in fixed size table, and uses binary search.
There are some tricks to support variable-length entries and to decrease space usage.
Compared to ``BTreeMap`` it is faster, has zero fragmentation, but is readonly.

``SortedTableMap`` is read-only and does not support updates.
Changes should be applied by creating new Map with Data Pump.
Usually one places change into secondary map,
and periodically merges two maps into new ``SortedTableMap``.

``SortedTableMap`` is read-only. Its created and filled with content by Data Pump and Consumer:

.. literalinclude:: ../src/test/java/doc/sortedtablemap_init.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Once file is created, it can be reopened:


.. literalinclude:: ../src/test/java/doc/sortedtablemap_reopen.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8


Binary search
----------------
Storage is split into `pages`. Page size is power of two, with maximal size 1MB. First key on each page is stored on-heap.

Each page contains several nodes composed of keys and values. Those are very similar to BTreeMap Leaf nodes.
Node offsets are known, so fast seek to beginning of node is used.

Each node contains several key-value pairs (by default 32). Their organization depends on serializer, but typically
are compressed together (delta compression, LZF..) to save space. So to find single entry, one has to load/traverse
entire node. Some fixed-lenght serializer (Serializer.LONG...) do not have to load entire node to find single entry.

Binary search on ``SortedTableMap`` is performed in three steps:

- First key for each page is stored on-heap in an array. So perform binary search to find page.

- First key on each node can by loaded without decompressing entire node.
  So perform binary search over first keys on each node

- Now we know node, so perform binary search over node keys. This depends on Serializer.
  Usually entire node is loaded, but other options are possible `TODO link to serializer binary search`.

Parameters
--------------

``SortedTableMap`` takes key **serializer** and value serializers. The keys and values are stored together inside
Value Array `TODO link to serializers`. They can be compressed together to save space.
Serializer is trade-off between space usage and performance.

Another setting is **Page Size**. Default and maximal value is 1MB. Its value must be power of two, other valuaes
are rounded up to nearest power of two. Smaller value typically means faster access. But for each page
one key is stored on-heap, smaller Page Size also means larger memory usage.

And finally there is **Node Size**. It has similar implications as BTreeMap node size. Larger node
means better compression, since large chunks are better compressible. But it also means slower access times,
since more entries are loaded to get single entry. Default node size is 32 entries, it should be lowered for large values.

Parameters are set following way

.. literalinclude:: ../src/test/java/doc/sortedtablemap_params.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8


Volume
----------

``SortedTableMap`` does not use ``DB`` object, but operates directly on ``Volume`` (MapDB abstraction over ByteBuffer).
Following example show how to construct various ``Volume`` using in-memory byte array or memory-mapped file:

.. literalinclude:: ../src/test/java/doc/sortedtablemap_volume.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Volume is than passed to ``SortecTableMap`` factory method as an parameter. It is recommended to open existing
Volumes in read-only mode (last param is ``true``) to minimize file locking and simplify your code.

Data Pump sync Volume content to disk, so file based ``SortedTableMap`` is durable once the ``Consumer.finish()``
method exits