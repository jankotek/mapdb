BTreeMap
==========

``BTreeMap`` provides ``TreeMap`` and ``TreeSet`` for MapDB. It is based on lock-free concurrent B-Linked-Tree.
It offers great performance for small keys and has good vertical scalability.

TODO explain compressions

TODO describe B-Linked-Tree

Parameters
---------------

BTreeMap has optional parameters which can be specified by using maker.:

The most important among them are **serializers**. General serialization has
some guessing and overhead, so better performance is always achieved if
more specific serializers are  used. To specify the key and value serializer, use the code
bellow. There are dozens ready to use serializers available as static
fields on ``Serializer`` interface:

.. literalinclude:: ../src/test/java/doc/btreemap_serializer.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Another useful parameter is **size counter**. By default, a BTreeMap does not keep
track of its size and calling ``map.size()`` requires linear scan to count
all entries. If you enable size counter, in that case
``map.size()`` is instant, but there is some overhead on inserts.

.. literalinclude:: ../src/test/java/doc/btreemap_counter.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

BTrees store all their keys and values as part of a btree node. The node size affects performance a lot.
A large node means that many keys have to be deserialized on lookup.
A smaller node loads faster, but makes large BTrees deeper and requires more operations.
The default maximal node size is 32 entries and it can be changed in this way:


.. literalinclude:: ../src/test/java/doc/btreemap_nodesize.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Values are also stored as part of BTree leaf nodes. Large values means huge overhead and
on single ``map.get("key")`` 32 values are deserialized, but only a single value returned. In this
case, it is better to store the values outside the leaf node, in a separate record. In this case leaf node
only has a 6 byte recid pointing to the value.

Large values can also possible be compressed to save space. This example stores places
node outside BTree leaf nodes and applies compression on each value:

.. literalinclude:: ../src/test/java/doc/btreemap_compressed.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

BTreeMap needs to sort its key somehow. By default it relies on ``Comparable`` interface implemented by most Java classes.
In case this interface is not implemented, a key serializer must be provided. One can for example compare Object arrays:

.. literalinclude:: ../src/test/java/doc/btreemap_object_array.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Also primitive arrays can be used as keys. One can replace ``String`` by ``byte[]``,which directly leads to better
performance:

.. literalinclude:: ../src/test/java/doc/btreemap_byte_array.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8

Key serializers
-----------------------

BTreeMap owns its performance to the way it handles keys. Lets illustrate this on an example with ``Long`` keys.

A long key occupies 8 bytes after serialization. To minimize space usage one could pack this value to make it smaller.
So the number 10 will occupy a single byte, 300 will take 2 bytes, 10000 three bytes etc. To make keys even more
packable, we need to store them in even smaller values. The keys are sorted, so lets use delta compression.
This will store the first value in full form and then only the differences between consecutive numbers.

Another improvement is to make the deserialization faster. In normal ``TreeMap`` keys are stored in wrapped form, such as
``Long[]``. That has a huge overhead, as each key requires new pointer, class header... BTreeMap will store keys in primitive
array ``long[]``. And finally if keys are small enough it can even fit into ``int[]``.
And because an array has better memory locality, there is a huge performance increase on binary searches.

It is simple to do such optimisation for numbers. But BTreeMap also applies that on other keys, such as ``String``
(common prefix compression,single ``byte[]`` with offsets), ``byte[]``, ``UUID``, ``Date`` etc.

This sort of optimization is used automatically. All you have to do is provide the specialized key serializer:
``.keySerializer(Serializer.LONG)``.

There are several options and implementations to pack keys. Have a look at
`BTreeKeySerializer <http://www.mapdb.org/apidocs/org/mapdb/BTreeKeySerializer.html>`_ for more details.

TODO this is major feature, document details and add benchmarks

Data Pump
----------------

TODO data pump

Fragmentation
-------------
A trade-off for lock-free design is fragmentation after deletion. The B-Linked-Tree does not delete btree nodes after
entry removal, once they become empty. If you fill a BTreeMap and then remove all entries, about 40% of space will not
be released. Any value updates (keys are kept) are not affected by this fragmentation.

This fragmentation is different from storage fragmentation, so ``DB.compact()`` will not help.
A solution is to move all content into a new ``BTreeMap``. As it is very fast with Data Pump streaming,
new Map will have zero fragmentation and better node locality (in theory disk cache friendly).

TODO provide utils to move BTreeMap content
TODO provide statistics to calculate BTreeMap fragmentation

In the future, we will provide BTreeMap wrapper, which will do this compaction automatically. It will use three collections:
the first ``BTreeMap`` will be read-only and will also contain the data. The econd small map will contain updates. Periodically a third map
will be produced as a merge of first two, and will be swapped with the primary.
``SSTable``'s in Cassandra and other databases works in a similar way.

TODO provide wrapper to compact/merge BTreeMap content automatically.

Composite keys and multimaps
-----------------------------

MapDB 1.0 had tuples that were replaced in 2.0 with ``Object[]``.

TODO composite keys

TODO multimap



Compared to HTreeMap
---------------------

BTreeMap is better for smaller keys, such as numbers and short strings.

TODO compare to HTreeMap


