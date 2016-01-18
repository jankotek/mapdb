MapDB: Database Engine
=================================

MapDB is an embedded database engine for Java.
It provides Maps and other collections backed by disk or off-heap memory storage.
MapDB is free under Apache License.

MapDB is not a database, but an engine. It is not complete solution, but set of building blocks
such as: memory allocators, caches, storages, indexes, transaction wrappers, serializers etc.
This gives MapDB lot of flexibility and space for performance optimizations.
Ever wanted off-heap cache with limited size and with disk overflow after expiry? Now its easy.

MapDB also has very user friendly API. In many cases it is drop-in replacement for existing Java classes,
for example on-heap Map can be replaced with off-heap Map, with just 20 characters.
It is a pure-java 500K JAR with no dependencies.

There is active community on `Github <https://github.com/jankotek/mapdb>`__. MapDB is sponsored by
`consulting services <http://www.kotek.net/consulting/>`__.

News
----
.. postlist:: 5
   :excerpts:
   :list-style: circle

Follow news on `RSS <http://www.mapdb.org/news.xml>`__ \|
`Mail-List <https://groups.google.com/forum/?fromgroups#!forum/mapdb-news>`__
\| `Twitter <http://twitter.com/MapDBnews>`__


MapDB is used by number of projects, checkout :doc:`changelog`

Doc
--------

.. toctree::
   doc/index

Get started
------------

Add `maven dependency <http://mvnrepository.com/artifact/org.mapdb/mapdb>`_:

.. code:: xml

 <dependency>
    <groupId>org.mapdb</groupId>
    <artifactId>mapdb</artifactId>
    <version>1.0.8</version>
 </dependency>

There is also newer and faster 2.0-beta (replace X with current version):

.. code:: xml

 <dependency>
    <groupId>org.mapdb</groupId>
    <artifactId>mapdb</artifactId>
    <version>2.0-betaX</version>
 </dependency

And simple in-memory example:

.. code:: java

    import org.mapdb.*;

    DB db = DBMaker.newMemoryDB().make();

    ConcurrentNavigableMap treeMap = db.getTreeMap("map");
    treeMap.put("something","here");

    db.commit();
    db.close();


Content
------------

.. toctree::
   format
