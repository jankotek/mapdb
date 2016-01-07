Getting started
===============

MapDB has very power-full API, but for 99% of cases you need just two
classes:
`DBMaker <http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html>`__ is a
builder style factory for configuring and opening a database. It has a
handful of static ``newXXX`` methods for particular storage mode.
`DB <http://www.mapdb.org/apidocs/org/mapdb/DB.html>`__ represents
storage. It has methods for accessing Maps and other collections. It
also controls the DB life-cycle with commit, rollback and close methods.

The best places to checkout various features of MapDB are
`Examples <https://github.com/jankotek/MapDB/tree/master/src/test/java/examples>`__.
There is also
`screencast <http://www.youtube.com/watch?v=FdZmyEHcWLI>`__ which
describes most aspects of MapDB.

.. There is :download:`MapDB Cheat Sheet <../down/cheatsheet.pdf>`, a quick reminder of the MapDB capabilities on just two pages.

Maven
-----

MapDB is in Maven Central. Just add the code bellow to your pom file to use
it and replace VERSION. Latest version number and binary jars can be found in
`Maven central <http://mvnrepository.com/artifact/org.mapdb/mapdb>`_.

There are two generations of MapDB, old 1.0 and newer 2.0. At this point I would recommend to use
2.0, despite its still beta, because it seems to be more robust.

.. code:: xml

    <dependency>
        <groupId>org.mapdb</groupId>
        <artifactId>mapdb</artifactId>
        <version>VERSION</version>
    </dependency>

We are working on the new generation of MapDB. It is faster and more reliable. The latest semi-stable build is at
`snapshot repository <https://oss.sonatype.org/content/repositories/snapshots/org/mapdb/mapdb/>`__:

.. code:: xml

    <repositories>
        <repository>
            <id>sonatype-snapshots</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>org.mapdb</groupId>
            <artifactId>mapdb</artifactId>
            <version>2.0.0-SNAPSHOT</version>
        </dependency>
    </dependencies>

Hello World
-----------

Hereafter is a simple example. It opens TreeMap backed by a file in temp
directory. The file is discarded after JVM exits:


.. literalinclude:: ../src/test/java/doc/start_hello_world.java
    :start-after: //a
    :end-before: //z
    :language: java
    :dedent: 8


.. TODO This is a more advanced example, with configuration and write-ahead-log transaction.


What you should know
--------------------

MapDB is very simple to use, however it bites when used in the wrong way.
Here is a list of the most common usage errors and things to avoid:

- Transactions (write-ahead-log) can be disabled with
  ``DBMaker.transactionDisable()``, this will MapDB much faster.
  However, without WAL the store gets corrupted when not closed correctly.

- Keys and values must be immutable. MapDB may serialize them on
  background thread, put them into instance cache... Modifying an
  object after it was stored is a bad idea.

- MapDB is much faster with memory mapped files. But those cause problems
  on 32bit JVMs and are disabled by default. Use ``DBMaker.fileMmapEnableIfSupported()``
  to enable them on 32bit systems.

- There is instance cache which uses more memory, but makes MapDB faster.
  Use ``DBMaker.cacheHashTableEnable()``

- MapDB does not run compaction on the background. You need to call
  ``DB.compact()`` from time to time.

- MapDB file storage can be only opened by one user at time.
  File lock should prevent file being used multiple times.
  But if file lock fails to prevent it, the file will become corrupted when opened (and written into)
  by multiple users.


