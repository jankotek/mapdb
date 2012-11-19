MapDB
===============
MapDB provides concurrent TreeMap and HashMap backed by disk storage. It is a fast,
scalable and easy to use embedded Java database. It is tiny (160KB jar),
yet packed with features such as transactions, space efficient serialization,
instance cache and transparent compression/encryption.
It also has outstanding performance rivaled only by C++ embedded db engines.

MapDB is free as speech and free as beer under [Apache License 2.0](https://github.com/jankotek/MapDB/blob/master/doc/license.txt).
However author would appreciate
[small donation](http://www.amazon.co.uk/gp/registry/registry.html?ie=UTF8&id=2CIB8H24EE6R3&type=wishlist),
it would improve this project and ensure it stays free in the future.
Right now we urgently need [decent SSD drive](http://www.amazon.co.uk/Intel-Series-240GB-Solid-State/dp/B0072R286S/ref=wl_it_dp_v_S_nC?ie=UTF8&colid=2CIB8H24EE6R3&coliid=I17CIOMF6C7R4M)
to speedup MapDB testing.

News
=====
3/11/2012 - JDBM4 project was [renamed to MapDB](http://kotek.net/blog/JDBM4_renamed_to_MapDB)

Main features:
==============

* Drop-in replacement for ConcurrentTreeMap and ConcurrentHashMap.

* Outstanding performance comparable to low-level native DBs (TokyoCabinet, LevelDB)

* Scales well on multi-core CPUs (fine grained locks, concurrent trees)

* Very easy configuration using builder class

* Space-efficient transparent serialization. 

* Instance cache to minimise deserialization overhead

* Various write modes (transactional journal, direct, async or append)
to match various requirements.

* ACID transactions (only one-global transaction at a time; MapDB does not have concurrent transactions).

* Very flexible; works equally well on an Android phone and
a supercomputer with multi-terabyte storage.

* Modular & extensible design; most features (compression, cache,
async writes) are just `Engine` wrappers. Introducing
new functionality (such as network replication) is very easy.

* Highly embeddable; 130 KB no-deps jar (50KB packed), pure java,
low memory & CPU overhead.

* Transparent encryption, compression and other stuff you may expect
from a complete database.

Intro
======
MapDB uses Maven build system. There is snapshot repository updated every a few days.
To use it add code bellow to your `pom.xml`. You may also download binaries
[directly](https://oss.sonatype.org/content/repositories/snapshots/org/mapdb/MapDB/0.9-SNAPSHOT/).

    <repositories>
        <repository>
            <id>mapdb-snapshots</id>
            <url>https://oss.sonatype.org/content/repositories/snapshots</url>
        </repository>
    </repositories>

    <dependencies>
        <dependency>
            <groupId>org.mapdb</groupId>
            <artifactId>mapdb</artifactId>
            <version>0.9-SNAPSHOT</version>
                </dependency>
    </dependencies>


Quick example:

        import org.mapdb.*;


        //Configure and open database using builder pattern.
        //All options are available with code auto-completion.
        DB db = DBMaker.newFileDB(new File("testdb"))
                .closeOnJvmShutdown()
                .encryptionEnable("password")
                .make();

        //open an collection, TreeMap has better performance then HashMap
        ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");

        map.put(1,"one");
        map.put(2,"two");
        //map.keySet() is now [1,2] even before commit

        db.commit();  //persist changes into disk

        map.put(3,"three");
        //map.keySet() is now [1,2,3]
        db.rollback(); //revert recent changes
        //map.keySet() is now [1,2]

        db.close();

What you should know
====================
* Transactions can be disabled, this will speedup writes. However without transactions
store gets corrupted easily without proper close.

* MapDB relies on mapped memory heavily. NIO implementation in JDK6 seems to be failing randomly under heavy load.
MapDB works best with JDK7

* There are two collections TreeMap (B+Tree) and HashMap (HTree). TreeMap is
optimized for small keys, HashMap works best with larger key.

* MapDB does not run defrag on background. You need to call `DB.defrag()` from time to time.

* MapDB uses unchecked exceptions. All `IOException` are wrapped into unchecked `IOError`.

* Maximal serialized record size is 64KB. This will be fixed in future.

Troubleshooting
===============

MapDB uses chained exception so user does not have to write try catch blocks.
IOException is usually wrapped in IOError which is unchecked. So please always check root exception in chain.

**java.lang.OutOfMemoryError: Map failed** -
MapDB can not map file into memory. Make sure you are using latest JVM (7+).
Common problem on Windows (use Linux).

**InternalError, Error, AssertionFailedError, IllegalArgumentException, StackOverflowError and so on** -
There was an problem in MapDB. It is possible that file store was corrupted thanks to an internal error or disk failure.
Disabling cache  `DBMaker.cacheDisable()` or async writes `DBMaker.asyncWriteDisable()` may workaround the problem.
Please [create new bug report](https://github.com/jankotek/JDBM4/issues/new) with code to reproduce this issue.

**OutOfMemoryError: GC overhead limit exceeded** -
Your app is creating new object instances faster then GC can collect them.
When using Soft/Weak cache use Hard cache to reduce GC overhead (is auto cleared when free memory is low).
There is JVM parameter to disable this assertion.

**Can not delete db files on Windows** -
Common problem with memory mapped files on Windows (use Linux). Make sure DB is closed correctly. Also use newest JVM.

**Computer becomes very slow during DB writes** -
MapDB uses all available CPU to speedup writes. Try to insert Thread.sleep(1) into your code, or lower thread priority.

**File locking, OverlappingFileLockException, some IOError** -
You are trying to open file already opened by another MapDB. Make sure that you `DB.close()` store correctly.
Some operating systems (Windows) may leave file lock even after JVM is terminated.
You may also try to open database in read-only mode.

**Strange behavior in collections** -
Maps and Sets in MapDB should be drop-in replacement for `java.util` collections. So any significant difference is  a bug.
Please [create new bug report](https://github.com/jankotek/JDBM4/issues/new) with code to reproduce issue.

**Concurrency related issues**
If you ran into hard to replicate concurrent problem (race condition, dead lock), 
I would suggest to record JVM execution with [Chronon](http://www.chrononsystems.com/learn-more/products-overview) 
and submit the record together with your bug report. This would make bug-fix faster.

Support
=======
MapDB has three places where you may find a help. For anything with stack-trace you should create
[new bug report](https://github.com/jankotek/JDBM4/issues/new).
Small feature request goes into bug tracker.
Use it also for trivial questions (how to open collection?) because incomplete documentation is a bug.

There is [mail-group](mailto:jdbm@googlegroups.com) with [archive](http://groups.google.com/group/jdbm).
You should use it for more general discussion (such as "can MapDB support transactional software memory?").
Also questions about performance and data-modeling should go into mail-group.

Last option is to [contact me directly](mailto:jan at kotek dot net).
I prefer public bug tracker and mail-group so others can find answers as well.
Unless you specify your question as confidential, I may forward it to public mail-group.

MapDB is a hobby project and time I can spend on it is limited.
Please always attach code to illustrate/reproduce your problem, so I can fix it efficiently.
You can also [buy me a gift](http://www.amazon.co.uk/gp/registry/registry.html?ie=UTF8&id=2CIB8H24EE6R3&type=wishlist)
to get your problem solved faster.
