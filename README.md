JDBM provides TreeMap and HashMap backed by disk storage. It is a fast,
scalable and easy to use embedded Java database. It is tiny (160KB jar),
yet packed with features such as transactions, space efficient serialization,
instance cache and transparent compression/encryption.

JDBM (Java Database Manager) aims to be a simple, yet powerful database
engine for Java. It provides great service for data processing, caching,
visualisation and persistence. JDBM has been around since the year 1999
and is currently at the 4th generation.

Main features:
==============

* Drop-in replacement for ConcurrentTreeMap and ConcurrentHashMap.

* Outstanding performance comparable to low-level C++ DBs (TokyoDB, LevelDB)

* Scales well on multi-core CPUs (fine grained locks, concurrent trees)

* Very easy configuration using builder classes

* Space-efficient transparent serialization. 

* Instance cache to minimise deserialization overhead

* Various write modes (transactional journal, direct, async or append)
to match various requirements.

* ACID transactions (only one-global transaction at a time; JDBM does not have concurrent transactions).

* Very flexible; works equally well on an Android phone and
a supercomputer with multi-terabyte storage.

* Modular & extensible design; most features (compression, cache,
async writes) are just `RecordManager` wrappers. Introducing
new functionality (such as network replication) is very easy.

* Highly embeddable; 130 KB no-deps jar (50KB packed), pure java,
low memory & CPU overhead.

* Transparent encryption, compression and other stuff you may expect
from a complete database.

Intro
======

JDBM is in [maven-central repository](http://search.maven.org/#browse%7C-92308161), but snapshot are usually more stable.

Quick example:

        import net.kotek.jdbm.*;


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


Troubleshooting
===============

JDBM uses chained exception so user does not have to write try catch blocks.
IOException is usually wrapped in IOError which is unchecked. So please always check root exception in chain.

**java.lang.OutOfMemoryError: Map failed** -
JDBM can not map file into memory. Make sure you are using latest JVM (7+).
Common problem on Windows (use Linux).

**InternalError, Error, AssertionFailedError, IllegalArgumentException, StackOverflowError and so on** -
There was an problem in JDBM. It is possible that file store was corrupted thanks to an internal error or disk failure.
Disabling cache  `DBMaker.cacheDisable()` or async writes `DBMaker.asyncWriteDisable()` may workaround the problem.
Please [create new bug report](https://github.com/jankotek/JDBM4/issues/new) with code to reproduce this issue.

**OutOfMemoryError: GC overhead limit exceeded** -
Your app is creating new object instances faster then GC can collect them.
When using Soft/Weak cache use Hard cache to reduce GC overhead (is auto cleared when free memory is low).
There is JVM parameter to disable this assertion.

**Can not delete db files on Windows** -
Common problem with memory mapped files on Windows (use Linux). Make sure DB is closed correctly. Also use newest JVM.

**Computer becomes very slow during DB writes** -
JDBM uses all available CPU to speedup writes. Try to insert Thread.sleep(1) into your code, or lower thread priority.

**File locking, OverlappingFileLockException, some IOError** -
You are trying to open file already opened by another JDBM. Make sure that you `DB.close()` store correctly.
Some operating systems (Windows) may leave file lock even after JVM is terminated.
You may also try to open database in read-only mode.

**Strange behavior in collections** -
Maps and Sets in JDBM should be drop-in replacement for `java.util` collections. So any significant difference is  a bug.
Please [create new bug report](https://github.com/jankotek/JDBM4/issues/new) with code to reproduce issue.

Support
=======
Anything with stack-trace and reproduction code goes into [new bug report](https://github.com/jankotek/JDBM4/issues/new).

Answers for trivial questions (how I open database?) are in documentation.
If documentation does not provide answer, it is a bug, please create [new bug report](https://github.com/jankotek/JDBM4/issues/new).

For small improvements (add method, new wrapper) [create bug report](https://github.com/jankotek/JDBM4/issues/new).

There is [mail-group](mailto:jdbm@googlegroups.com) for discussion, general problems (performance, data-modeling) and
suggestions. You may also browse [group archive](http://groups.google.com/group/jdbm).

JDBM does not have commercial support (it is my hobby project). However you may always
[buy me a gift](http://www.amazon.co.uk/gp/registry/registry.html?ie=UTF8&id=2CIB8H24EE6R3&type=wishlist)
to get my attention.

Last option is to [contact me directly](mailto:jan at kotek dot net).
However I prefer public bug tracker and mail-conference, so other people may also find solution.