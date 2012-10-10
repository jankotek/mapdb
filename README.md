JDBM provides TreeMap and HashMap backed by disk storage. It is a fast,
scalable and easy to use embedded Java database. It is tiny (130KB jar),
yet packed with features such as transactions, space efficient serialization,
instance cache and transparent compression/encryption.

JDBM (Java Database Manager) aims to be a simple, yet powerful database
engine for Java. It provides great service for data processing, caching,
visualisation and persistence. JDBM has been around since the year 1999
and is currently at the 4th generation.

Main features:

* Drop-in replacement for ConcurrentTreeMap and ConcurrentHashMap.

* Outstanding performance comparable to low-level C++ DBs (TokyoDB, LevelDB)

* Scales well on multi-core CPUs (fine grained locks, concurrent trees)

* Very easy configuration using builder classes

* Space-efficient transparent serialization. 

* Instance cache to minimise serialization overhead

* Various write modes (transactional journal, direct, async or append)
to match various requirements.

* Very flexible; works equally well on an Android phone and
a supercomputer with multi-terabyte storage.

* Modular & extensible design; most features (compression, cache,
async writes) are just `RecordManager` wrappers. Introducing
new functionality (such as network replication) is very easy.

* Highly embeddable; 130 KB no-deps jar (50KB packed), pure java,
low memory & CPU overhead.

* Transparent encryption, compression and other stuff you may expect
from a complete database.

Limitations:

* JDBM allows only a single global transaction at a time. There are no
concurrent transactions, MVCC or transaction isolations. I have some
ideas  about how-to fix it, but no resources.

* JDBM does not communicate over network. There are some thoughts
for client-server architecture, replication and clustering; however
this has low priority.


Intro
======
Quick example
-------------

    import net.kotek.jdbm.*;


        //Configure and open database using builder pattern.
        //All options are available with code auto-completion.
        DB db = DBMaker.newFileDB(new File("testdb"))
                .closeOnJvmShutdown()
                .encryptionEnable("password")
                .make();

        //open an collection, TreeMap has better performance then HashMap
        ConcurrentSortedMap<Integer,String> map = db.getTreeMap("collectionName");

        map.put(1,"one");
        map.put(2,"two");
        //map.keySet() is now [1,2] even before commit

        db.commit();  //persist changes into disk

        map.put(3,"three");
        //map.keySet() is now [1,2,3]
        db.rollback(); //revert recent changes
        //map.keySet() is now [1,2]

        db.close();
