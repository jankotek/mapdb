JDBM4 provides HashMap and TreeMap backed by disk storage. It is fast and easy to use embedded Java database.

Currently there is only early development version. There is not even user friendly API yet.
Only ConcurrentHashMap is implemented. To test it use following code:

    import net.kotek.jdbm.*;
    RecordStore db = new RecordStoreCache("filename",true);
    HashMap2 map = new HashMap2(db,0L);
    //do something with map
    db.close()

To reopen map you need to save its rootRecid between sessions:

    long rootRecid = map.rootRecid; //save this number somewhere
    //restart JVM or whatever, and latter reopen map:
    RecordStore db = new RecordStoreCache("filename",true);
    HashMap2 map = new HashMap2(db,rootRecid);
    //do something with map, it is populated with previous data
    db.close()
  

What works (or should)

* low level RecordStorage (basically Map<long,byte[]>)
* serializers for most `java.lang.*` and `java.util.*` classes
* Hard Reference Cache with autoclear on memory low
* Full thread safety
* Concurrent scalability should be nearly linear with number of cores (even writes)
* All writes are done in background thread

What is not there yet

* Transactions
* Weak/Soft/MRU cache
* POJO serialization
* TreeMap aka BTree
* Friendly interface (DB & DBMaker)
* Max record size is currently 64KB. 


  
