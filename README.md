JDBM provides HashMap and TreeMap backed by disk storage. It is fast, scalable and easy to use embedded Java database.
It has minimalistic design with standalone jar taking only 160KB. Yet it is packed with features such as instance cache,
space efficient serialization, transactions and concurrently scalable BTree/HTree.

JDBM is very fast (with disk store it can outperform some in-memory databases). Thanks to its compact design it is
easy to optimize and has minimal overhead. It is also very 'hackable' and can be easily bended for your
own purposes. JDBM is tiny, yet it easily handles 1e9 records in multi-terabyte store and scales well
in multi-threaded environment.

JDBM is opensource and free-as-beer under Apache License 2.0. There is no catch and no strings attached.
(It is also beer-ware, if you like it, you should come to Galway and buy me a beer :-) ).

JDBM4 is currently under development. It is usable, but some stuff is not implemented yet:

* Transactions
* Weak/Soft/MRU cache (only hard ref cache implemented)
* POJO serialization (only basic serializer for java.util and java.lang classes)
* Max record size is currently 64KB.
* Defrag


An usage example:


        import net.kotek.jdbm.*;

        DB db = DBMaker.newFileDB("filename")
                    .transactionDisable() //transactions are not implemented yet
                    .make();

        ConcurrentSortedMap<Integer, String> map = db.getTreeMap("treeMap");
        map.put(1,"some string");
        map.put(2,"some other string");

        db.close(); //make sure db is correctly closed!!

Maven repository with weekly snapshots is coming soon!



  
