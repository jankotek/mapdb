`DBMaker and DB
==============

MapDB is set of loosely coupled components. One could wire classes such as `CacheMRU`, `StoreWAL` and `BTreeMap` manually, 
byt there are two factory classes to do it for you: `DBMaker` and `DB`. 
They use maker (builder) pattern, so most configuration options are quickly available via code assistant in IDE. 

[DBMaker](apidocs/org/mapdb/DBMaker.html) handles database configuration, creation and opening.
MapDB has several modes and configuration options. Most of those can be set using this class.

[DB](apidocs/org/mapdb/DB.html) represents opened database (or single transaction session). 
It creates and opens collections . It also handles transaction with methods such as `commit()`,
`rollback()` and `close()`.

To open (or create) store use one of `DBMaker.newXXX()` static methods. MapDB has more formats and modes, 
each `newXXX()` uses different: `newMemoryDB()` opens in-memory database backed by `byte[]`, 
`newAppendFileDB()` opens db which uses append-only log files and so on. 

`newXXX()` method is followed by configuration options and `make()` method which applyes all options and returns `DB` object.
This example opens file storage with encryption enabled:

```java
  DB db = DBMaker
    .newAppendFileDB(new File("/some/file"))
    .encryptionEnable("password")
    .make();
```

Once you have DB you may open collection or other record. DB has two types of factory methods: 

`getXXX()` opens existing collection (or record). If collection with given name does not exist, 
it is silently created with default settings and returned. An example:

```java
  NavigableSet treeSet = db.getTreeSet("treeSet");
```  

`createXXX()` creates new collection (or settings) with customized settings. Specialized serializers, 
node size, entry compression and so on affect performance a lot and they are customizable here.

```java
  Atomic.Var<Person> var = = db.createAtomicVar("mainPerson", Person.SERIALIZER);
```

Some  `create` method may use builder style configuration. In that case you may finish with two methods:
`make()` creates new collection, if collection with given name already exists it throws an exception. 
`makerOrGet()` is same, except if collection already exist it does not fail, but returns existing collection. 

```java
  NavigableSet<String> treeSet = db.createTreeSet("treeSet);
    .nodeSize(112)
    .serializer(BTreeKeySerializer.STRING) 
    .makeOrGet();
```


Transactions
---------------

`DB` has methods to handle transaction lifecycle: `commit()`, `rollback()` and `close()`.

```java
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
```

One `DB` object represents single transactions. Examples above use single global transaction, which is sufficient for some usages.
MapDB support concurrent transactions as well with full serializable isolation, optimistic locking and MVCC snapshots. 
In that case we need one extra factory which creates transactions: `TxMaker`. 
We use `DBMaker` to create it, but instead of `make()` we call `makeTxMaker()`

```java
  TxMaker txMaker = DBMaker
    .newMemoryDB()
    .makeTxMaker();
```

And `TxMaker` is than used to create multiple `DB` objects, each representing single transaction:

```java
  DB tx0 = txMaker.makeTx();
  Map map0 = tx0.getTreeMap("testMap");
  map0.put(0,"zero");

  DB tx1 = txMaker.makeTx();
  Map map1 = tx1.getTreeMap("testMap");
  
  DB tx2 = txMaker.makeTx();
  Map map2 = tx1.getTreeMap("testMap");
    
  map1.put(1,"one");
  map2.put(2,"two");
  
  //each map sees only its modifications,
  //map1.keySet() contains [0,1]
  //map2.keySet() contains [0,2]
  
  //persist changes
  tx1.commit();
  tx2.commit();  
  // second commit fails  with write conflict, both maps share single BTree node, 
  // this does not happend on large maps with sufficent number of BTree nodes. 