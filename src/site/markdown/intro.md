Intro
======
MapDB has very power-full API, but for 99% cases you need just two classes: [DBMaker](http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html) is builder style factory for configuring and opening a database. It has handful of static 'newXXX' methods for particular storage mode. [DB](http://www.mapdb.org/apidocs/org/mapdb/DB.html) represents storage. It has methods for accessing Maps and other collections. It also controls DB life-cycle with commit, rollback and close methods.

Best place to checkout various features of MapDB are [Examples](https://github.com/jankotek/MapDB/tree/master/src/test/java/examples). There is also [screencast](http://www.youtube.com/watch?v=FdZmyEHcWLI) which describes most aspects of MapDB.


Maven
------
MapDB is in Maven Central. Just add code bellow to your pom file to use it. You may also download jar files directly from [repo](http://search.maven.org/#browse%7C845836981).

```xml
<dependency>
    <groupId>org.mapdb</groupId>
    <artifactId>mapdb</artifactId>
    <version>0.9.7</version>
</dependency>
```

There is also repository with [daily builds](https://oss.sonatype.org/content/repositories/snapshots/org/mapdb/mapdb/):

```xml
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
        <version>0.9.8-SNAPSHOT</version>
    </dependency>
</dependencies>
```

Hello World
-----------
Hereafter is a simple example. It opens TreeMap backed by file in temp directory, file is discarded after JVM exit:

```java
import org.mapdb.*;
ConcurrentNavigableMap treeMap = DBMaker.newTempTreeMap()

// and now use disk based Map as any other Map
treeMap.put(111,"some value")
```

More advanced example with configuration and write-ahead-log transaction.

```java
import org.mapdb.*;

// configure and open database using builder pattern.
// all options are available with code auto-completion.
DB db = DBMaker.newFileDB(new File("testdb"))
               .closeOnJvmShutdown()
               .encryptionEnable("password")
               .make();

// open existing an collection (or create new)
ConcurrentNavigableMap<Integer,String> map = db.getTreeMap("collectionName");

map.put(1, "one");
map.put(2, "two");
// map.keySet() is now [1,2]

db.commit();  //persist changes into disk

map.put(3, "three");
// map.keySet() is now [1,2,3]
db.rollback(); //revert recent changes
// map.keySet() is now [1,2]

db.close();
```

What you should know
--------------------
MapDB is very simple to use, however it bites when used wrong way. Here is list of most common usage errors
and things to avoid:

* Transactions (write-ahead-log) can be disabled with <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#transactionDisable()">DBMaker.transactionDisable()</a>, this will speedup writes. However without transactions store gets corrupted when not closed correctly.

* Keys and values must be immutable. MapDB may serialize them on background thread, put them into instance cache... Modifying an object after it was stored is a bad idea.

* MapDB relies on memory mapped files. On 32bit JVM you will need <a href="http://www.mapdb.org/apidocs/org/mapdb/DBMaker.html#randomAccessFileEnable()">DBMaker.randomAccessFileEnable()</a> configuration option to access files larger than 2GB. RAF introduces overhead compared to memory mapped files.

* MapDB does not run defrag on background. You need to call `DB.compact()` from time to time.

* MapDB uses unchecked exceptions. All `IOException` are wrapped into unchecked `IOError`. MapDB has weak error handling and assumes disk failure can not be recovered at runtime. However this does not affects data safety, if you use durable commits.
