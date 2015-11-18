package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.util.concurrent.TimeUnit;

public class CacheOverflow {

    public static void main(String[] args) throws InterruptedException {
        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();

        // Big map into populated with data expired from cache
        // It is on slow, but large medium such as disk.
        // (for simplicity here we use the same db)
        HTreeMap onDisk = db.hashMapCreate("onDisk")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.STRING)
                .make();

        // fast in-memory collection with limited size
        // its content is moved to disk, if not accessed for some time
        HTreeMap inMemory = db.hashMapCreate("inMemory")
                .expireAfterAccess(1, TimeUnit.SECONDS)

                // register overflow
                .expireOverflow(onDisk, true)

                .executorEnable()
                .make();


        //add some data to onDisk
        onDisk.put(1,"one");
        onDisk.put(2, "two");
        onDisk.put(3, "three");

        // in memory is empty
        inMemory.size();  // > 0

        // When an entry is not found inMemory, it takes content from onDisk
        inMemory.get(1);  // > one

        // inMemory now contains one item
        inMemory.size();  // > 1

        // wait until data is expired
        Thread.sleep(10000);

        // inMemory is now empty
        inMemory.size();  // > 0

        /*
         * This code snippet removes data from both collections
         */

        //Add some random data, this just simulates filled cache
        inMemory.put(1,"oneXX");

        //first remove from inMemory, when removed, listener will move it to onDisk map
        inMemory.remove(1);

        // onDisk now contains data removed from inMemory
        // (there is no difference between expiration and manual removal)
        // So remove from onDisk as well
        onDisk.remove(1);

        /*
         * There are two ways to add data.
         *
         * Add them to onDisk. This is more durable, since you can commit and fsync data.
         * In this case data are loaded to inMemory automatically when accessed.
         *
         * Add them to inMemory. OnDisk will get updated after data expire,
         * this might take long time (or never) if data are hot and frequently accessed.
         * Also it might not be durable, since some data only exist in memory.
         * But it is very fast for frequently updated values, since no data are written to disk
         * when value changes, until necessary.
         *
         * Depending on which collection is authoritative you should set 'overwrite' parameter
         * in 'expireOverflow()' method. in first case sets it to 'false', in second set it to 'true's
         *
         */

        //first option, update on disk
        onDisk.put(4, "four");
        inMemory.get(4); //>  four

        //however if onDisk value gets updated (not just inserted), inMemory might have oldValue
        // in that case you should update collections
        onDisk.put(4, "four!!!!");
        inMemory.get(4); //>  four

        //second option, just update inMemory, change will eventually overflow to onDisk
        inMemory.put(5, "five");
        Thread.sleep(10000);
        onDisk.get(5); //>  five

        db.close();
    }

}
