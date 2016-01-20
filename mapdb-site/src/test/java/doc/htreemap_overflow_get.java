package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;


public class htreemap_overflow_get {

    @Test
    public void run() throws IOException {

        File file = File.createTempFile("mapdb", "mapdb");
        file.delete();
        DB dbDisk = DBMaker
                .fileDB(file.getPath())
                .make();

        DB dbMemory = DBMaker
                .memoryDB()
                .make();

        // Big map populated with data expired from cache
        HTreeMap onDisk = dbDisk
                .hashMap("onDisk")
                .create();

        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMap("inMemory")
                .expireAfterGet(1, TimeUnit.SECONDS)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk)
                //good idea is to enable background expiration
                .expireExecutor(Executors.newScheduledThreadPool(2))
                .create();
        //a
        onDisk.put(1,"one");    //onDisk has content, inMemory is empty
        inMemory.size();        //> 0
        // get method will not find value inMemory, and will get value from onDisk
        inMemory.get(1);        //> "one"
        // inMemory now caches result, it will latter expire and move to onDisk
        inMemory.size();        //> 1
        //z
        assertEquals("one", inMemory.get(1));
        onDisk.close();
        inMemory.close();
    }
}
