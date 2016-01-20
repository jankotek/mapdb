package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;


public class htreemap_overflow_remove {

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
        //insert entry manually into both maps for demonstration
        inMemory.put("key", "map");

        //first remove from inMemory
        inMemory.remove("key");
        onDisk.get("key"); // -> not found
        //z
        assertFalse(onDisk.containsKey("key"));
        onDisk.close();
        inMemory.close();
    }
}
