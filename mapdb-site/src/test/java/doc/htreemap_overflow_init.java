package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


public class htreemap_overflow_init {

    @Test
    public void run() throws IOException {

        File file2 = File.createTempFile("mapdb","mapdb");
        file2.delete();
        String file = file2.getPath();

        //a
        DB dbDisk = DBMaker
                .fileDB(file)
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
        //z
        onDisk.close();
        inMemory.close();
    }
}
