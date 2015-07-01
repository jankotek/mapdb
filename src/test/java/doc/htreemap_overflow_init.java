package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class htreemap_overflow_init {

    public static void main(String[] args) throws IOException {
        File file = File.createTempFile("mapdb","mapdb");
        //a
        DB dbDisk = DBMaker
                .fileDB(file)
                .make();

        DB dbMemory = DBMaker
                .memoryDB()
                .make();

        // Big map populated with data expired from cache
        HTreeMap onDisk = dbDisk
                .hashMapCreate("onDisk")
                .make();

        // fast in-memory collection with limited size
        HTreeMap inMemory = dbMemory
                .hashMapCreate("inMemory")
                .expireAfterAccess(1, TimeUnit.SECONDS)
                //this registers overflow to `onDisk`
                .expireOverflow(onDisk, true)
                //good idea is to enable background expiration
                .executorEnable()
                .make();
        //z
    }
}
