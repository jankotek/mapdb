package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class htreemap_overflow_get {

    public static void main(String[] args) throws IOException {
        File file = File.createTempFile("mapdb", "mapdb");
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
        //a
        onDisk.put(1,"one");    //onDisk has content, inMemory is empty
        inMemory.size();        //> 0
        // get method will not find value inMemory, and will get value from onDisk
        inMemory.get(1);        //> "one"
        // inMemory now caches result, it will latter expire and move to onDisk
        inMemory.size();        //> 1
        //z
    }
}
