package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;


public class htreemap_overflow_main_inmemory {

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

        //a
        HTreeMap inMemory = dbMemory
                .hashMapCreate("inMemory")
                .expireOverflow(onDisk, true) // <<< true here
                .make();

        //add two different entries
        onDisk.put(1, "uno");
        inMemory.put(1, "one");
        //simulate expiration by removing entry
        inMemory.remove(1);
        //data onDisk are overwritten, inMemory wins
        onDisk.get(1);      //> "one"
        // inMemory gets repopulated from onDisk
        inMemory.get(1);    //> "one"
        //z
    }
}
