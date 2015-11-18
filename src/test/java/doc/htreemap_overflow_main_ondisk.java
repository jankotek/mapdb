package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;


public class htreemap_overflow_main_ondisk {

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
                .expireOverflow(onDisk, false) // <<< false here
                .make();
        
        //add two different entries
        onDisk.put(1, "uno");
        inMemory.put(1, "one");
        //simulate expiration by removing entry
        inMemory.remove(1);
        //data onDisk are not overwritten, inMemory loses
        onDisk.get(1);      //> "uno"
        // inMemory gets repopulated from onDisk
        inMemory.get(1);    //> "uno"

        //add stuff to inMemory and expire it
        inMemory.put(2,"two");
        inMemory.remove(2);
        //onDisk still gets updated, because it did not contained this key
        onDisk.get(2); //> two

        //z
    }
}
