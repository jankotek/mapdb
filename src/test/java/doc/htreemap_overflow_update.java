package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.io.File;
import java.io.IOException;


public class htreemap_overflow_update {

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

        HTreeMap inMemory = dbMemory
                .hashMapCreate("inMemory")
                .expireOverflow(onDisk, false) // <<< false here
                .make();

        //a

        //put value to on disk
        onDisk.put(1, "one");
        //in memory gets updated from on disk, no problem here
        inMemory.get(1); //> "one"

        //updating just one collection creates consistency problem
        onDisk.put(1,"uno");
        //old content of inMemory has not expired yet
        inMemory.get(1); //> "one"

        //one has to update both collections at the same time
        onDisk.put(1,"uno");
        inMemory.put(1,"uno");
        //z
    }
}
