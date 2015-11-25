package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;


public class cache_size {

    public static void main(String[] args) throws IOException {
        File file = File.createTempFile("mapdb","mapdb");
        //a
        DB db = DBMaker
                .fileDB(file)      //or memory db
                .cacheSize(128)    //change cache size
                .make();
        //z
    }
}
