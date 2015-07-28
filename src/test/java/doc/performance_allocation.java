package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;


public class performance_allocation {

    public static void main(String[] args) throws IOException {
        File file = File.createTempFile("mapdb","mapdb");
        //a
        DB db = DBMaker
            .fileDB(file)
            .fileMmapEnable()
            .allocateStartSize( 10 * 1024*1024*1024)  // 10GB
            .allocateIncrement(512 * 1024*1024)       // 512MB
            .make();
        //z
    }
}
