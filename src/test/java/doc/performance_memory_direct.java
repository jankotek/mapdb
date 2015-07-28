package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.IOException;


public class performance_memory_direct {

    public static void main(String[] args) throws IOException {
        //a
        // run with: java -XX:MaxDirectMemorySize=10G
        DB db = DBMaker
            .memoryDirectDB()
            .make();
        //z
    }
}
