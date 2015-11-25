package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.io.File;
import java.io.IOException;


public class performance_memory_heap {

    public static void main(String[] args) throws IOException {
        //a
        DB db = DBMaker
            .heapDB()
            .make();
        //z
    }
}
