package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;

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
