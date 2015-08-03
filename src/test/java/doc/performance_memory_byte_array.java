package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;

import java.io.IOException;


public class performance_memory_byte_array {

    public static void main(String[] args) throws IOException {
        //a
        DB db = DBMaker
            .memoryDB()
            .make();
        //z
    }
}
