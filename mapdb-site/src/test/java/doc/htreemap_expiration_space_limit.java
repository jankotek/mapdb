package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Map;


public class htreemap_expiration_space_limit {

    @Test
    public void run() {

        DB db = DBMaker.memoryDB().make();
        //a
        // Off-heap map with max size 16GB
        Map cache = db
                .hashMap("map")
                .expireMaxSize(16 * 1024*1024*1024)
                .expireAfterGet()
                .create();
        //z
    }
}
