package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.HTreeMap;

import java.util.Map;


public class htreemap_cache_space_limit {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        // Off-heap map with max size 16GB
        Map cache = DBMaker
                .newCacheDirect(16);
        //z
    }
}
