package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Map;


public class caches_hash_table {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()
                .cacheHashTableEnable()
                .cacheSize(1000000)     //optionally change cache size
                .make();
        //z
    }
}
