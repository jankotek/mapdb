package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;


public class cache_lru {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()

                .cacheLRUEnable()
                .cacheSize(1000000)     //optionally change cache size

                //optionally enable executor, so cache is cleared in background thread
                .cacheExecutorEnable()

                .make();
        //z
    }
}
