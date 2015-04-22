package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;


public class caches_hardref {

    public static void main(String[] args) {
        //a
        DB db = DBMaker
                .memoryDB()
                .cacheHardRefEnable()
                //optionally enable executor, so cache is cleared in background thread
                .cacheExecutorEnable()
                .make();
        //z
    }
}
