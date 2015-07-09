package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;


public class cache_hardref {

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
