package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.concurrent.Executors;


public class concurrency_executor_cache {

    public static void main(String[] args) {
        //a
        DB db = DBMaker.memoryDB()
                // enable executor just for instance cache
                .cacheExecutorEnable()
                // or one can use its own executor
                .cacheExecutorEnable(Executors.newSingleThreadScheduledExecutor())

                //only some caches are using executor for its expirations:
                .cacheHardRefEnable()    //TODO check hardref cache uses executors
                .cacheLRUEnable()        //TODO check LRU cache uses executors
                .cacheWeakRefEnable()
                .cacheSoftRefEnable()

                .make();

        //z
    }
}
