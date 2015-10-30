package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;


public class cache_weak_soft {

    public static void main(String[] args) {
        //a

        DB db = DBMaker
                .memoryDB()

                //enable Weak Reference cache
                .cacheWeakRefEnable()
                //or enable Soft Reference cache
                .cacheSoftRefEnable()

                 //optionally enable executor, so cache is cleared in background thread
                .cacheExecutorEnable()

                .make();

        //z
    }
}
