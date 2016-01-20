package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.TimeUnit;


public class htreemap_expiration_ttl_limit {

    @Test
    public void run() {

        DB db = DBMaker.memoryDB().make();
        //a
        // remove entries 10 minutes  after their last modification,
        // or 1 minute after last get()
        HTreeMap cache = db
                .hashMap("cache")
                .expireAfterUpdate(10, TimeUnit.HOURS)
                .expireAfterCreate(10, TimeUnit.HOURS)
                .expireAfterGet(1, TimeUnit.MINUTES)
                .create();
        //z
    }
}
