package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.TimeUnit;


public class htreemap_cache_ttl_limit {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        // remove entries 1 after their last modification,
        // or 10 minutes after last get()
        HTreeMap cache = db.hashMapCreate("cache")
                .expireAfterWrite(1, TimeUnit.HOURS)
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .makeOrGet();
        //z
    }
}
