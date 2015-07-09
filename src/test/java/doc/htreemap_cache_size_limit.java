package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.HTreeMap;


public class htreemap_cache_size_limit {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap cache = db.hashMapCreate("cache")
                .expireMaxSize(128)
                .makeOrGet();
        //z
    }
}
