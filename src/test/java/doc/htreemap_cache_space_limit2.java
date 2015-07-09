package doc;

import org.mapdb10.DB;
import org.mapdb10.DBMaker;
import org.mapdb10.HTreeMap;


public class htreemap_cache_space_limit2 {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap cache = db.createHashMap("cache")
                .expireStoreSize(128)
                .makeOrGet();
        //z
    }
}
