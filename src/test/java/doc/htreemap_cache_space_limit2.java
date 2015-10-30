package doc;

import org.mapdb20.DB;
import org.mapdb20.DBMaker;
import org.mapdb20.HTreeMap;


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
