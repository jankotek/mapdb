package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


public class htreemap_cache_space_limit2 {

    public static void main(String[] args) {
        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap cache = db.hashMapCreate("cache")
                .expireStoreSize(128)
                .makeOrGet();
        //z
    }
}
