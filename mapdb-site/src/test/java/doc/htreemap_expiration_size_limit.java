package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


public class htreemap_expiration_size_limit {

    @Test
    public void run() {

        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap cache = db
                .hashMap("cache")
                .expireMaxSize(128)
                .expireAfterGet()
                .create();
        //z
    }
}
