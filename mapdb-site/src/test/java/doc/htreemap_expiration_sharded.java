package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


public class htreemap_expiration_sharded {

    @Test
    public void run() {

        //a
        HTreeMap cache = DBMaker
                .memoryShardedHashMap(16)
                .expireAfterUpdate()
                .expireStoreSize(128*1024*1024)
                .create();
        //z
    }
}
