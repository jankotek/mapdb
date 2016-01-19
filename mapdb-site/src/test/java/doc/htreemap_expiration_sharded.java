package doc;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;


public class htreemap_expiration_sharded {

    public static void main(String[] args) {
        //a
        HTreeMap cache = DBMaker
                .memoryShardedHashMap(16)
                .expireAfterUpdate()
                .expireStoreSize(128*1024*1024)
                .create();
        //z
    }
}
