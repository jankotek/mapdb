package doc;

import org.junit.Test;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.Executors;


public class htreemap_expiration_sharded2 {

    @Test
    public void run() {

        //a
        HTreeMap cache = DBMaker
                .memoryShardedHashMap(16)
                .expireAfterUpdate()
                .expireStoreSize(128*1024*1024)

                //entry expiration in 3 background threads
                .expireExecutor(
                        Executors.newScheduledThreadPool(3))

                //trigger Store compaction if 40% of space is free
                .expireCompactThreshold(0.4)

                .create();
        //z
    }
}
