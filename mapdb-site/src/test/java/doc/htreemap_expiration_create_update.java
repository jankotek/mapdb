package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


public class htreemap_expiration_create_update {

    @Test
    public void run() {

        DB db = DBMaker.memoryDB().make();
        //a
        HTreeMap cache = db
                .hashMap("cache")
                .expireAfterUpdate(1000)
                .create();
        //z
    }
}
