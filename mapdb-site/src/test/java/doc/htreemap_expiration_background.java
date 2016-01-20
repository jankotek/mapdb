package doc;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class htreemap_expiration_background {

    @Test public void run() {

        //a
        DB db = DBMaker.memoryDB().make();

        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(2);

        HTreeMap cache = db
                .hashMap("cache")
                .expireMaxSize(1000)
                .expireAfterGet()
                .expireExecutor(executor)
                .expireExecutorPeriod(10000)
                .create();

        //once we are done, background threads needs to be stopped
        db.close();
        //z
    }
}
