package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Utils;

import java.util.concurrent.TimeUnit;


/**
 * HTreeMap (HashMap) can be used as cache, where items are removed after timeout or when maximal size is reached.
 */
public class Cache {

    public static void main(String[] args) {
        //init off-heap store with 2GB size limit
        DB db = DBMaker
                .newDirectMemoryDB()
                .sizeLimit(2) //limit size to 2GB
                .transactionDisable()
                .make();

        //create map, entries are expired if not accessed (get,iterate) in 10 seconds or 30 seconds after 'put'
        //there is also maximal size limit
        HTreeMap map = db
                .createHashMap("cache")
                .expireMaxSize(1000)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .make();

        //load stuff
        for(int i = 0;i<100000;i++){
            map.put(i, Utils.randomString(1000));
        }
    }

}
