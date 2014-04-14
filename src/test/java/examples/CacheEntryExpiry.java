package examples;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Store;

import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * HTreeMap (HashMap) can be used as cache, where items are removed after timeout or when maximal size is reached.
 *
 *
 */
public class CacheEntryExpiry {



    public static void main(String[] args) {
        //init off-heap store with 2GB size limit
        DB db = DBMaker
                .newMemoryDirectDB()    //use off-heap memory, on-heap is `.newMemoryDB()`
                .sizeLimit(2)           //limit store size to 2GB
                .transactionDisable()   //better performance
                .make();

        //create map, entries are expired if not accessed (get,iterate) for 10 seconds or 30 seconds after 'put'
        //There is also maximal size limit to prevent OutOfMemoryException
        HTreeMap map = db
                .createHashMap("cache")
                .expireMaxSize(1000000)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .expireAfterAccess(10, TimeUnit.SECONDS)
                .make();

        //load stuff
        for(int i = 0;i<100000;i++){
            map.put(i, randomString(1000));
        }

        //one can monitor two space usage numbers:

        //free space in store
        long freeSize = Store.forDB(db).getFreeSize();

        //current size of store (how much memory it has allocated
        long currentSize = Store.forDB(db).getCurrSize();


    }


    public static String randomString(int size) {
        String chars = "0123456789abcdefghijklmnopqrstuvwxyz !@#$%^&*()_+=-{}[]:\",./<>?|\\";
        StringBuilder b = new StringBuilder(size);
        Random r = new Random();
        for(int i=0;i<size;i++){
            b.append(chars.charAt(r.nextInt(chars.length())));
        }
        return b.toString();
    }


}
