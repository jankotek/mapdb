package examples;

import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Store;

import java.util.Random;

/**
 * This example shows how-to create off-heap cache,
 * where entries expire when maximal store size is reached.
 *
 * It also shows howto get basic statistics about store size.
 */
public class CacheOffHeap {

    public static void main(String[] args) {

        final double cacheSizeInGB = 1.0;

        // Create cache backed by off-heap store
        // In this case store will use ByteBuffers backed by byte[].
        HTreeMap cache = DBMaker.newCache(cacheSizeInGB);

        // Other alternative is to use Direct ByteBuffers.
        // In this case the memory is not released if cache is not correctly closed.

        //  cache = DBMaker.newCacheDirect(cacheSizeInGB);

        //generates random key and values
        Random r = new Random();
        //used to print store statistics
        Store store = Store.forEngine(cache.getEngine());


        // insert some stuff in cycle
        for(long counter=1; counter<1e8; counter++){
            long key = r.nextLong();
            byte[] value = new byte[1000];
            r.nextBytes(value);

            cache.put(key,value);

            if(counter%1e5==0){
                System.out.printf("Map size: %,d, counter %,d, store size: %,d, store free size: %,d\n",
                        cache.sizeLong(), counter, store.getCurrSize(),  store.getFreeSize());
            }

        }

        // and release memory. Only necessary with `DBMaker.newCacheDirect()`
        cache.close();

    }
}
