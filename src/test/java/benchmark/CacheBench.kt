package benchmark

import com.carrotsearch.junitbenchmarks.AbstractBenchmark
import org.mapdb.*
import org.junit.Test
import org.junit.Assume

class CacheBench:AbstractBenchmark(){

    val cacheSize = 1024;
    val useDirectMemory = true;

    val size = 1E6.toInt();

    fun storage() =
            AsyncWriteEngine(
                StorageDirect(Volume.memoryFactory(useDirectMemory)),
                true, false
            )

    fun benchCache(cache:Engine){
        Assume.assumeTrue(CC.FULL_TEST)
        val map = DB(cache).getTreeMap<Int, String>("test");

        for(i in 1..size){
            map.put(i, i.toString())
        }


        for(j in 1..10){

            for(i in 1..size){
                if(map.get(i)=="qwdqwd")
                    throw Error();
            }
        }
        cache.close();
    }

    Test fun cacheHashTable(){
        val cache = CacheHashTable(storage(),cacheSize);
        benchCache(cache);
    }

    Test fun cacheHashTableSynchronized(){
        val cache = CacheHashTableSynchronized(storage(),cacheSize);
        benchCache(cache);
    }

    Test fun cacheLRU(){
        val cache = CacheLRU(storage(),cacheSize);
        benchCache(cache);
    }

    Test fun cacheHardRef(){
        val cache = CacheHardRef(storage(),cacheSize);
        benchCache(cache);
    }

    Test fun cacheSoftRef(){
        val cache = CacheWeakSoftRef(storage(),false);
        benchCache(cache);
    }

    Test fun cacheWeakRef(){
        val cache = CacheWeakSoftRef(storage(),true);
        benchCache(cache);
    }


}
