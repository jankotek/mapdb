package stress

import org.junit.*
import kotlin.test.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors
import org.mapdb.*
import java.util.concurrent.TimeUnit

class CasStressTest{

    val threadNum = 32;
    val count = 100000;


    Test fun direct() = stress(StorageDirect(Volume.memoryFactory(false)))

    Test fun journaled() = stress(StorageJournaled(Volume.memoryFactory(false)))

    Test fun default() = stress(DBMaker.newMemoryDB().makeEngine())

    Test fun defaultNoJournal() = stress(DBMaker.newMemoryDB().journalDisable().makeEngine())

    Test fun lruCache() = stress(DBMaker.newMemoryDB().journalDisable()
            .cacheLRUEnable()
            .makeEngine())

    Test fun hardCache() = stress(DBMaker.newMemoryDB().journalDisable()
            .cacheHardRefEnable()
            .makeEngine())

    Test fun softCache() = stress(DBMaker.newMemoryDB().journalDisable()
            .cacheSoftRefEnable()
            .makeEngine())

    Test fun weakCache() = stress(DBMaker.newMemoryDB().journalDisable()
            .cacheWeakRefEnable()
            .makeEngine())


    fun stress(engine:Engine){

        val recid = engine.put(0,Serializer.INTEGER_SERIALIZER);

        val exec = Executors.newCachedThreadPool()
        for(i in 1..threadNum){
            exec.execute(runnable{
                for(j in 1..count){
                    var incremented = false;
                    while(!incremented){
                        val oldVal = engine.get(recid, Serializer.INTEGER_SERIALIZER)!!;
                        incremented = engine.compareAndSwap(recid, oldVal, oldVal+1, Serializer.INTEGER_SERIALIZER)
                    }
                }
            });
        }

        exec.shutdown();
        while(!exec.awaitTermination(1,TimeUnit.DAYS)){}

        val finalCount = engine.get(recid, Serializer.INTEGER_SERIALIZER);
        assertEquals(threadNum * count, finalCount)
        engine.close();


    }

}
