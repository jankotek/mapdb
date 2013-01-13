package stress

import org.junit.*
import kotlin.test.*
import java.util.concurrent.atomic.*
import java.util.concurrent.*
import org.mapdb.*
import java.util.*

class QueueStressTest{

    val threadNum:Long = 16;
    val max:Long = 1000;
    val maxNodeNum:Long = threadNum * (max * (max+1))/2;


    Test fun jucConcurrentLinkedQueue() = stress(ConcurrentLinkedQueue<Long?>());


    Test(timeout=1000000)
    fun LinkedQueueLifo_noLocks(){
        val engine = DBMaker.newMemoryDB().journalDisable().makeEngine();
        val queue = LinkedQueueLifo<Long?>(engine, Serializer.LONG_SERIALIZER,false);
        stress(queue)
    }

    Test(timeout=1000000)
    fun LinkedQueueLifo_withLocks(){
        val engine = DBMaker.newMemoryDB().journalDisable().makeEngine();
        val queue = LinkedQueueLifo<Long?>(engine, Serializer.LONG_SERIALIZER,true);
        stress(queue)
    }


    Test(timeout=1000000)
    fun LinkedQueueLifo_StorageDirect(){
        val engine = StorageDirect(Volume.memoryFactory(false));
        val queue = LinkedQueueLifo<Long?>(engine, Serializer.LONG_SERIALIZER,false);
        stress(queue)
    }


    fun stress(val q:Queue<Long?>){
        val counter = AtomicLong(0);

        //start producer threads
        val exec = Executors.newCachedThreadPool()
        for(i in 1..threadNum){
            exec.execute(runnable{
                for(j in 1..max){
                    q.add(j);
                }
            });
        }

        val nodeNum = AtomicLong(maxNodeNum)

        //start consumer threads
        for(i in 1..threadNum){
            exec.execute(runnable{
                for(j in 1..max){
                    var n = q.poll();
                    while(n==null)
                        n = q.poll();
                    //increment counter
                    var updated = false;
                    while(!updated){
                        val old = counter.get();
                        updated = counter.compareAndSet(old, old + n!!);
                    }
                }
            });
        }


        exec.shutdown();
        while(!exec.awaitTermination(1,TimeUnit.DAYS)){}

        assertEquals(maxNodeNum, counter.get());
    }
}
