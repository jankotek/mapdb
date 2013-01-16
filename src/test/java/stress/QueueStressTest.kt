package stress

import org.junit.*
import kotlin.test.*
import java.util.concurrent.atomic.*
import java.util.concurrent.*
import org.mapdb.*
import java.util.*

abstract class QueueStressTest(val q:Queue<Any?>){

    val threadNum:Long = 16;
    val max:Long = 1000;
    val maxNodeNum:Long = threadNum * (max * (max+1))/2;


    Test(timeout=1000000)
    fun stress(){
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
                        updated = counter.compareAndSet(old, old + (n as Long));
                    }
                }
            });
        }


        exec.shutdown();
        while(!exec.awaitTermination(1,TimeUnit.DAYS)){}

        assertEquals(maxNodeNum, counter.get());
    }
}


class QueueStress_JUC_ConcurrentLinkedQueue:QueueStressTest(ConcurrentLinkedQueue<Any?>()){}

class QueueStress_LinkedQueueFifo_noLocks:QueueStressTest(
    Queue2.Fifo<Any?>(testEngine(), Serializer.BASIC_SERIALIZER)
){}

class QueueStress_LinkedQueueLifo_noLocks:QueueStressTest(
        Queue2.Lifo<Any?>(testEngine(), Serializer.BASIC_SERIALIZER, true)
){}

class QueueStress_LinkedQueueLifo_withLocks:QueueStressTest(
        Queue2.Lifo<Any?>(testEngine(), Serializer.BASIC_SERIALIZER, false)
){}



class QueueStress_LinkedQueueLifo_storageDirect:QueueStressTest(
        Queue2.Lifo<Any?>(testEngine(), Serializer.BASIC_SERIALIZER, false)
){}



class LinkedQueueLifo_StorageDirect:QueueStressTest(
    Queue2.Lifo<Any?>(
            StorageDirect(Volume.memoryFactory(false)),
            Serializer.BASIC_SERIALIZER,false)
){}
