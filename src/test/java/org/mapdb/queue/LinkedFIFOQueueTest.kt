package org.mapdb.queue

import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.TT
import org.mapdb.jsr166Tests.BlockingQueueTest
import org.mapdb.ser.Serializers
import org.mapdb.store.StoreOnHeapSer
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.locks.LockSupport


class LinkedFIFOQueue166Test: BlockingQueueTest() {

    override fun emptyCollection(): BlockingQueue<*> {
        return LinkedFIFOQueueTest.newOnHeap()
    }
}

class LinkedFIFOQueueTest{

    companion object {
        fun newOnHeap():LinkedFIFOQueue<Int>{
            val store = StoreOnHeapSer()
            val rootRecid = store.put(0L, Serializers.RECID)
            return LinkedFIFOQueue(store, rootRecid, Serializers.INTEGER)
        }
    }

    @Test fun failFastIter(){
        val q = newOnHeap()
        q.put(1)
        q.put(2)

        val iter = q.iterator()
        iter.next() shouldBe 2

        q.put(3)
        TT.assertFailsWith(ConcurrentModificationException::class){
            iter.next()
        }
        TT.assertFailsWith(ConcurrentModificationException::class){
            iter.next()
        }

        val iter2 = q.iterator()
        iter2.next() shouldBe 3
        iter2.next() shouldBe 2
        iter2.next() shouldBe 1
    }


    @Test fun concurrent_iter_forEach(){
        if(TT.shortTest())
            return

        val q = newOnHeap()
        q.put(1)
        q.put(2)

        TT.withBool{b->
            TT.async{
                while(b.get()){
                    q.put(3)
                    q.take()
                }
            }

            for(i in 1 until 1e5.toInt()){
                assert(q.isNotEmpty())

                val iter = q.iterator()
                LockSupport.parkNanos(100)
                iter.forEachRemaining{i->

                }
            }
        }
    }



    @Test
    fun concurrent_spliter_forEach(){
        if(TT.shortTest())
            return
        val q = newOnHeap()
        q.put(1)
        q.put(2)

        TT.withBool{b->
            TT.async{
                while(b.get()){
                    q.put(3)
                    q.take()
                }
            }

            for(i in 1 until 1e5.toInt()){
                assert(q.isNotEmpty())

                val iter = q.spliterator()
                LockSupport.parkNanos(100)
                iter.forEachRemaining{i->

                }
            }
        }
    }


    @Test fun leak() {
        if(TT.shortTest())
            return

        val q = newOnHeap()
        q.put(1)
        q.put(2)

        for (i in 1 until 1e8.toInt()) {
            q.put(3)
            q.take()


        }
    }
}

