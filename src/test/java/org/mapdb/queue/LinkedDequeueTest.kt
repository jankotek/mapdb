package org.mapdb.queue

import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.TT
import org.mapdb.jsr166.BlockingQueueTest
import org.mapdb.serializer.Serializers
import org.mapdb.store.StoreOnHeapSer
import java.util.*
import java.util.concurrent.BlockingDeque
import java.util.concurrent.locks.LockSupport
import kotlin.NoSuchElementException


class LinkedDeueue166Test: BlockingQueueTest() {

    override fun emptyCollection(): BlockingDeque<*> {
        return LinkedDequeTest.newOnHeap()
    }
}

class LinkedDequeTest{

    companion object {
        fun newOnHeap():LinkedDeque<Int>{
            val store = StoreOnHeapSer()
            val headRecid = store.put(0L, Serializers.RECID)
            val tailRecid = store.put(0L, Serializers.RECID)
            val q =  LinkedDeque(store, headRecid, tailRecid, Serializers.INTEGER)
            TT.installValidateReadWriteLock(q, "lock")
            return q
        }
    }

    @Test fun revertIter(){
        val q = newOnHeap()
        for(i in 1 .. 100)
            q.add(i)

        val iter = q.descendingIterator()
        for(i in 1..100){
            iter.hasNext() shouldBe true
            iter.next() shouldBe i
        }
        iter.hasNext() shouldBe false
        TT.assertFailsWith(NoSuchElementException::class){
            iter.next()
        }

    }


    //TODO some test methods are duplicated
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

