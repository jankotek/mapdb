package org.mapdb.queue

import io.kotlintest.shouldBe
import org.junit.Test
import org.mapdb.TT
import org.mapdb.jsr166.BlockingQueueTest
import org.mapdb.serializer.Serializers
import org.mapdb.store.StoreOnHeapSer
import java.util.*
import java.util.concurrent.BlockingQueue



class LinkedQueue166Test: BlockingQueueTest() {

    override fun emptyCollection(): BlockingQueue<*> {
        return LinkedQueueTest.newOnHeap()
    }
}

class LinkedQueueTest{

    companion object {
        fun newOnHeap():LinkedQueue<Int>{
            val store = StoreOnHeapSer()
            val rootRecid = store.put(0L, Serializers.RECID)
            return LinkedQueue(store, rootRecid, Serializers.INTEGER)
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


}

