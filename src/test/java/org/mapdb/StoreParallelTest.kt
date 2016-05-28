package org.mapdb

import org.junit.Test
import org.junit.Assert.*
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread
import kotlin.concurrent.timer

/**
 * Tests if store is thread safe
 */

@RunWith(Parameterized::class)
class StoreParallelTest(val maker:()->Store){


    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = listOf(
                {StoreDirect.make()},
                {StoreWAL.make()},
                {StoreTrivial()},
                {StoreOnHeap()}
            ).map{arrayOf(it)}

            return if(TT.shortTest()) ret.take(1) else ret
        }
    }

    val threadCount = 10

    @Test(timeout = 10*60*1000)
    fun close(){
        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            TT.forkExecutor(executor, threadCount){
                store.close()
            }
        }
        executor.shutdown()
    }


    @Test(timeout = 10*60*1000)
    fun update(){
        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{store.put(it.toLong(), Serializer.LONG)}
            TT.forkExecutor(executor, threadCount){
                recids.forEach { store.update(it, -1, Serializer.LONG) }
            }
            recids.forEach {
                assertEquals(-1L, store.get(it, Serializer.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }



    @Test(timeout = 10*60*1000)
    fun cas(){
        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{store.put(100, Serializer.LONG)}
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    assertTrue(store.compareAndSwap(it, 100, 100, Serializer.LONG))
                }
            }
            recids.forEach {
                assertEquals(100L, store.get(it, Serializer.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }


    @Test(timeout = 10*60*1000)
    fun commit(){
        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{
                store.put(100L, Serializer.LONG)
            }
            store.commit()
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    store.update(it, it, Serializer.LONG)
                    store.commit()
                }
            }
            recids.forEach {
                assertEquals(it, store.get(it, Serializer.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }


    @Test(timeout = 10*60*1000)
    fun rollback(){
        if(maker() !is StoreTx)
            return
        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker() as StoreTx
            val recids = (0..100).map{store.put(100L, Serializer.LONG)}
            store.commit()
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    store.update(it, it, Serializer.LONG)
                    store.rollback()
                }
            }
            recids.forEach {
                assertEquals(100L, store.get(it, Serializer.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }



}
