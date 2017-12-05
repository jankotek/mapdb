package org.mapdb.store

import org.junit.Assert.*
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.*
import org.mapdb.serializer.Serializers

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
        if(TT.shortTest())
            return

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
        if(TT.shortTest())
            return

        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{store.put(it.toLong(), Serializers.LONG)}
            TT.forkExecutor(executor, threadCount){
                recids.forEach { store.update(it, -1, Serializers.LONG) }
            }
            recids.forEach {
                assertEquals(-1L, store.get(it, Serializers.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }



    @Test(timeout = 10*60*1000)
    fun cas(){
        if(TT.shortTest())
            return

        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{store.put(100, Serializers.LONG)}
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    assertTrue(store.compareAndSwap(it, 100, 100, Serializers.LONG))
                }
            }
            recids.forEach {
                assertEquals(100L, store.get(it, Serializers.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }


    @Test(timeout = 10*60*1000)
    fun commit(){
        if(TT.shortTest())
            return

        val end = TT.nowPlusMinutes(2.0)
        val executor = TT.executor(threadCount)
        while(System.currentTimeMillis()<end){
            val store = maker()
            val recids = (0..100).map{
                store.put(100L, Serializers.LONG)
            }
            store.commit()
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    store.update(it, it, Serializers.LONG)
                    store.commit()
                }
            }
            recids.forEach {
                assertEquals(it, store.get(it, Serializers.LONG))
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
            val recids = (0..100).map{store.put(100L, Serializers.LONG)}
            store.commit()
            TT.forkExecutor(executor, threadCount){
                recids.forEach {
                    store.update(it, it, Serializers.LONG)
                    store.rollback()
                }
            }
            recids.forEach {
                assertEquals(100L, store.get(it, Serializers.LONG))
            }

            store.close()
        }
        executor.shutdown()
    }



}
