package org.mapdb.store

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.*
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReadWriteLock

class StoreOnHeapTest{


    val StoreOnHeap.lock:  ReadWriteLock?
        get() = TT.reflectionGetField(this, "lock")

    val StoreOnHeap.freeRecids: LongArrayStack
        get() = TT.reflectionGetField(this, "freeRecids")


    val StoreOnHeap.records: LongObjectHashMap<ByteArray>
        get() = TT.reflectionGetField(this, "records")


    val StoreOnHeap.maxRecid: AtomicLong
        get() = TT.reflectionGetField(this, "maxRecid")


    @Test fun compact(){
        val store = StoreOnHeap()
        val r1 = store.put("aa", Serializer.STRING)
        assertEquals(r1, 1)
        val r2 = store.put("aa", Serializer.STRING)
        assertEquals(r2, 2)
        assertEquals(store.maxRecid.get(), 2)
        assertTrue(store.freeRecids.isEmpty)

        store.delete(r2, Serializer.STRING)
        assertEquals(store.freeRecids.size(),1)
        assertEquals(store.maxRecid.get(), 2)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid.get(), 1)

        store.delete(r1, Serializer.STRING)
        assertEquals(store.freeRecids.size(),1)
        assertEquals(store.maxRecid.get(), 1)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid.get(), 0)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid.get(), 0)

    }

}