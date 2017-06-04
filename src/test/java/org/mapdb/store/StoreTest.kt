package org.mapdb.store

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.*
import org.mapdb.TT.assertFailsWith
import java.util.*
import java.util.concurrent.atomic.AtomicLong

/**
 * Tests contract on `Store` interface
 */
abstract class StoreTest {

    abstract fun openStore(): Store;

    @Test fun put_get() {
        val e = openStore()
        val l = 11231203099090L
        val recid = e.put(l, Serializer.LONG)
        assertEquals(l, e.get(recid, Serializer.LONG))
        e.verify()
        e.close()
    }

    @Test fun put_get_large() {
        val e = openStore()
        val b = TT.randomByteArray(1000000)
        Random().nextBytes(b)
        val recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))
        e.verify()
        e.close()
    }

    @Test fun testSetGet() {
        val e = openStore()
        val recid = e.put(10000.toLong(), Serializer.LONG)
        val s2 = e.get(recid, Serializer.LONG)
        assertEquals(s2, java.lang.Long.valueOf(10000))
        e.verify()
        e.close()
    }


    @Test fun reserved_recids(){
        val e = openStore()
        for(expectedRecid in 1 .. CC.RECID_MAX_RESERVED){
            val allocRecid = e.put(1, Serializer.INTEGER)
            assertEquals(expectedRecid, allocRecid)
        }
        e.verify()
        e.close()
    }

    @Test
    fun large_record() {
        val e = openStore()
        val b = TT.randomByteArray(100000)
        val recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        val b2 = e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, b2))
        e.verify()
        e.close()
    }

    @Test fun large_record_delete() {
        val e = openStore()
        val b = TT.randomByteArray(100000)
        val recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        e.verify()
        e.delete(recid, Serializer.BYTE_ARRAY_NOSIZE)
        e.verify()
        e.close()
    }


    @Test fun large_record_delete2(){
        val s = openStore()

        val b = TT.randomByteArray(200000)
        val recid1 = s.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        s.verify()
        val b2 = TT.randomByteArray(220000)
        val recid2 = s.put(b2, Serializer.BYTE_ARRAY_NOSIZE)
        s.verify()

        assertTrue(Arrays.equals(b, s.get(recid1, Serializer.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializer.BYTE_ARRAY_NOSIZE)))

        s.delete(recid1, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializer.BYTE_ARRAY_NOSIZE)))
        s.verify()

        s.delete(recid2, Serializer.BYTE_ARRAY_NOSIZE)
        s.verify()

        s.verify()
        s.close()
    }

    @Test fun large_record_update(){
        val s = openStore()

        var b = TT.randomByteArray(200000)
        val recid1 = s.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        s.verify()
        val b2 = TT.randomByteArray(220000)
        val recid2 = s.put(b2, Serializer.BYTE_ARRAY_NOSIZE)
        s.verify()

        assertTrue(Arrays.equals(b, s.get(recid1, Serializer.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializer.BYTE_ARRAY_NOSIZE)))

        b = TT.randomByteArray(210000)
        s.update(recid1, b, Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Arrays.equals(b, s.get(recid1, Serializer.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializer.BYTE_ARRAY_NOSIZE)))
        s.verify()

        b = TT.randomByteArray(28001)
        s.update(recid1, b, Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Arrays.equals(b, s.get(recid1, Serializer.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializer.BYTE_ARRAY_NOSIZE)))
        s.verify()

        s.close()
    }


    @Test
    fun get_non_existent() {
        val e = openStore()

        TT.assertFailsWith(DBException.GetVoid::class.java) {
            e.get(1, TT.Serializer_ILLEGAL_ACCESS)
        }

        e.verify()
        e.close()
    }

    @Test fun preallocate_cas() {
        val e = openStore()
        val recid = e.preallocate()
        e.verify()
        assertFalse(e.compareAndSwap(recid, 1L, 2L, Serializer.LONG))
        assertTrue(e.compareAndSwap(recid, null, 2L, Serializer.LONG))
        assertEquals(2L.toLong(), e.get(recid, Serializer.LONG))
        e.verify()
        e.close()
    }

    @Test fun preallocate_get_update_delete_update_get() {
        val e = openStore()
        val recid = e.preallocate()
        e.verify()
        assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        e.update(recid, 1L, Serializer.LONG)
        assertEquals(1L.toLong(), e.get(recid, Serializer.LONG))
        e.delete(recid, Serializer.LONG)
        assertFailsWith(DBException.GetVoid::class.java) {
            assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        }
        e.verify()
        assertFailsWith(DBException.GetVoid::class.java) {
            e.update(recid, 1L, Serializer.LONG)
        }
        e.verify()
        e.close()
    }

    @Test fun cas_delete() {
        val e = openStore()
        val recid = e.put(1L, Serializer.LONG)
        e.verify()
        assertTrue(e.compareAndSwap(recid, 1L, null, Serializer.LONG))
        assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        assertTrue(e.compareAndSwap(recid, null, 1L, Serializer.LONG))
        assertEquals(1L, e.get(recid, Serializer.LONG))
        e.verify()
        e.close()
    }


    @Test fun cas_prealloc() {
        val e = openStore()
        val recid = e.preallocate()
        assertTrue(e.compareAndSwap(recid, null, 1L, Serializer.LONG))
        e.verify()
        assertEquals(1L, e.get(recid, Serializer.LONG))
        assertTrue(e.compareAndSwap(recid, 1L, null, Serializer.LONG))
        e.verify()
        assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        e.verify()
        e.close()
    }

    @Test fun cas_prealloc_delete() {
        val e = openStore()
        val recid = e.preallocate()
        e.delete(recid, Serializer.LONG)
        assertFailsWith(DBException.GetVoid::class.java) {
            assertTrue(e.compareAndSwap(recid, null, 1L, Serializer.LONG))
        }
        e.verify()
        e.close()
    }

    @Test fun putGetUpdateDelete() {
        val e = openStore()
        var s = "aaaad9009"
        val recid = e.put(s, Serializer.STRING)

        assertEquals(s, e.get(recid, Serializer.STRING))

        s = "da8898fe89w98fw98f9"
        e.update(recid, s, Serializer.STRING)
        assertEquals(s, e.get(recid, Serializer.STRING))
        e.verify()
        e.delete(recid, Serializer.STRING)
        assertFailsWith(DBException.GetVoid::class.java) {
            e.get(recid, Serializer.STRING)
        }
        e.verify()
        e.close()
    }


    @Test fun nosize_array() {
        val e = openStore()
        var b = ByteArray(0)
        val recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))

        b = byteArrayOf(1, 2, 3)
        e.update(recid, b, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))
        e.verify()
        b = byteArrayOf()
        e.update(recid, b, Serializer.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)))
        e.verify()
        e.delete(recid, Serializer.BYTE_ARRAY_NOSIZE)
        assertFailsWith(DBException.GetVoid::class.java) {
            e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
        }
        e.verify()
        e.close()
    }

    @Test fun get_deleted() {
        val e = openStore()
        val recid = e.put(1L, Serializer.LONG)
        e.verify()
        e.delete(recid, Serializer.LONG)
        assertFailsWith(DBException.GetVoid::class.java) {
            e.get(recid, Serializer.LONG)
        }
        e.verify()
        e.close()
    }

    @Test fun update_deleted() {
        val e = openStore()
        val recid = e.put(1L, Serializer.LONG)
        e.delete(recid, Serializer.LONG)
        assertFailsWith(DBException.GetVoid::class.java) {
            e.update(recid, 2L, Serializer.LONG)
        }
        e.verify()
        e.close()
    }

    @Test fun double_delete() {
        val e = openStore()
        val recid = e.put(1L, Serializer.LONG)
        e.delete(recid, Serializer.LONG)
        assertFailsWith(DBException.GetVoid::class.java) {
            e.delete(recid, Serializer.LONG)
        }
        e.verify()
        e.close()
    }


    @Test fun empty_update_commit() {
        if (TT.shortTest())
            return

        var e = openStore()
        val recid = e.put("", Serializer.STRING)
        assertEquals("", e.get(recid, Serializer.STRING))

        for (i in 0..9999) {
            val s = TT.randomString(80000)
            e.update(recid, s, Serializer.STRING)
            assertEquals(s, e.get(recid, Serializer.STRING))
            e.commit()
            assertEquals(s, e.get(recid, Serializer.STRING))
        }
        e.verify()
        e.close()
    }

    @Test fun delete_reuse() {
        for(size in 1 .. 20){
            val e = openStore()
            val recid = e.put(TT.randomString(size), Serializer.STRING)
            e.delete(recid, Serializer.STRING)
            assertFailsWith(DBException.GetVoid::class.java) {
                e.get(recid, TT.Serializer_ILLEGAL_ACCESS)
            }

            val recid2 = e.put(TT.randomString(size), Serializer.STRING)
            assertEquals(recid, recid2)
            e.verify()
            e.close()
        }
    }



    @Test fun empty_rollback(){
        val e = openStore()
        if(e is StoreTx)
            e.rollback()
        e.verify()
        e.close()
    }

    @Test fun empty_commit(){
        val e = openStore()
        e.commit()
        e.verify()
        e.close()
    }

    @Test fun randomUpdates() {
        if(TT.shortTest())
            return;
        val s = openStore()
        val random = Random(1);
        val endTime = TT.nowPlusMinutes(10.0)
        val ref = LongObjectHashMap<ByteArray>()
        //TODO params could cause OOEM if too big. Make another case of tests with extremely large memory, or disk space
        val maxRecSize = 1000
        val maxSize = 66000 * 3

        //fill up
        for (i in 0 until maxSize){
            val size = random.nextInt(maxRecSize)
            val b = TT.randomByteArray(size, random.nextInt())
            val recid = s.put(b, Serializer.BYTE_ARRAY_NOSIZE)
            ref.put(recid, b)
        }
        s.verify()

        while(endTime>System.currentTimeMillis()){
            ref.forEachKeyValue { recid, record ->
                val old = s.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
                assertTrue(Arrays.equals(record, old))

                val size = random.nextInt(maxRecSize)
                val b = TT.randomByteArray(size, random.nextInt())
                ref.put(recid,b.clone())
                s.update(recid, b, Serializer.BYTE_ARRAY_NOSIZE)
                assertTrue(Arrays.equals(b, s.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
            }
            s.verify()
            if(s is StoreWAL) {
                s.commit()
                s.verify()
            }
        }
    }

    @Test fun concurrent_CAS(){
        if(TT.shortTest())
            return;
        val s = openStore();
        if(s.isThreadSafe.not())
            return;

        val ntime = TT.nowPlusMinutes(1.0)
        var counter = AtomicLong(0);
        val recid = s.put(0L, Serializer.LONG)
        TT.fork(10){
            val random = Random();
            while(ntime>System.currentTimeMillis()){
                val plus = random.nextInt(1000).toLong()
                val v:Long = s.get(recid, Serializer.LONG)!!
                if(s.compareAndSwap(recid, v, v+plus, Serializer.LONG)){
                    counter.addAndGet(plus);
                }
            }
        }

        assertTrue(counter.get()>0)
        assertEquals(counter.get(), s.get(recid, Serializer.LONG))
    }


    @Test fun varRecordSizeCompact(){
        if(TT.shortTest())
            return
        val maxStoreSize = 10*1024*1024
        var size = 1

        while(size<maxStoreSize) {
            val store = openStore()
            val maxCount = 1 + maxStoreSize / size;
            //insert recids
            val recids = LongArrayList()
            for (i in 0..maxCount) {
                val r = TT.randomByteArray(size, seed = i)
                val recid = store.put(r, Serializer.BYTE_ARRAY_NOSIZE)
                recids.add(recid)
            }

            fun verify() {
               //verify recids
                recids.forEachWithIndex { recid, i ->
                    val r = store.get(recid, Serializer.BYTE_ARRAY_NOSIZE)
                    assertTrue(Arrays.equals(r, TT.randomByteArray(size, seed=i)))
                }
            }
            verify()
            store.compact()
            verify()

            size += 1 + size/113
        }
    }

}

class StoreHeapTest : StoreTest() {
    override fun openStore() = StoreOnHeap();
}

