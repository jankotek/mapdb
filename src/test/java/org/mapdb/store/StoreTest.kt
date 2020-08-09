package org.mapdb.store

import io.kotlintest.shouldBe
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.DBException
import org.mapdb.TT
import org.mapdb.io.DataIO
import org.mapdb.io.DataInput2
import org.mapdb.io.DataOutput2
import org.mapdb.ser.Serializer
import org.mapdb.ser.Serializers
import org.mapdb.ser.Serializers.LONG
import org.mapdb.store.li.LiStore
import java.util.*
import java.util.concurrent.atomic.AtomicLong



class HeapBufStoreTest : StoreTest() {
    override fun openStore() = HeapBufStore()
}
class HeapBufStoreRWLockTest : StoreTest() {
    override fun openStore() = HeapBufStoreRWLock()
}



class ConcMapStoreTest : StoreTest() {
    override fun openStore() = ConcMapStore()

    override fun recid_getAll_sorted(){
        //TODO support multiform store
    }

}

class LiStoreTest : StoreTest() {
    override fun openStore() = LiStore()
}



/**
 * Tests contract on `Store` interface
 */
abstract class StoreTest {

    abstract fun openStore(): Store

    @Test fun put_get() {
        val e = openStore()
        val l = 11231203099090L
        val recid = e.put(l, LONG)
        assertEquals(l, e.get(recid, LONG))
        e.verify()
        e.close()
    }

    @Test fun put_get_large() {
        val e = openStore()
        if(e.maxRecordSize()<1e6)
            return

        val b = TT.randomByteArray(1000000)
        Random().nextBytes(b)
        val recid = e.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))
        e.verify()
        e.close()
    }

    @Test fun testSetGet() {
        val e = openStore()
        val recid = e.put(10000.toLong(), LONG)
        val s2 = e.get(recid, LONG)
        assertEquals(s2, java.lang.Long.valueOf(10000))
        e.verify()
        e.close()
    }


    @Test fun reserved_recids(){
        val e = openStore()
        for(expectedRecid in 1 .. Recids.RECID_MAX_RESERVED){
            val allocRecid = e.preallocate()
            assertEquals(expectedRecid, allocRecid)
        }
        e.verify()
        e.close()
    }

    @Test
    fun large_record() {
        val e = openStore()
        if(e.maxRecordSize()<1e6)
            return

        val b = TT.randomByteArray(100000)
        val recid = e.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        val b2 = e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, b2))
        e.verify()
        e.close()
    }

    @Test fun large_record_delete() {
        val e = openStore()
        if(e.maxRecordSize()<1e6)
            return
        val b = TT.randomByteArray(100000)
        val recid = e.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        e.verify()
        e.delete(recid, Serializers.BYTE_ARRAY_NOSIZE)
        e.verify()
        e.close()
    }


    @Test fun large_record_delete2(){
        val s = openStore()
        if(s.maxRecordSize()<1e6)
            return

        val b = TT.randomByteArray(200000)
        val recid1 = s.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        s.verify()
        val b2 = TT.randomByteArray(220000)
        val recid2 = s.put(b2, Serializers.BYTE_ARRAY_NOSIZE)
        s.verify()

        assertTrue(Arrays.equals(b, s.get(recid1, Serializers.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializers.BYTE_ARRAY_NOSIZE)))

        s.delete(recid1, Serializers.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializers.BYTE_ARRAY_NOSIZE)))
        s.verify()

        s.delete(recid2, Serializers.BYTE_ARRAY_NOSIZE)
        s.verify()

        s.verify()
        s.close()
    }

    @Test fun large_record_update(){
        val s = openStore()
        if(s.maxRecordSize()<1e6)
            return

        var b = TT.randomByteArray(200000)
        val recid1 = s.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        s.verify()
        val b2 = TT.randomByteArray(220000)
        val recid2 = s.put(b2, Serializers.BYTE_ARRAY_NOSIZE)
        s.verify()

        assertTrue(Arrays.equals(b, s.get(recid1, Serializers.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializers.BYTE_ARRAY_NOSIZE)))

        b = TT.randomByteArray(210000)
        s.update(recid1, Serializers.BYTE_ARRAY_NOSIZE, b);
        assertTrue(Arrays.equals(b, s.get(recid1, Serializers.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializers.BYTE_ARRAY_NOSIZE)))
        s.verify()

        b = TT.randomByteArray(28001)
        s.update(recid1, Serializers.BYTE_ARRAY_NOSIZE, b);
        assertTrue(Arrays.equals(b, s.get(recid1, Serializers.BYTE_ARRAY_NOSIZE)))
        assertTrue(Arrays.equals(b2, s.get(recid2, Serializers.BYTE_ARRAY_NOSIZE)))
        s.verify()

        s.close()
    }


    @Test
    fun get_non_existent() {
        val e = openStore()

        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.get(1, TT.Serializer_ILLEGAL_ACCESS)
        }

        e.verify()
        e.close()
    }

    @Test fun preallocate_cas() {
        val e = openStore()
        val recid = e.preallocate()
        e.verify()
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.compareAndUpdate(recid, LONG, 1L, 2L)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.compareAndUpdate(recid, LONG, 2L, 2L)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.get(recid, LONG)
        }

        e.preallocatePut(recid, LONG, 2L)

        assertFalse(e.compareAndUpdate(recid, LONG, 1L, 3L))
        assertTrue(e.compareAndUpdate(recid, LONG, 2L, 2L))
        assertTrue(e.compareAndUpdate(recid, LONG, 2L, 3L))
        assertEquals(3L, e.get(recid, LONG))

        e.verify()
        e.close()
    }

    @Test fun preallocate_get_update_delete_update_get() {
        val e = openStore()
        val recid = e.preallocate()
        e.verify()
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.get(recid, TT.Serializer_ILLEGAL_ACCESS)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.update(recid, LONG, 1L)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            assertEquals(1L.toLong(), e.get(recid, LONG))
        }
        e.preallocatePut(recid, LONG, 1L)
        assertEquals(1L.toLong(), e.get(recid, LONG))

        e.delete(recid, LONG)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        }
        e.verify()
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.update(recid,  LONG, 1L)
        }
        e.verify()
        e.close()
    }

    @Test fun cas_delete() {
        val e = openStore()
        val recid = e.put(1L, LONG)
        e.verify()
        assertTrue(e.compareAndDelete(recid, LONG, 1L))
        
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            assertNull(e.get(recid, TT.Serializer_ILLEGAL_ACCESS))
        }
        e.verify()
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.update(recid,  LONG, 1L)
        }
        e.verify()
        e.close()
    }


    @Test fun cas_prealloc() {
        val e = openStore()
        val recid = e.preallocate()
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.compareAndUpdate(recid,  LONG, 2L, 1L)
        }
        e.preallocatePut(recid, LONG, 2L)
        assertTrue(e.compareAndUpdate(recid,  LONG, 2L, 1L))
        e.verify()
        assertEquals(1L, e.get(recid, LONG))
        assertTrue(e.compareAndUpdate(recid, LONG, 1L, 3L))
        e.verify()
        assertEquals(3L, e.get(recid, LONG))
        e.verify()
        e.close()
    }

    @Test fun cas_prealloc_delete() {
        val e = openStore()
        val recid = e.preallocate()
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.delete(recid, LONG)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            e.get(recid, LONG)
        }
        e.preallocatePut(recid, LONG, 1L)

        e.delete(recid, LONG)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            assertTrue(e.compareAndUpdate(recid, LONG, 3L, 1L))
        }
        e.verify()
        e.close()
    }

    @Test fun putGetUpdateDelete() {
        val e = openStore()
        var s = "aaaad9009"
        val recid = e.put(s, Serializers.STRING)

        assertEquals(s, e.get(recid, Serializers.STRING))

        s = "da8898fe89w98fw98f9"
        e.update(recid, Serializers.STRING, s)
        assertEquals(s, e.get(recid, Serializers.STRING))
        e.verify()
        e.delete(recid, Serializers.STRING)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.get(recid, Serializers.STRING)
        }
        e.verify()
        e.close()
    }


    @Test fun not_preallocated() {
        val e = openStore()
        TT.assertFailsWith(DBException.RecordNotPreallocated::class) {
            e.preallocatePut(222, LONG, 1L)
        }
        val recid = e.put(1L, LONG)
        TT.assertFailsWith(DBException.RecordNotPreallocated::class) {
            e.preallocatePut(recid, LONG, 1L)
        }
    }

     @Test fun nosize_array() {
        val e = openStore()
        var b = ByteArray(0)
        val recid = e.put(b, Serializers.BYTE_ARRAY_NOSIZE)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))

        b = byteArrayOf(1, 2, 3)
        e.update(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))
        e.verify()
        b = byteArrayOf()
        e.update(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))
        e.verify()
        e.delete(recid, Serializers.BYTE_ARRAY_NOSIZE)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)
        }
        e.verify()
        e.close()
    }


    @Test fun nosize_array_prealloc() {
        val e = openStore()
        var b = ByteArray(0)
        val recid = e.preallocate();
        e.preallocatePut(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))

        b = byteArrayOf(1, 2, 3)
        e.update(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))
        e.verify()
        b = byteArrayOf()
        e.update(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
        assertTrue(Arrays.equals(b, e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)))
        e.verify()
        e.delete(recid, Serializers.BYTE_ARRAY_NOSIZE)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.get(recid, Serializers.BYTE_ARRAY_NOSIZE)
        }
        e.verify()
        e.close()
    }


    @Test fun get_deleted() {
        val e = openStore()
        val recid = e.put(1L, LONG)
        e.verify()
        e.delete(recid, LONG)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.get(recid, LONG)
        }
        e.verify()
        e.close()
    }

    @Test fun update_deleted() {
        val e = openStore()
        val recid = e.put(1L, LONG)
        e.delete(recid, LONG)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.update(recid, LONG, 2L)
        }
        e.verify()
        e.close()
    }

    @Test fun double_delete() {
        val e = openStore()
        val recid = e.put(1L, LONG)
        e.delete(recid, LONG)
        TT.assertFailsWith(DBException.RecordNotFound::class) {
            e.delete(recid, LONG)
        }
        e.verify()
        e.close()
    }


    @Test fun empty_update_commit() {
        if (TT.shortTest())
            return

        var e = openStore()
        val recid = e.put("", Serializers.STRING)
        assertEquals("", e.get(recid, Serializers.STRING))

        for (i in 0..9999) {
            val s = TT.randomString(80000)
            e.update(recid, Serializers.STRING, s)
            assertEquals(s, e.get(recid, Serializers.STRING))
            e.commit()
            assertEquals(s, e.get(recid, Serializers.STRING))
        }
        e.verify()
        e.close()
    }

    @Test fun delete_reuse() {
        for(size in 1 .. 20){
            val e = openStore()
            val recid = e.put(TT.randomString(size), Serializers.STRING)
            e.delete(recid, Serializers.STRING)
            TT.assertFailsWith(DBException.RecordNotFound::class) {
                e.get(recid, TT.Serializer_ILLEGAL_ACCESS)
            }

            val recid2 = e.put(TT.randomString(size), Serializers.STRING)
            assertEquals(recid, recid2)
            e.verify()
            e.close()
        }
    }


//TODO test
//    @Test fun empty_rollback(){
//        val e = openStore()
//        if(e is StoreTx)
//            e.rollback()
//        e.verify()
//        e.close()
//    }

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
            val recid = s.put(b, Serializers.BYTE_ARRAY_NOSIZE)
            ref.put(recid, b)
        }
        s.verify()

        while(endTime>System.currentTimeMillis()){
            ref.forEachKeyValue { recid, record ->
                val old = s.get(recid, Serializers.BYTE_ARRAY_NOSIZE)
                assertTrue(Arrays.equals(record, old))

                val size = random.nextInt(maxRecSize)
                val b = TT.randomByteArray(size, random.nextInt())
                ref.put(recid,b.clone())
                s.update(recid, Serializers.BYTE_ARRAY_NOSIZE, b)
                assertTrue(Arrays.equals(b, s.get(recid, Serializers.BYTE_ARRAY_NOSIZE)));
            }
            s.verify()
            //TODO TX test
//            if(s is StoreWAL) {
//                s.commit()
//                s.verify()
//            }
        }
    }

    @Test fun concurrent_CAS(){
        if(TT.shortTest())
            return;
        val s = openStore();
        if(s.isThreadSafe().not())
            return;

        val ntime = TT.nowPlusMinutes(1.0)
        var counter = AtomicLong(0);
        val recid = s.put(0L, LONG)
        TT.fork(10){
            val random = Random();
            while(ntime>System.currentTimeMillis()){
                val plus = random.nextInt(1000).toLong()
                val v:Long = s.get(recid, LONG)!!
                if(s.compareAndUpdate(recid, LONG, v, v+plus)){
                    counter.addAndGet(plus);
                }
            }
        }

        assertTrue(counter.get()>0)
        assertEquals(counter.get(), s.get(recid, LONG))
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
                val recid = store.put(r, Serializers.BYTE_ARRAY_NOSIZE)
                recids.add(recid)
            }

            fun verify() {
                //verify recids
                recids.forEachWithIndex { recid, i ->
                    val r = store.get(recid, Serializers.BYTE_ARRAY_NOSIZE)
                    assertTrue(Arrays.equals(r, TT.randomByteArray(size, seed=i)))
                }
            }
            verify()
            store.compact()
            verify()

            size += 1 + size/113
        }
    }

//    @Test
    //TODO reentry test with preprocessor in paranoid mode
    fun reentry(){
        val store = openStore()

        val recid = store.put("aa", Serializers.STRING)
        val reentrySer1 = object: Serializer<String> {

            override fun serializedType() = String::class.java


            override fun serialize(out: DataOutput2, k: String) {
                out.writeUTF(k)
                // that should fail
                store.update(recid, Serializers.STRING, k)
            }

            override fun deserialize(input: DataInput2): String {
                return input.readUTF()
            }
        }

        val reentrySer2 = object: Serializer<String> {

            override fun serializedType() = String::class.java

            override fun serialize(out: DataOutput2, k: String) {
                out.writeUTF(k)
            }

            override fun deserialize(input: DataInput2): String {
                val s = input.readUTF()
                // that should fail
                store.update(recid, Serializers.STRING, s)
                return s
            }
        }

        TT.assertFailsWith(DBException.StoreReentry::class){
            store.put("aa", reentrySer1)
            store.commit()
        }

        val recid2 = store.put("aa", Serializers.STRING)

        TT.assertFailsWith(DBException.StoreReentry::class){
            store.get(recid2, reentrySer2)
        }

        TT.assertFailsWith(DBException.StoreReentry::class){
            store.update(recid2, reentrySer1, "bb")
            store.commit()
        }


        TT.assertFailsWith(DBException.StoreReentry::class){
            store.compareAndUpdate(recid2, reentrySer1, "aa", "bb")
            store.commit()
        }

        TT.assertFailsWith(DBException.StoreReentry::class){
            store.compareAndUpdate(recid2, reentrySer2, "aa", "bb")
            store.commit()
        }

        TT.assertFailsWith(DBException.StoreReentry::class){
            store.compareAndDelete(recid2, reentrySer2, "aa")
            store.commit()
        }

    }


    open @Test fun recid_getAll_sorted(){
        val store = openStore()

        val max = 1000

        val records = (0 until max).map{ n->
            val recid = store.put(n, Serializers.INTEGER)
            Pair(recid, n)
        }

        val records2 = ArrayList<Pair<Long, Int>>()
        store.getAll{recid, data:ByteArray? ->
            data!!.size shouldBe 4
            val n = DataIO.getInt(data!!, 0)
            records2+=Pair(recid, n)
        }
//TODO check preallocated records are excluded
//TODO check deleted records are excluded
        records2.size shouldBe max
        for(i in 0 until max){
            val (recid, n) = records2[i]
            n shouldBe i
            store.get(recid, Serializers.INTEGER) shouldBe i

            records[i].first shouldBe recid
            records[i].second shouldBe n
        }
    }
//
//    @Test fun export_to_archive(){
//        val store = openStore()
//
//        val max = 1000
//
//        val records = (0 until max).map{ n->
//            val recid = store.put(n*1000, Serializers.INTEGER)
//            Pair(recid, n*1000)
//        }
//
//        val out = ByteArrayOutputStream()
//        StoreArchive.importFromStore(store, out)
//
//        val archive = StoreArchive(ByteBuffer.wrap(out.toByteArray()))
//
//        for((recid,n) in records){
//            archive.get(recid, Serializers.INTEGER) shouldBe n
//        }
//    }

    @Test fun empty(){
        val store = openStore()
        store.isEmpty() shouldBe true
        val recid = store.put(1, Serializers.INTEGER)
        store.isEmpty() shouldBe false

        store.delete(recid, Serializers.INTEGER)
        //TODO restore empty state when no data in store? perhaps not possible, so replace is 'isFresh()` (no data inserted yet)
//        store.isEmpty() shouldBe true
    }

    @Test fun null_prealloc_update_delete(){
        val store = openStore()
        val recid = store.preallocate()
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            store.update(recid, LONG, 1L)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            store.get(recid, TT.Serializer_ILLEGAL_ACCESS)
        }
        TT.assertFailsWith(DBException.PreallocRecordAccess::class) {
            store.delete(recid, TT.Serializer_ILLEGAL_ACCESS)
        }
        store.preallocatePut(recid, LONG, 1L)
        store.delete(recid, LONG);

        TT.assertFailsWith(DBException.RecordNotFound::class) {
            store.get(recid, TT.Serializer_ILLEGAL_ACCESS)
        }
    }

    //TODO nullability tests outside of kotlin

}

