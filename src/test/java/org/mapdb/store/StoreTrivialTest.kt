package org.mapdb.store

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.*
import org.mapdb.serializer.Serializers
import org.mapdb.util.lockRead
import org.mapdb.volume.RandomAccessFileVol
import java.io.*
import java.util.concurrent.locks.ReadWriteLock

class StoreTrivialTest : StoreReopenTest() {

    val StoreTrivial.lock:  ReadWriteLock?
        get() = TT.reflectionInvokeMethod(this, "getLock", StoreTrivial::class.java)

    val StoreTrivial.freeRecids:LongArrayStack
        get() = TT.reflectionInvokeMethod(this, "getFreeRecids", StoreTrivial::class.java)


    val StoreTrivial.records: LongObjectHashMap<ByteArray>
        get() = TT.reflectionInvokeMethod(this, "getRecords", StoreTrivial::class.java)


    val StoreTrivial.maxRecid: Long
        get() = TT.reflectionInvokeMethod(this, "getMaxRecid", StoreTrivial::class.java)

    override fun openStore() = StoreTrivial();

    override fun openStore(file: File) = StoreTrivialTx(file);


    override val headerType: Long = CC.FILE_TYPE_STORETRIVIAL

    @Test fun headerType2(){
        val s = openStore(file)
        s.put(11L, Serializers.LONG)
        s.commit()
        s.close()
        val vol = RandomAccessFileVol.FACTORY.makeVolume(file.path+".0.d", true)
        assertEquals(CC.FILE_HEADER,vol.getUnsignedByte(0L).toLong())
        assertEquals(headerType, vol.getUnsignedByte(1L).toLong())
    }

    @Test fun load_save(){
        val e = openStore()
        TT.randomFillStore(e)

        //clone into second store
        val outBytes = ByteArrayOutputStream()
        e.saveTo(outBytes)

        val e2 = openStore()
        e2.loadFrom(ByteArrayInputStream(outBytes.toByteArray()))

        assertEquals(e,e2)

        e.close()
        e2.close()
    }

    @Test fun find_commit_marker(){
        val e = openStore(file)
        for(i in 100 downTo  10){
            File(file.path+"."+i+StoreTrivialTx.COMMIT_MARKER_SUFFIX).createNewFile()
        }
        e.lock.lockRead{
            assertEquals(
                    100L,
                    e.findLattestCommitMarker())
        }
        e.close()
    }


    @Test fun commit_file_num(){
        val s = openStore(file)
        val f0 = File(file.toString()+".0"+StoreTrivialTx.DATA_SUFFIX)
        val m0 = File(file.toString()+".0"+StoreTrivialTx.COMMIT_MARKER_SUFFIX)
        val f1 = File(file.toString()+".1"+StoreTrivialTx.DATA_SUFFIX)
        val m1 = File(file.toString()+".1"+StoreTrivialTx.COMMIT_MARKER_SUFFIX)
        val f2 = File(file.toString()+".2"+StoreTrivialTx.DATA_SUFFIX)
        val m2 = File(file.toString()+".2"+StoreTrivialTx.COMMIT_MARKER_SUFFIX)


        s.put(1L, Serializers.LONG)

        assertTrue(!f0.exists())
        assertTrue(!m0.exists())
        assertTrue(!f1.exists())
        assertTrue(!m1.exists())
        assertTrue(!f2.exists())
        assertTrue(!m2.exists())

        s.commit()
        assertTrue(f0.exists())
        assertTrue(m0.exists())
        assertTrue(!f1.exists())
        assertTrue(!m1.exists())
        assertTrue(!f2.exists())
        assertTrue(!m2.exists())

        s.commit()
        assertTrue(!f0.exists())
        assertTrue(!m0.exists())
        assertTrue(f1.exists())
        assertTrue(m1.exists())
        assertTrue(!f2.exists())
        assertTrue(!m2.exists())
        s.rollback()
        assertTrue(!f0.exists())
        assertTrue(!m0.exists())
        assertTrue(f1.exists())
        assertTrue(m1.exists())
        assertTrue(!f2.exists())
        assertTrue(!m2.exists())
        s.commit()
        assertTrue(!f0.exists())
        assertTrue(!m0.exists())
        assertTrue(!f1.exists())
        assertTrue(!m1.exists())
        assertTrue(f2.exists())
        assertTrue(m2.exists())
        s.commit()
        assertTrue(!f0.exists())
        assertTrue(!m0.exists())
        assertTrue(!f1.exists())
        assertTrue(!m1.exists())
        assertTrue(!f2.exists())
        assertTrue(!m2.exists())

    }


    @Test fun delete_after_close(){
        val dir = TT.tempDir()
        val store = StoreTrivialTx(file=File(dir.path,"aa"),deleteFilesAfterClose = true)
        store.put(11, Serializers.INTEGER)
        store.commit()
        store.put(11, Serializers.INTEGER)
        store.commit()
        assertNotEquals(0, dir.listFiles().size)
        store.close()
        assertEquals(0, dir.listFiles().size)
    }

    @Test fun compact(){
        val store = StoreTrivial()
        val r1 = store.put("aa", Serializers.STRING)
        assertEquals(r1, 1)
        val r2 = store.put("aa", Serializers.STRING)
        assertEquals(r2, 2)
        assertEquals(store.maxRecid, 2)
        assertTrue(store.freeRecids.isEmpty)

        store.delete(r2, Serializers.STRING)
        assertEquals(store.freeRecids.size(),1)
        assertEquals(store.maxRecid, 2)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid, 1)

        store.delete(r1, Serializers.STRING)
        assertEquals(store.freeRecids.size(),1)
        assertEquals(store.maxRecid, 1)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid, 0)

        store.compact()
        assertEquals(store.freeRecids.size(),0)
        assertEquals(store.maxRecid, 0)

    }

}
