package org.mapdb

import org.fest.reflect.core.Reflection
import org.junit.Assert.*
import org.junit.Test
import org.mapdb.volume.RandomAccessFileVol
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.util.concurrent.locks.ReadWriteLock

class StoreTrivialTest : StoreReopenTest() {

    val StoreTrivial.lock:  ReadWriteLock?
        get() = Reflection.method("getLock").`in`(this).invoke() as  ReadWriteLock?


    override fun openStore() = StoreTrivial();

    override fun openStore(file: File) = StoreTrivialTx(file);


    override val headerType: Long = CC.FILE_TYPE_STORETRIVIAL

    @Test fun headerType2(){
        val s = openStore(file)
        s.put(11L, Serializer.LONG)
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
        Utils.lockRead(e.lock) {
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


        s.put(1L, Serializer.LONG);

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
        store.put(11, Serializer.INTEGER)
        store.commit()
        store.put(11, Serializer.INTEGER)
        store.commit()
        assertNotEquals(0, dir.listFiles().size)
        store.close()
        assertEquals(0, dir.listFiles().size)
    }
}
