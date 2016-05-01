package org.mapdb

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.volume.FileChannelVol
import org.mapdb.volume.MappedFileVol
import org.mapdb.volume.RandomAccessFileVol
import org.mapdb.StoreAccess.*

class DBMakerTest{

    @Test fun sharded_htreemap_close(){
        val executor = TT.executor()

        val map = DBMaker.heapShardedHashMap(8).expireExecutor(executor).expireAfterCreate(100).create()
        assertTrue(executor.isShutdown.not())
        map.close()
        assertTrue(executor.isShutdown)
        assertTrue(executor.isTerminated)
    }

    @Test fun conc_scale(){
        val db =DBMaker.memoryDB().concurrencyScale(32).make()
        assertEquals(DataIO.shift(32), (db.getStore() as StoreDirect).concShift)
    }


    @Test fun conc_disable(){
        var db =DBMaker.memoryDB().make()
        assertTrue(db.isThreadSafe)
        assertTrue(db.getStore().isThreadSafe)
        assertTrue(db.hashMap("aa1").create().isThreadSafe)
        assertTrue(db.treeMap("aa2").create().isThreadSafe)

        db =DBMaker.memoryDB().concurrencyDisable().make()
        assertFalse(db.isThreadSafe)
        assertFalse(db.getStore().isThreadSafe)
        assertFalse(db.hashMap("aa1").create().isThreadSafe)
        assertFalse(db.treeMap("aa2").create().isThreadSafe)
    }

    @Test fun raf(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).make()
        assertTrue((db.getStore() as StoreDirect).volumeFactory == RandomAccessFileVol.FACTORY)
        file.delete()
    }

    @Test fun channel(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileChannelEnable().make()
        assertTrue((db.getStore() as StoreDirect).volumeFactory == FileChannelVol.FACTORY)
        file.delete()
    }


    @Test fun mmap(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileMmapEnable().make()
        assertTrue((db.getStore() as StoreDirect).volumeFactory is MappedFileVol.MappedFileFactory)
        file.delete()
    }


    @Test fun mmap_if_supported(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileChannelEnable().fileMmapEnableIfSupported().make()
        if(DataIO.JVMSupportsLargeMappedFiles())
            assertTrue((db.getStore() as StoreDirect).volumeFactory is MappedFileVol.MappedFileFactory)
        else
            assertTrue((db.getStore() as StoreDirect).volumeFactory == FileChannelVol.FACTORY)

        file.delete()
    }


    @Test fun readonly_vol(){
        val f = TT.tempFile()
        //fill with content
        var db = DBMaker.fileDB(f).make()
        db.atomicInteger("aa",1)
        db.close()

        fun checkReadOnly(){
            assertTrue(((db.getStore()) as StoreDirect).volume.isReadOnly)
            TT.assertFailsWith(UnsupportedOperationException::class.java){
                db.hashMap("zz").create()
            }
        }

        db = DBMaker.fileDB(f).readOnly().make()
        checkReadOnly()
        db.close()

        db = DBMaker.fileDB(f).readOnly().fileChannelEnable().make()
        checkReadOnly()
        db.close()

        db = DBMaker.fileDB(f).readOnly().fileMmapEnable().make()
        checkReadOnly()
        db.close()

        f.delete()
    }

    @Test fun checksumStore(){
        val db = DBMaker.memoryDB().checksumStoreEnable().make()
        assertTrue(((db.getStore()) as StoreDirect).checksum)
    }
}