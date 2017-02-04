package org.mapdb

import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.mapdb.StoreAccess.volume
import org.mapdb.VolumeAccess.sliceShift
import org.mapdb.elsa.Bean1
import org.mapdb.volume.ByteArrayVol
import org.mapdb.volume.FileChannelVol
import org.mapdb.volume.MappedFileVol
import org.mapdb.volume.RandomAccessFileVol
import java.util.*

class DBMakerTest{

    @Rule @JvmField
    val expectedException = ExpectedException.none()!!

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
        assertEquals(DataIO.shift(32), (db.store as StoreDirect).concShift)
    }


    @Test fun conc_disable(){
        var db =DBMaker.memoryDB().make()
        assertTrue(db.isThreadSafe)
        assertTrue(db.store.isThreadSafe)
        assertTrue(db.hashMap("aa1").create().isThreadSafe)
        assertTrue(db.treeMap("aa2").create().isThreadSafe)

        db =DBMaker.memoryDB().concurrencyDisable().make()
        assertFalse(db.isThreadSafe)
        assertFalse(db.store.isThreadSafe)
        assertFalse(db.hashMap("aa1").create().isThreadSafe)
        assertFalse(db.treeMap("aa2").create().isThreadSafe)
    }

    @Test fun raf(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).make()
        assertTrue((db.store as StoreDirect).volumeFactory == RandomAccessFileVol.FACTORY)
        file.delete()
    }

    @Test fun channel(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileChannelEnable().make()
        assertTrue((db.store as StoreDirect).volumeFactory == FileChannelVol.FACTORY)
        file.delete()
    }


    @Test fun mmap(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileMmapEnable().make()
        assertTrue((db.store as StoreDirect).volumeFactory is MappedFileVol.MappedFileFactory)
        file.delete()
    }


    @Test fun mmap_if_supported(){
        val file = TT.tempFile()
        val db = DBMaker.fileDB(file).fileChannelEnable().fileMmapEnableIfSupported().make()
        if(DataIO.JVMSupportsLargeMappedFiles())
            assertTrue((db.store as StoreDirect).volumeFactory is MappedFileVol.MappedFileFactory)
        else
            assertTrue((db.store as StoreDirect).volumeFactory == FileChannelVol.FACTORY)

        file.delete()
    }


    @Test fun readonly_vol(){
        val f = TT.tempFile()
        //fill with content
        var db = DBMaker.fileDB(f).make()
        db.atomicInteger("aa",1)
        db.close()

        fun checkReadOnly(){
            assertTrue(((db.store) as StoreDirect).volume.isReadOnly)
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
        assertTrue(((db.store) as StoreDirect).checksum)
    }

    @Test(timeout=10000)
    fun file_lock_wait(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).make()
        TT.fork{
            Thread.sleep(2000)
            db1.close()
        }
        val db2 = DBMaker.fileDB(f).fileLockWait(6000).make()
        db2.close()
        f.delete()
    }


    @Test(timeout=10000)
    fun file_lock_wait2(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).make()
        TT.fork{
            Thread.sleep(2000)
            db1.close()
        }
        val db2 = DBMaker.fileDB(f).fileLockWait().make()
        db2.close()
        f.delete()
    }

    @Test(timeout=10000)
    fun file_lock_wait_time_out_same_jvm() {
        val f = TT.tempFile()

        val db1 = DBMaker.fileDB(f)
                .make()

        try {
            expectedException.expect(DBException.FileLocked::class.java)
            DBMaker.fileDB(f)
                    .fileLockWait(2000)
                    .make()
        } finally {
            db1.close()
            f.delete()
        }
    }

    @Test(timeout=10000)
    fun file_lock_wait_time_out() {
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).make()
        TT.fork {
            Thread.sleep(2000) // Test succeeds if this is commented out
        }

        val maker = DBMaker.fileDB(f).fileLockWait(500)
        try {
            maker.make()
        } catch (e : DBException.FileLocked) {
            // Expected
        }

        db1.close()
        f.delete()
    }

    @Test(timeout=30000)
    fun file_lock_wait_time_out_different_jvm() {
        val f = TT.tempFile()
        val process = TT.forkJvm(ForkedLockTestMain::class.java, f.absolutePath)

        // Wait for the forked process to write to STDOUT, which happens after it
        // has successfully opened and locked the database.
        process.inputStream.read()

        try {
            expectedException.expect(DBException.FileLocked::class.java)
            DBMaker.fileDB(f)
                    .fileLockWait(2000)
                    .make()
        } finally {
            if(!process.isAlive) {
                fail(process.errorStream.reader().readText())
            } else {
                process.destroyForcibly()
                f.delete()
            }
        }
    }

    @Test fun file_lock_disable_RAF(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).make()
        DBMaker.fileDB(f).fileLockDisable().make()
    }

    @Test fun file_lock_disable_RAF2(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).transactionEnable().make()
        DBMaker.fileDB(f).fileLockDisable().transactionEnable().make()
    }

    @Test fun file_lock_disable_Channel(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).make()
        DBMaker.fileDB(f).fileLockDisable().make()
    }

    @Test fun file_lock_disable_Channel2(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).fileChannelEnable().transactionEnable().make()
        DBMaker.fileDB(f).fileChannelEnable().fileLockDisable().transactionEnable().make()
    }

    @Test fun file_lock_disable_mmap(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).fileMmapEnable().make()
        DBMaker.fileDB(f).fileLockDisable().make()
    }

    @Test fun file_lock_disable_mmap2(){
        val f = TT.tempFile()
        val db1 = DBMaker.fileDB(f).transactionEnable().make()
        DBMaker.fileDB(f).fileLockDisable().fileMmapEnable().transactionEnable().make()
    }

    @Test fun fileIncrement(){
        val db = DBMaker.memoryDB().allocateIncrement(100).make()
        val store = db.store as StoreDirect
        val volume = store.volume as ByteArrayVol
        assertEquals(CC.PAGE_SHIFT, volume.sliceShift)
    }


    @Test fun fileIncrement2(){
        val db = DBMaker.memoryDB().allocateIncrement(2*1024*1024).make()
        val store = db.store as StoreDirect
        val volume = store.volume as ByteArrayVol
        assertEquals(1+CC.PAGE_SHIFT, volume.sliceShift)
    }


    @Test fun fromVolume(){
        val vol = ByteArrayVol()
        val db = DBMaker.volumeDB(vol, false).make()
        assertTrue(vol === (db.store as StoreDirect).volume)
    }



    @Test fun classLoader(){
        val classes = ArrayList<String>()
        val cl = object: ClassLoader() {
            override fun loadClass(name: String?): Class<*> {
                classes+= name!!
                return super.loadClass(name)
            }
        }

        val db = DBMaker.memoryDB().classLoader(cl).make()
        assertEquals(db.classLoader, cl )

        val m = db.atomicVar("a").create()
        m.set(Bean1("aa","bb"))
        m.get()

        assert(classes.contains(Bean1::class.java.name))
    }


    object ForkedLockTestMain {
        @JvmStatic
        fun main(args : Array<String>) {
            if(args.size != 1) {
                System.err.println("No database specified!")
                System.exit(3)
            }

            val file = args[0]
            val db1 = DBMaker.fileDB(file).make()
            System.out.println("Locked database.")
            Thread.sleep(60000)
            db1.close()
        }
    }
}
