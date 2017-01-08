package org.mapdb.volume

import org.junit.Assert.*
import org.mapdb.CC
import org.mapdb.DBException
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.*

class VolumeTest {

    companion object {

        internal val scale = org.mapdb.TT.testScale()
        internal val sub = Math.pow(10.0, (2.0 + 3* scale)).toLong()

        internal val BYTE_ARRAY_FAB:Function1<String, Volume> = { file -> ByteArrayVol(CC.PAGE_SHIFT, 0L) }

        internal val MEMORY_VOL_FAB:Function1<String, Volume> = { file -> ByteBufferMemoryVol(false, CC.PAGE_SHIFT, false, 0L) }

        val VOL_FABS: Array<Function1<String, Volume>> =
                if(org.mapdb.TT.shortTest())
                    arrayOf(BYTE_ARRAY_FAB, MEMORY_VOL_FAB)
                else
                    arrayOf(
                            BYTE_ARRAY_FAB,
                            MEMORY_VOL_FAB,
                        {file -> SingleByteArrayVol(4e7.toInt()) },
                        {file -> ByteBufferMemoryVol(true, CC.PAGE_SHIFT, false, 0L) },
                        {file ->  Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false, -1L, CC.PAGE_SHIFT, 0, false)},
                        {file -> FileChannelVol(File(file), false, 0L, CC.PAGE_SHIFT, 0L) },
                        {file -> RandomAccessFileVol(File(file), false, 0L, 0L) },
                        {file -> MappedFileVol(File(file), false, 0L, CC.PAGE_SHIFT, false, 0L, false) },
                        {file -> MappedFileVolSingle(File(file), false, 0L, 4e7.toLong(), false) },
                        {file -> ByteBufferMemoryVolSingle(false, 4e7.toLong(), false) },
                        {file -> UnsafeVolume(0, CC.PAGE_SHIFT, 0) }
                    )
    }



    @org.junit.Test fun direct_bb_overallocate() {
        if (org.mapdb.TT.shortTest())
            return

        val vol = ByteBufferMemoryVol(true, CC.PAGE_SHIFT, false, 0L)
        try {
            vol.ensureAvailable(1e10.toLong())
        } catch (e: DBException.OutOfMemory) {
            assertTrue(e.message!!.contains("-XX:MaxDirectMemorySize"))
        }

        vol.close()
    }

    @org.junit.Test fun byte_overallocate() {
        if (org.mapdb.TT.shortTest())
            return

        val vol = ByteArrayVol(CC.PAGE_SHIFT, 0L)
        try {
            vol.ensureAvailable(1e10.toLong())
        } catch (e: DBException.OutOfMemory) {
            assertFalse(e.message!!.contains("-XX:MaxDirectMemorySize"))
        }

        vol.close()
    }

    @org.junit.Test
    @Throws(IOException::class)
    fun mmap_init_size() {
        //test if mmaping file size repeatably increases file
        val f = File.createTempFile("mapdbTest", "mapdb")

        val chunkSize = (1 shl CC.PAGE_SHIFT).toLong()
        val add = 100000L

        //open file channel and write some size
        var raf = RandomAccessFile(f, "rw")
        raf.seek(add)
        raf.writeInt(11)
        raf.close()

        //open mmap file, size should grow to multiple of chunk size
        var m = MappedFileVol(f, false, 0L, CC.PAGE_SHIFT, true, 0L, false)
        assertEquals(1, m.slices.size.toLong())
        m.sync()
        m.close()
        assertEquals(chunkSize, f.length())

        //open mmap file, size should grow to multiple of chunk size
        m = MappedFileVol(f, false, 0L, CC.PAGE_SHIFT, true, 0L, false)
        assertEquals(1, m.slices.size.toLong())
        m.ensureAvailable(add + 4)
        assertEquals(11, m.getInt(add).toLong())
        m.sync()
        m.close()
        assertEquals(chunkSize, f.length())

        raf = RandomAccessFile(f, "rw")
        raf.seek(chunkSize + add)
        raf.writeInt(11)
        raf.close()

        m = MappedFileVol(f, false, 0L, CC.PAGE_SHIFT, true, 0L, false)
        assertEquals(2, m.slices.size.toLong())
        m.sync()
        m.ensureAvailable(chunkSize + add + 4)
        assertEquals(chunkSize * 2, f.length())
        assertEquals(11, m.getInt(chunkSize + add).toLong())
        m.sync()
        m.close()
        assertEquals(chunkSize * 2, f.length())

        m = MappedFileVol(f, false, 0L, CC.PAGE_SHIFT, true, 0L, false)
        m.sync()
        assertEquals(chunkSize * 2, f.length())
        m.ensureAvailable(chunkSize + add + 4)
        assertEquals(11, m.getInt(chunkSize + add).toLong())
        m.sync()
        assertEquals(chunkSize * 2, f.length())

        m.ensureAvailable(chunkSize * 2 + add + 4)
        m.putInt(chunkSize * 2 + add, 11)
        assertEquals(11, m.getInt(chunkSize * 2 + add).toLong())
        m.sync()
        assertEquals(3, m.slices.size.toLong())
        assertEquals(chunkSize * 3, f.length())

        m.close()
        f.delete()
    }

    @org.junit.Test @Throws(IOException::class)
    fun small_mmap_file_single() {
        val f = File.createTempFile("mapdbTest", "mapdb")
        val raf = RandomAccessFile(f, "rw")
        val len = 10000000
        raf.setLength(len.toLong())
        raf.close()
        assertEquals(len.toLong(), f.length())

        val v = MappedFileVol.FACTORY.makeVolume(f.path, true)

        assertTrue(v is MappedFileVolSingle)
        val b = (v as MappedFileVolSingle).buffer
        assertEquals(len.toLong(), b.limit().toLong())
    }

    @org.junit.Test @Throws(IOException::class)
    fun single_mmap_grow() {
        val f = File.createTempFile("mapdbTest", "mapdb")
        val raf = RandomAccessFile(f, "rw")
        raf.seek(0)
        raf.writeLong(112314123)
        raf.close()
        assertEquals(8, f.length())

        val v = MappedFileVolSingle(f, false, 0L, 1000, false)
        assertEquals(1000, f.length())
        assertEquals(112314123, v.getLong(0))
        v.close()
    }

    @org.junit.Test
    @Throws(IOException::class)
    fun lock_double_open() {
        val f = File.createTempFile("mapdbTest", "mapdb")
        val v = RandomAccessFileVol(f, false, 0L, 0L)
        v.ensureAvailable(8)
        v.putLong(0, 111L)

        //second open should fail, since locks are enabled
        assertTrue(v.fileLocked)

        try {
            val v2 = RandomAccessFileVol(f, false, 0L, 0L)
            fail()
        } catch (l: DBException.FileLocked) {
            //ignored
        }

        v.close()
        val v2 = RandomAccessFileVol(f, false, 0L, 0L)

        assertEquals(111L, v2.getLong(0))
    }

    @org.junit.Test fun initsize() {
        if (org.mapdb.TT.shortTest())
            return

        val factories = arrayOf(
                CC.DEFAULT_FILE_VOLUME_FACTORY,
                CC.DEFAULT_MEMORY_VOLUME_FACTORY,
                ByteArrayVol.FACTORY,
                FileChannelVol.FACTORY,
                MappedFileVol.FACTORY,
                MappedFileVol.FACTORY,
                ByteBufferMemoryVol.FACTORY,
                ByteBufferMemoryVol.FACTORY_WITH_CLEANER_HACK,
                RandomAccessFileVol.FACTORY,
                SingleByteArrayVol.FACTORY,
                MappedFileVolSingle.FACTORY,
                MappedFileVolSingle.FACTORY_WITH_CLEANER_HACK,
                Volume.UNSAFE_VOL_FACTORY)

        for (fac in factories) {
            val f = org.mapdb.TT.tempFile()
            val initSize = 20 * 1024 * 1024.toLong()
            val vol = fac.makeVolume(f.toString(), false, -1L, CC.PAGE_SHIFT, initSize, false)
            assertEquals(vol.javaClass.name, initSize, vol.length())
            vol.close()
            f.delete()
        }
    }

    @org.junit.Test fun hash() {
        val r = Random()
        for (i in 0..99) {
            val len = 100 + r.nextInt(1999)
            val b = ByteArray(len)
            r.nextBytes(b)

            val vol = SingleByteArrayVol(len)
            vol.putData(0, b, 0, b.size)

            assertEquals(
                    CC.HASH_FACTORY.hash64().hash(b, 0, b.size, 0),
                    vol.hash(0, b.size.toLong(), 0))

        }
    }

    @org.junit.Test fun clearOverlap() {
        //TODO is this test necessary?
        if (org.mapdb.TT.testScale() < 100)
            return

        val v = ByteArrayVol()
        v.ensureAvailable(5 * 1024 * 1024.toLong())
        val vLength = v.length()
        val ones = ByteArray(1024)
        Arrays.fill(ones, 1.toByte())

        for (size in longArrayOf(100, (1024 * 1024).toLong(), 3 * 1024 * 1024.toLong(), (3 * 1024 * 1024 + 6000).toLong())) {
            for (startPos in 0..vLength - size - 1) {
                //fill with ones
                run {
                    var pos: Long = 0
                    while (pos < vLength) {
                        v.putData(pos, ones, 0, ones.size)
                        pos += ones.size.toLong()
                    }
                }

                //clear section of the volume
                v.clearOverlap(startPos, startPos + size)
                //ensure zeroes
                v.assertZeroes(startPos, startPos + size)

                //ensure ones before
                for (pos in 0..startPos - 1) {
                    if (v.getByte(pos) != 1.toByte())
                        throw AssertionError()
                }

                //ensure ones after
                for (pos in startPos + size..vLength - 1) {
                    if (v.getByte(pos) != 1.toByte())
                        throw AssertionError()
                }
            }
        }
    }

    @org.junit.Test
    fun testClearOverlap2() {
        clearOverlap(0, 1000)
        clearOverlap(0, 10000000)
        clearOverlap(100, 10000000)
        clearOverlap(CC.PAGE_SIZE, 10000000)
        clearOverlap(CC.PAGE_SIZE - 1, CC.PAGE_SIZE * 3)
        clearOverlap(CC.PAGE_SIZE + 1, CC.PAGE_SIZE * 3)
    }

    internal fun clearOverlap(startPos: Long, size: Long) {
        val v = ByteArrayVol()
        v.ensureAvailable(startPos + size + 10000)
        val ones = ByteArray(1024)
        Arrays.fill(ones, 1.toByte())
        val vLength = v.length()

        //fill with ones
        run {
            var pos: Long = 0
            while (pos < vLength) {
                v.putData(pos, ones, 0, ones.size)
                pos += ones.size.toLong()
            }
        }

        //clear section of the volume
        v.clearOverlap(startPos, startPos + size)
        //ensure zeroes
        v.assertZeroes(startPos, startPos + size)

        //ensure ones before
        for (pos in 0..startPos - 1) {
            if (v.getByte(pos) != 1.toByte())
                throw AssertionError()
        }

        //ensure ones after
        for (pos in startPos + size..vLength - 1) {
            if (v.getByte(pos) != 1.toByte())
                throw AssertionError()
        }
    }
    
}
