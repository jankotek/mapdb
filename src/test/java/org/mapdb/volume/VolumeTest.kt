package org.mapdb.volume

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.nio.ByteBuffer
import java.util.ArrayList
import java.util.Arrays
import java.util.Random

import org.junit.Assert.*
import org.mapdb.CC
import org.mapdb.DBException
import org.mapdb.DBUtil
import org.mapdb.Serializer
import org.mapdb.volume.*
import java.io.*
import java.lang.Byte
import java.nio.file.Files

class VolumeTest {

    companion object {

        internal val scale = org.mapdb.TT.testScale()
        internal val sub = Math.pow(10.0, (2.0 + 4* scale)).toLong()

        internal val BYTE_ARRAY_FAB:Function1<String, Volume> = { file -> ByteArrayVol(CC.PAGE_SHIFT, 0L) }

        internal val MEMORY_VOL_FAB:Function1<String, Volume> = { file -> Volume.MemoryVol(false, CC.PAGE_SHIFT, false, 0L) }

        val VOL_FABS: Array<Function1<String, Volume>> =
                if(org.mapdb.TT.shortTest())
                    arrayOf(BYTE_ARRAY_FAB, MEMORY_VOL_FAB)
                else
                    arrayOf(
                            BYTE_ARRAY_FAB,
                            MEMORY_VOL_FAB,
                        {file -> SingleByteArrayVol(4e7.toInt()) },
                        {file -> Volume.MemoryVol(true, CC.PAGE_SHIFT, false, 0L) },
                        {file ->  Volume.UNSAFE_VOL_FACTORY.makeVolume(null, false, false, CC.PAGE_SHIFT, 0, false)},
                        {file -> FileChannelVol(File(file), false, false, CC.PAGE_SHIFT, 0L) },
                        {file -> RandomAccessFileVol(File(file), false, false, 0L) },
                        {file -> MappedFileVol(File(file), false, false, CC.PAGE_SHIFT, false, 0L, false) },
                        {file -> MappedFileVolSingle(File(file), false, false, 4e7.toLong(), false) },
                        {file -> Volume.MemoryVolSingle(false, 4e7.toLong(), false) }
                    )
    }


    @org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
    class IndividualTest(val fab: Function1<String, Volume>) {


        companion object {

            @org.junit.runners.Parameterized.Parameters
            @Throws(IOException::class)
            @JvmStatic
            fun params(): Iterable<Any> {
                val ret = ArrayList<Any>()
                for (o in VOL_FABS) {
                    ret.add(arrayOf<Any>(o))
                }

                return ret
            }
        }

        @org.junit.Test
        @Throws(Exception::class)
        fun testPackLong() {
            val v = fab(org.mapdb.TT.tempFile().toString())

            v.ensureAvailable(10000)

            var i: Long = 0
            while (i < DBUtil.PACK_LONG_RESULT_MASK) {
                v.clear(0, 20)
                val size = v.putPackedLong(10, i).toLong()
                assertTrue(i > 100000 || size < 6)

                assertEquals(i or (size shl 60), v.getPackedLong(10))
                i = i + 1 + i / 1000
            }
            v.close()
        }


        @org.junit.Test
        @Throws(Throwable::class)
        fun overlap() {
            val v = fab(org.mapdb.TT.tempFile().toString())

            putGetOverlap(v, 100, 1000)
            putGetOverlap(v, CC.PAGE_SIZE - 500, 1000)
            putGetOverlap(v, 2e7.toLong() + 2000, 1e7.toInt())
            putGetOverlapUnalligned(v)

            v.close()

        }

        @org.junit.Test fun hash() {
            val b = ByteArray(11111)
            Random().nextBytes(b)
            val v = fab(org.mapdb.TT.tempFile().toString())
            v.ensureAvailable(b.size.toLong())
            v.putData(0, b, 0, b.size)

            assertEquals(DBUtil.hash(b, 0, b.size, 11), v.hash(0, b.size.toLong(), 11))

            v.close()
        }

        @org.junit.Test fun clear() {
            val offset = 7339936L
            val size = 96
            val v = fab(org.mapdb.TT.tempFile().toString())
            v.ensureAvailable(offset + 10000)
            for (o in 0..offset + 10000 - 1) {
                v.putUnsignedByte(o, 11)
            }
            v.clear(offset, offset + size)

            for (o in 0..offset + 10000 - 1) {
                val b = v.getUnsignedByte(o)
                var expected = 11
                if (o >= offset && o < offset + size)
                    expected = 0
                assertEquals(expected.toLong(), b.toLong())
            }
        }

        @Throws(IOException::class)
        internal fun putGetOverlap(vol: Volume, offset: Long, size: Int) {
            val b = org.mapdb.TT.randomByteArray(size)

            vol.ensureAvailable(offset + size)
            vol.putDataOverlap(offset, b, 0, b.size)

            val b2 = ByteArray(size)
            vol.getDataInputOverlap(offset, size).readFully(b2, 0, size)

            assertTrue(Serializer.BYTE_ARRAY.equals(b, b2))
        }


        @Throws(IOException::class)
        internal fun putGetOverlapUnalligned(vol: Volume) {
            val size = 1e7.toInt()
            val offset = (2e6+2000).toLong()
            vol.ensureAvailable(offset + size)

            val b = org.mapdb.TT.randomByteArray(size)

            val b2 = ByteArray(size + 2000)

            System.arraycopy(b, 0, b2, 1000, size)

            vol.putDataOverlap(offset, b2, 1000, size)

            val b3 = ByteArray(size + 200)
            vol.getDataInputOverlap(offset, size).readFully(b3, 100, size)


            for (i in 0..size - 1) {
                assertEquals(b2[i + 1000].toLong(), b3[i + 100].toLong())
            }
        }

    }


    @org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
    class DoubleTest(internal val fab1: Function1<String, Volume>,
                     internal val fab2: Function1<String, Volume>) {

        companion object {

            @org.junit.runners.Parameterized.Parameters
            @Throws(IOException::class)
            @JvmStatic
            fun params(): Iterable<Any>? {
                val ret = ArrayList<Any>()
                for (o in VOL_FABS) {
                    for (o2 in VOL_FABS) {
                        ret.add(arrayOf<Any>(o, o2))
                    }
                }

                return ret
            }
        }

        @org.junit.Test
        fun unsignedShort_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(8)

            for (i in Character.MIN_VALUE..Character.MAX_VALUE) {
                v1.putUnsignedShort(7, i.toInt())
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i.toLong(), v2.getUnsignedShort(7).toLong())
            }

            v1.close()
            v2.close()
        }


        @org.junit.Test
        fun unsignedByte_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(8)

            for (i in 0..255) {
                v1.putUnsignedByte(7, i)
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i.toLong(), v2.getUnsignedByte(7).toLong())
            }

            v1.close()
            v2.close()
        }


        @org.junit.Test
        fun long_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(8)

            for (i in longArrayOf(1L, 2L, Integer.MAX_VALUE.toLong(), Integer.MIN_VALUE.toLong(), java.lang.Long.MAX_VALUE, java.lang.Long.MIN_VALUE, -1, 0x982e923e8989229L, -2338998239922323233L, 0xFFF8FFL, -0xFFF8FFL, 0xFFL, -0xFFL, 0xFFFFFFFFFF0000L, -0xFFFFFFFFFF0000L)) {
                v1.putLong(7, i)
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i, v2.getLong(7))
            }

            v1.close()
            v2.close()
        }


        @org.junit.Test
        fun long_pack() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(21)
            v2.ensureAvailable(20)
            val b = ByteArray(12)

            var i: Long = 0
            while (i < DBUtil.PACK_LONG_RESULT_MASK) {
                val len = v1.putPackedLong(7, i).toLong()
                v1.getData(7, b, 0, 12)
                v2.putData(7, b, 0, 12)
                assertTrue(len <= 10)
                assertEquals((len shl 60) or i, v2.getPackedLong(7))
                i = i + 1 + i / sub
            }

            v1.close()
            v2.close()
        }


        @org.junit.Test
        fun long_six_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(9)

            var i: Long = 0
            while (i ushr 48 == 0L) {
                v1.putSixLong(7, i)
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i, v2.getSixLong(7))
                i = i + 1 + i / sub
            }

            v1.close()
            v2.close()
        }

        @org.junit.Test
        fun int_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(8)

            for (i in intArrayOf(1, 2, Integer.MAX_VALUE, Integer.MIN_VALUE, -1, -1741778391, -233899233, 16775423, -16775423, 255, -255, 268431360, -268435200)) {
                v1.putInt(7, i)
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i.toLong(), v2.getInt(7).toLong())
            }

            v1.close()
            v2.close()
        }


        @org.junit.Test
        fun byte_compatible() {
            val v1 = fab1(org.mapdb.TT.tempFile().toString())
            val v2 = fab2(org.mapdb.TT.tempFile().toString())

            v1.ensureAvailable(16)
            v2.ensureAvailable(16)
            val b = ByteArray(8)

            for (i in java.lang.Byte.MIN_VALUE..java.lang.Byte.MAX_VALUE - 1 - 1) {
                v1.putByte(7, i.toByte())
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i.toLong(), v2.getByte(7).toLong())
            }


            for (i in 0..255) {
                v1.putUnsignedByte(7, i)
                v1.getData(7, b, 0, 8)
                v2.putData(7, b, 0, 8)
                assertEquals(i.toLong(), v2.getUnsignedByte(7).toLong())
            }


            v1.close()
            v2.close()
        }


    }


    @org.junit.Test fun direct_bb_overallocate() {
        if (org.mapdb.TT.shortTest())
            return

        val vol = Volume.MemoryVol(true, CC.PAGE_SHIFT, false, 0L)
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
        var m = MappedFileVol(f, false, false, CC.PAGE_SHIFT, true, 0L, false)
        assertEquals(1, m.slices.size.toLong())
        m.sync()
        m.close()
        assertEquals(chunkSize, f.length())

        //open mmap file, size should grow to multiple of chunk size
        m = MappedFileVol(f, false, false, CC.PAGE_SHIFT, true, 0L, false)
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

        m = MappedFileVol(f, false, false, CC.PAGE_SHIFT, true, 0L, false)
        assertEquals(2, m.slices.size.toLong())
        m.sync()
        m.ensureAvailable(chunkSize + add + 4)
        assertEquals(chunkSize * 2, f.length())
        assertEquals(11, m.getInt(chunkSize + add).toLong())
        m.sync()
        m.close()
        assertEquals(chunkSize * 2, f.length())

        m = MappedFileVol(f, false, false, CC.PAGE_SHIFT, true, 0L, false)
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

        val v = MappedFileVolSingle(f, false, false, 1000, false)
        assertEquals(1000, f.length())
        assertEquals(112314123, v.getLong(0))
        v.close()
    }

    @org.junit.Test
    @Throws(IOException::class)
    fun lock_double_open() {
        val f = File.createTempFile("mapdbTest", "mapdb")
        val v = RandomAccessFileVol(f, false, false, 0L)
        v.ensureAvailable(8)
        v.putLong(0, 111L)

        //second open should fail, since locks are enabled
        assertTrue(v.fileLocked)

        try {
            val v2 = RandomAccessFileVol(f, false, false, 0L)
            fail()
        } catch (l: DBException.FileLocked) {
            //ignored
        }

        v.close()
        val v2 = RandomAccessFileVol(f, false, false, 0L)

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
                Volume.MemoryVol.FACTORY,
                Volume.MemoryVol.FACTORY_WITH_CLEANER_HACK,
                RandomAccessFileVol.FACTORY,
                SingleByteArrayVol.FACTORY,
                MappedFileVolSingle.FACTORY,
                MappedFileVolSingle.FACTORY_WITH_CLEANER_HACK,
                Volume.UNSAFE_VOL_FACTORY)

        for (fac in factories) {
            val f = org.mapdb.TT.tempFile()
            val initSize = 20 * 1024 * 1024.toLong()
            val vol = fac.makeVolume(f.toString(), false, true, CC.PAGE_SHIFT, initSize, false)
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
                    DBUtil.hash(b, 0, b.size, 0),
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
