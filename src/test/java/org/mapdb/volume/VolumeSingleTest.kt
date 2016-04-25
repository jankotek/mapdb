package org.mapdb.volume

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.CC
import org.mapdb.DataIO
import org.mapdb.Serializer
import org.mapdb.TT
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.*

@org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
class VolumeSingleTest(val fab: Function1<String, Volume>) {


    companion object {

        @org.junit.runners.Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()
            for (o in VolumeTest.VOL_FABS) {
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
        while (i < DataIO.PACK_LONG_RESULT_MASK) {
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

        assertEquals(CC.HASH_FACTORY.hash64().hash(b, 0, b.size, 11), v.hash(0, b.size.toLong(), 11))

        v.close()
    }

    @org.junit.Test fun hashOverlap() {
        val b = ByteArray(CC.PAGE_SIZE.toInt()*3)
        Random().nextBytes(b)
        val v = fab(org.mapdb.TT.tempFile().toString())
        v.ensureAvailable(CC.PAGE_SIZE*4)
        v.putDataOverlap(100,b,0,b.size)

        fun t(offset:Int, size:Int) {
            assertEquals(
                    CC.HASH_FACTORY.hash64().hash(b, offset, size, 11),
                    v.hash(100+offset.toLong(), size.toLong(), 11))
        }
        t(0, b.size)
        if(TT.shortTest().not()){
            val nums = intArrayOf(1,12,16,128,1024,31290,1024*1024-1,1024*1024-128, 1024*1024, 1024*1024+1, 1024*1024+128)
            for(off in nums){
                for(size in nums){
                    t(off, size)
                }
            }
        }
        v.close()
    }

    @Test fun IOStreams(){
        val b = TT.randomByteArray(1024*12*1024)
        val v = fab(TT.tempFile().toString())
        v.copyFrom(ByteArrayInputStream(b))
        assertTrue(v.length()>1024*1024)
        val out = ByteArrayOutputStream()
        v.copyTo(out)

        assertEquals(b.size, out.toByteArray().size)
        assertTrue(Arrays.equals(b, out.toByteArray()))
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

