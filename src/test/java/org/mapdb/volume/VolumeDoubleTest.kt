package org.mapdb.volume

import org.junit.Assert
import org.mapdb.util.DataIO
import java.io.IOException
import java.util.*


@org.junit.runner.RunWith(org.junit.runners.Parameterized::class)
class VolumeDoubleTest(internal val fab1: Function1<String, Volume>,
                 internal val fab2: Function1<String, Volume>) {

    companion object {

        @org.junit.runners.Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any>? {
            val ret = ArrayList<Any>()
            for (o in VolumeTest.VOL_FABS) {
                for (o2 in VolumeTest.VOL_FABS) {
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
            Assert.assertEquals(i.toLong(), v2.getUnsignedShort(7).toLong())
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
            Assert.assertEquals(i.toLong(), v2.getUnsignedByte(7).toLong())
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
            Assert.assertEquals(i, v2.getLong(7))
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
        while (i < DataIO.PACK_LONG_RESULT_MASK) {
            val len = v1.putPackedLong(7, i).toLong()
            v1.getData(7, b, 0, 12)
            v2.putData(7, b, 0, 12)
            Assert.assertTrue(len <= 10)
            Assert.assertEquals((len shl 60) or i, v2.getPackedLong(7))
            i = i + 1 + i / VolumeTest.sub
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
            Assert.assertEquals(i, v2.getSixLong(7))
            i = i + 1 + i / VolumeTest.sub
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
            Assert.assertEquals(i.toLong(), v2.getInt(7).toLong())
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
            Assert.assertEquals(i.toLong(), v2.getByte(7).toLong())
        }


        for (i in 0..255) {
            v1.putUnsignedByte(7, i)
            v1.getData(7, b, 0, 8)
            v2.putData(7, b, 0, 8)
            Assert.assertEquals(i.toLong(), v2.getUnsignedByte(7).toLong())
        }


        v1.close()
        v2.close()
    }


}

