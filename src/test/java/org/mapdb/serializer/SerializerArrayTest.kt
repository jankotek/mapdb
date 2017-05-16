package org.mapdb.serializer

import org.junit.Assert.*
import org.junit.Test
import org.mapdb.Serializer
import org.mapdb.TT
import java.util.*

class SerializerArrayTest {

    @Test
    fun subtype() {
        val s = arrayOf("aa", "bb")
        val ser = SerializerArray(Serializer.STRING, String::class.java)

        val s2 = ser.clone(s)
        assertTrue(Arrays.equals(s, s2))
    }

    @Test fun array_hashCode(){
        val data = HashMap<Serializer<*>, Any>()

        data[Serializer.BYTE_ARRAY] = byteArrayOf(1)
        data[Serializer.BYTE_ARRAY_DELTA] = byteArrayOf(1)
        data[Serializer.BYTE_ARRAY_DELTA2] = byteArrayOf(1)
        data[Serializer.BYTE_ARRAY_NOSIZE] = byteArrayOf(1)

        data[Serializer.CHAR_ARRAY] = charArrayOf(1.toChar())

        data[Serializer.SHORT_ARRAY] = shortArrayOf(1)

        data[Serializer.INT_ARRAY] = intArrayOf(1)

        data[Serializer.LONG_ARRAY] = longArrayOf(1)
        data[Serializer.RECID_ARRAY] = longArrayOf(1)

        data[Serializer.DOUBLE_ARRAY] = doubleArrayOf(1.0)

        data[Serializer.FLOAT_ARRAY] = floatArrayOf(1.0F)

        //check all static fields are included in map
       Serializer::class.java.fields
               .filter{it.name.toLowerCase().contains("array")}
               .sortedBy { it.name }
               .forEach {assertTrue("not found ${it.name}", data.containsKey(it.get(null)))}



        data.forEach{ ser, value ->
            val ser2 = ser as Serializer<Any>
            val hash0 = ser2.hashCode(value, 0)
            val hash1 = ser2.hashCode(value, 1)

            assertNotEquals(hash0, System.identityHashCode(value))
            if(!ser.toString().contains("SerializerCharArray"))
                assertNotEquals(hash0, hash1)

            val cloned = TT.clone(value, ser2)
            assertEquals(0, ser2.compare(value, cloned))

            assertEquals(hash0, ser2.hashCode(cloned, 0))

            assertTrue(ser2.equals(value, cloned))
        }
    }

}