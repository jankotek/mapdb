package org.mapdb.issues

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Test
import org.mapdb.BTreeMap
import org.mapdb.DBMaker
import org.mapdb.Serializer
import org.mapdb.serializer.SerializerArrayDelta
import java.util.*

class Issue778Test {

    @Test
    fun test() {

        val db = DBMaker.heapDB()
                .closeOnJvmShutdown().make()

        val ser = SerializerArrayDelta(Serializer.STRING)
        val m1 = db.treeMap("test")
                .counterEnable()
                .keySerializer(ser)
                .createOrOpen() as BTreeMap<Array<Any>, String>
        val m2 = db.treeMap("test2")
                .counterEnable()
                .keySerializer(ser)
                .createOrOpen() as BTreeMap<Array<Any>, String>

        val ref = TreeMap<Array<Any>, String>(ser as Comparator<Array<Any>>)

        for (i in 0..99) {
            val counter = i
            m1.put(arrayOf<Any>("two", "three" + counter), "two")
            ref.put(arrayOf<Any>("two", "three" + counter), "two")
        }

        m2.putAll(m1)
        assertEquals(m1.size, m2.size)
        assertEquals(m1, m2)
        assertEquals(m1, ref)
        assertEquals(ref, m2)

        assert(m2.size > 0)
        m2.clear()
        assertEquals(0, m2.size)
        assertFalse(m2.keyIterator().hasNext())
        m2.putAll(m1)
        assertEquals(m1.size, m2.size)
        assertEquals(m1, m2)
        assertEquals(m1, ref)
        assertEquals(ref, m2)

        m2.clear()
        assertEquals(0, m2.size)

    }
}
