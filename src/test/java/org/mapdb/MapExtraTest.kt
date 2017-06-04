package org.mapdb

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.mapdb.tree.HTreeMap
import java.util.*

abstract class MapExtraTest{

    abstract fun makeMap(): DBConcurrentMap<Int?, String?>

    val map = makeMap()


    @Test fun forEach(){
        for(i in 1 ..100)
            map.put(i, "aa"+i)

        val ref = HashMap<Int?,String?>()
        map.forEach { key, value ->
            ref.put(key,value)
        }
        assertEquals(100, ref.size)
        for(i in 1 ..100)
            assertEquals("aa"+i, ref[i])
    }


    @Test fun forEachKey(){
        for(i in 1 ..100)
            map.put(i, "aa"+i)

        val ref = ArrayList<Int?>()
        map.forEachKey { key->
            ref.add(key)
        }
        assertEquals(100, ref.size)
        for(i in 1 ..100)
            assertTrue(ref.contains(i))
    }

    @Test fun forEachValue(){
        for(i in 1 ..100)
            map.put(i, "aa"+i)

        val ref = ArrayList<String?>()
        map.forEachValue { value->
            ref.add(value)
        }
        assertEquals(100, ref.size)
        for(i in 1 ..100)
            assertTrue(ref.contains("aa"+i))
    }

    class HTreeMapExtraTest:MapExtraTest(){
        override fun makeMap(): DBConcurrentMap<Int?, String?>  = HTreeMap.make(
                keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING)

    }

}

