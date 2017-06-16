package org.mapdb.tree

import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.Serializer
import org.mapdb.TT
import java.util.concurrent.ConcurrentMap

/**
 * Concurrent tests for HTreeMap
 */
@RunWith(Parameterized::class)
class HTreeMapConcTest(val mapMaker:(generic:Boolean)-> ConcurrentMap<Any?, Any?>) {

    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            return HTreeMap_GuavaTest.params()
        }
    }
    @Test
    fun basicTest(){
        val map = mapMaker(false);
        var max = 10000;
        if(map is HTreeMap && map.keySerializer == Serializer.INTEGER)
            max += 1e6.toInt()*TT.testScale()
        val threadCount = 16

        TT.fork(threadCount){i->
            for(key in i until max step threadCount){
                map.put(key, "aa"+key)
            }
        }
        if(map is HTreeMap)
            map.stores.toSet().forEach{it.verify()}

        assertEquals(max, map.size)
        for(key in 0 until max){
            assertEquals("aa"+key, map[key])
        }
    }
}