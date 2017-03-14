package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.guavaTests.SortedMapInterfaceTest
import java.util.*
import java.util.concurrent.ConcurrentMap

/**
 * Created by jan on 1/29/16.
 */
@RunWith(Parameterized::class)
class BTreeMap_SortedMap_GuavaTest(val mapMaker:(generic:Boolean)-> ConcurrentMap<Any?, Any?>) :
        SortedMapInterfaceTest<Int, String>(
                false,  // boolean allowsNullKeys,
                false,  // boolean allowsNullValues,
                true,   // boolean supportsPut,
                true,   // boolean supportsRemove,
                true    // boolean supportsClear,
        ) {

    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            return BTreeMap_ConcurrentMap_GuavaTest.params()
        }
    }


    override fun getKeyNotInPopulatedMap(): Int = -10

    override fun getValueNotInPopulatedMap(): String = "-120"

    override fun makeEmptyMap(): NavigableMap<Int?, String?> {
        @Suppress("UNCHECKED_CAST")
        return mapMaker(false) as NavigableMap<Int?, String?>
    }

    override fun makePopulatedMap(): NavigableMap<Int?, String?>? {
        val ret = makeEmptyMap()
        for(i in 0 until 30) {
            ret.put(i,  "aa"+i)
        }
        return ret;
    }


    override fun supportsValuesHashCode(map: MutableMap<Int, String>?): Boolean {
        // keySerializer returns wrong hash on purpose for this test, so pass it
        return false;
    }

}