package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.jsr166Tests.ConcurrentSkipListMapTest
import java.util.concurrent.ConcurrentNavigableMap

@RunWith(Parameterized::class)
class BTreeMap_ConcurrentSkipListMapTest_JSR166Test(
        val mapMaker:(generic:Boolean?)-> ConcurrentNavigableMap<Int, String>
) : ConcurrentSkipListMapTest()
{

    override fun emptyMap(): ConcurrentNavigableMap<Int, String>? {
        return mapMaker(false)
    }

    override fun emptyIntMap(): ConcurrentNavigableMap<Int, Int>? {
        @Suppress("UNCHECKED_CAST")
        return mapMaker(null) as ConcurrentNavigableMap<Int, Int>
    }

    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            return BTreeMap_ConcurrentMap_GuavaTest.params()
        }
    }

}
