package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.jsr166Tests.ConcurrentHashMapTest
import java.util.concurrent.ConcurrentMap

@RunWith(Parameterized::class)
class BTreeMap_HashMap_JSR166Test(
        val mapMaker:(generic:Boolean)-> ConcurrentMap<Any?, Any?>
) : ConcurrentHashMapTest()
{

    override fun makeGenericMap(): ConcurrentMap<Any?, Any?>? {
        return mapMaker(true)
    }

    override fun makeMap(): ConcurrentMap<Int?, String?>? {
        @Suppress("UNCHECKED_CAST")
        return mapMaker(false) as ConcurrentMap<Int?, String?>
    }

    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            return BTreeMap_ConcurrentMap_GuavaTest.params()
        }
    }


}
