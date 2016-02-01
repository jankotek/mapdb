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
        return mapMaker(false) as ConcurrentMap<Int?, String?>
    }

    companion object {
        @Parameterized.Parameters
        @JvmStatic
        fun params(): Iterable<Any> {
            return BTreeMap_ConcurrentMap_GuavaTest.params()
        }
    }


    override fun testGenericComparable() {
        //ignored test, must be comparable
    }

    override fun testGenericComparable2() {
        //ignored test, must be comparable
    }

    override fun testMixedComparable() {
        //ignored test, must be comparable
    }

    override fun testComparableFamily() {
        //ignored test, must be comparable
    }

}
