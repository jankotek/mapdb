package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.guavaTests.ConcurrentMapInterfaceTest
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentMap


@RunWith(Parameterized::class)
class BTreeMap_ConcurrentMap_GuavaTest(
        val mapMaker:(generic:Boolean)-> ConcurrentMap<Any?, Any?>
    ):ConcurrentMapInterfaceTest<Int, String>(
            false,  // boolean allowsNullKeys,
            false,  // boolean allowsNullValues,
            true,   // boolean supportsPut,
            true,   // boolean supportsRemove,
            true,   // boolean supportsClear,
            true    // boolean supportsIteratorRemove
    ){

    companion object {


        @Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()

            val bools = if(TT.shortTest()) TT.boolsFalse else TT.bools

            for(inlineValue in bools)
            for(reversedComparator in bools)
            for(small in bools)
            for(storeType in 0..2)
            {
                val store = when(storeType){
                    0-> StoreOnHeap()
                    1-> StoreTrivial()
                    2-> StoreDirect.make()
                    else -> throw AssertionError()
                }

                val nodeSize = if(small) 4 else 32

                ret.add(arrayOf<Any>({generic:Boolean->
                    if(generic)
                        BTreeMap.make<Int,String>(comparator =
                            (if(reversedComparator) Serializer.JAVA.reversed() else Serializer.JAVA) as Comparator<Int>,
                            store = store, maxNodeSize =  nodeSize)
                    else
                        BTreeMap.make(keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING,
                            comparator = if(reversedComparator) Serializer.INTEGER.reversed() else Serializer.INTEGER ,
                            store = store, maxNodeSize =  nodeSize)
                }))

            }

            return ret
        }

    }

    override fun getKeyNotInPopulatedMap(): Int = -10

    override fun getValueNotInPopulatedMap(): String = "-120"
    override fun getSecondValueNotInPopulatedMap(): String = "-121"

    open override fun makeEmptyMap(): ConcurrentMap<Int?, String?> {
        return mapMaker(false) as ConcurrentMap<Int?, String?>
    }

    override fun makePopulatedMap(): ConcurrentMap<Int?, String?>? {
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
