package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.guavaTests.ConcurrentMapInterfaceTest
import org.mapdb.serializer.GroupSerializer
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentMap


@RunWith(Parameterized::class)
class BTreeMap_ConcurrentMap_GuavaTest(
        val mapMaker:(generic:Boolean?)-> ConcurrentMap<Any?, Any?>
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
            for(otherComparator in bools)
            for(small in bools)
            for(storeType in 0..2)
            for(threadSafe in bools)
            for(counter in bools)
            {
                ret.add(arrayOf<Any>({generic:Boolean?->
                    val store = when(storeType){
                        0-> StoreOnHeap()
                        1-> StoreTrivial()
                        2-> StoreDirect.make()
                        else -> throw AssertionError()
                    }

                    val nodeSize = if(small) 4 else 32
                    val counterRecid = if(counter) store.put(0L, Serializer.LONG) else 0L
                    var keySer:GroupSerializer<Int> = if(generic==null) Serializer.INTEGER else {
                            if(generic) Serializer.JAVA as GroupSerializer<Int> else Serializer.INTEGER
                        }

                    if(otherComparator)
                        keySer = object: GroupSerializer<Int> by keySer{
                            override fun compare(o1: Int?, o2: Int?): Int {
                                throw AssertionError()
                            }

                            override fun equals(a1: Int?, a2: Int?): Boolean {
                                throw AssertionError()
                            }
                        }

                    val valSer = if(generic==null) Serializer.INTEGER else{
                            if(generic) Serializer.JAVA as GroupSerializer<String> else Serializer.STRING
                        }
                    BTreeMap.make(keySerializer = keySer, valueSerializer = valSer,
                            comparator = if(otherComparator) Serializer.JAVA as Comparator<Int> else keySer,
                            store = store, maxNodeSize =  nodeSize, threadSafe = threadSafe,
                            counterRecid = counterRecid)
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
