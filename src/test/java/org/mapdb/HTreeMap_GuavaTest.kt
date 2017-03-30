package org.mapdb

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.mapdb.guavaTests.ConcurrentMapInterfaceTest
import java.io.IOException
import java.util.*
import java.util.concurrent.ConcurrentMap


@RunWith(Parameterized::class)
class HTreeMap_GuavaTest(val mapMaker:(generic:Boolean)-> ConcurrentMap<Any?, Any?>) :
        ConcurrentMapInterfaceTest<Int, String>(
            false,  // boolean allowsNullKeys,
            false,  // boolean allowsNullValues,
            true,   // boolean supportsPut,
            true,   // boolean supportsRemove,
            true,   // boolean supportsClear,
            true    // boolean supportsIteratorRemove
    ){

    companion object {

        val singleHashSerializer = object : Serializer<Int> {
            override fun deserialize(input: DataInput2, available: Int) = input.readInt()

            override fun serialize(out: DataOutput2, value: Int) {
                out.writeInt(value)
            }

            override fun hashCode(a: Int, seed: Int): Int {
                //NOTE: fixed hash to generate collisions
                return seed
            }
        }

        @Parameterized.Parameters
        @Throws(IOException::class)
        @JvmStatic
        fun params(): Iterable<Any> {
            val ret = ArrayList<Any>()

            val bools = if(TT.shortTest()) TT.boolsFalse else TT.bools

            for(inlineValue in bools)
            for(singleHash in bools)
            for(segmented in bools)
            for(createExpire in bools)
            for(updateExpire in bools)
            for(getExpire in bools)
            for(onHeap in bools)
            for(counter in bools)
            for(collapse in bools)
            {
                ret.add(arrayOf<Any>({generic:Boolean->

                    var maker =
                            if(segmented) {
                                if(onHeap)DBMaker.heapShardedHashMap(8)
                                else DBMaker.memoryShardedHashMap(8)
                            }else {
                                val db =
                                        if(onHeap) DBMaker.heapDB().make()
                                        else DBMaker.memoryDB().make()
                                db.hashMap("aa")
                            }

                    val keySerializer =
                            if (singleHash.not()) Serializer.INTEGER
                            else singleHashSerializer

                    if(inlineValue)
                        maker.valueInline()

                    if(createExpire)
                        maker.expireAfterCreate(Integer.MAX_VALUE.toLong())
                    if(updateExpire)
                        maker.expireAfterUpdate(Integer.MAX_VALUE.toLong())
                    if(getExpire)
                        maker.expireAfterGet(Integer.MAX_VALUE.toLong())
                    if(counter)
                        maker.counterEnable()

                    if(!generic)
                        maker.keySerializer(keySerializer).valueSerializer(Serializer.STRING)

                    if(!collapse)
                        maker.removeCollapsesIndexTreeDisable()

                    maker.hashSeed(1).create()

                }))

            }

            return ret
        }

    }

    override fun getKeyNotInPopulatedMap(): Int = -10

    override fun getValueNotInPopulatedMap(): String = "-120"
    override fun getSecondValueNotInPopulatedMap(): String = "-121"

    override fun makeEmptyMap(): ConcurrentMap<Int?, String?> {
        @Suppress("UNCHECKED_CAST")
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
