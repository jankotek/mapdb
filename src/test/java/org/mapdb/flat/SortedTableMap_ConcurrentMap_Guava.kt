package org.mapdb.flat

import org.mapdb.*
import org.mapdb.serializer.Serializers
import org.mapdb.tree.guavaTests.ConcurrentMapInterfaceTest
import java.util.concurrent.ConcurrentMap

class SortedTableMap_ConcurrentMap_Guava:
        ConcurrentMapInterfaceTest<Int, String>(
            false,  // boolean allowsNullKeys,
            false,  // boolean allowsNullValues,
            false,   // boolean supportsPut,
            false,   // boolean supportsRemove,
            false,   // boolean supportsClear,
            false    // boolean supportsIteratorRemove
){
    override fun getKeyNotInPopulatedMap(): Int? {
        return 51
    }

    override fun getValueNotInPopulatedMap(): String? {
        return "511"
    }

    override fun getSecondValueNotInPopulatedMap(): String? {
        return "521"
    }

    override fun makeEmptyMap(): ConcurrentMap<Int, String>? {
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializers.INTEGER,
                valueSerializer = Serializers.STRING,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
            )
        return consumer.create()
    }

    override fun makePopulatedMap(): ConcurrentMap<Int, String>? {
        val consumer = SortedTableMap.createFromSink(
                keySerializer = Serializers.INTEGER,
                valueSerializer = Serializers.STRING,
                volume = CC.DEFAULT_MEMORY_VOLUME_FACTORY.makeVolume(null, false)
            )
        for(i in 1..100){
            consumer.put(Pair(i*2, ""+i*10))
        }

        return consumer.create()
    }

    override fun supportsValuesHashCode(map: MutableMap<Int, String>?): Boolean {
        // keySerializer returns wrong hash on purpose for this test, so pass it
        return false;
    }

}