package org.mapdb.tree

import org.mapdb.DBConcurrentMap
import org.mapdb.MapExtraTest
import org.mapdb.serializer.Serializers

class BTreeMapExtraTest:MapExtraTest(){

    override fun makeMap(): DBConcurrentMap<Int?, String?> {
        return BTreeMap.make(keySerializer = Serializers.INTEGER, valueSerializer = Serializers.STRING)
    }

}

