package org.mapdb.tree

import org.mapdb.DBConcurrentMap
import org.mapdb.MapExtraTest
import org.mapdb.Serializer

class BTreeMapExtraTest:MapExtraTest(){

    override fun makeMap(): DBConcurrentMap<Int?, String?> {
        return BTreeMap.make(keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING)
    }

}

