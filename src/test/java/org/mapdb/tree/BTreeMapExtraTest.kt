package org.mapdb.tree

import org.mapdb.MapExtra
import org.mapdb.MapExtraTest
import org.mapdb.Serializer

class BTreeMapExtraTest:MapExtraTest(){

    override fun makeMap(): MapExtra<Int?, String?> {
        return BTreeMap.make(keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING)
    }

}

