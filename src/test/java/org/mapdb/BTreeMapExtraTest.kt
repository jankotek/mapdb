package org.mapdb

class BTreeMapExtraTest:MapExtraTest(){

    override fun makeMap(): MapExtra<Int?, String?> {
        return BTreeMap.make(keySerializer = Serializer.INTEGER, valueSerializer = Serializer.STRING)
    }

}

