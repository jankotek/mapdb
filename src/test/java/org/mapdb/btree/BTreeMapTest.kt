package org.mapdb.btree

import org.mapdb.guavaTests.MapInterfaceTest
import org.mapdb.ser.Serializers
import org.mapdb.store.StoreOnHeapSer

class GuavaBTreeMapTest: MapInterfaceTest<String, Int>(false,false,true,true,true) {

    override fun makeEmptyMap(): MutableMap<String, Int> {
        return BTreeMap.empty(store=StoreOnHeapSer(), keySer= Serializers.STRING, valSer=Serializers.INTEGER)
    }

    override fun makePopulatedMap(): MutableMap<String, Int> {
        val m = makeEmptyMap();
        m.put("1",1)
        m.put("2",2)
        m.put("3",3)
        return m
    }

    override fun getKeyNotInPopulatedMap(): String = "4"

    override fun getValueNotInPopulatedMap(): Int  = 4

}