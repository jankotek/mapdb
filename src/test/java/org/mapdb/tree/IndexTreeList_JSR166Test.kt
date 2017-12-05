package org.mapdb.tree

import org.mapdb.store.*
import org.mapdb.*
import org.mapdb.serializer.Serializers
import org.mapdb.tree.jsr166Tests.CopyOnWriteArrayListTest

class IndexTreeList_JSR166Test:CopyOnWriteArrayListTest(){

    override fun emptyArray():MutableList<Int?> {
        val store = StoreDirect.make();
        val index = IndexTreeLongLongMap.make(store)
        val list = IndexTreeList(store = store, serializer= Serializers.INTEGER, isThreadSafe = true,
                map =index, counterRecid =  store.put(0L, Serializers.LONG_PACKED))
        return list
    }

}