package org.mapdb.tree

import org.mapdb.DBMaker
import org.mapdb.Serializer
import org.mapdb.tree.jsr166Tests.ConcurrentSkipListSubSetTest

/**
 * Created by jan on 4/2/16.
 */
class BTreeKeySet_JSR166_ConcurrentSkipListSubSetTest : ConcurrentSkipListSubSetTest(){
    override fun emptySet() = DBMaker
            .memoryDB().make().treeSet("aa")
            .serializer(Serializer.INTEGER).create()

}