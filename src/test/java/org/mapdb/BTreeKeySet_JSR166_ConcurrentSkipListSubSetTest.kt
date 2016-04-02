package org.mapdb

import org.mapdb.jsr166Tests.ConcurrentSkipListSubSetTest
import java.util.concurrent.ConcurrentSkipListSet

/**
 * Created by jan on 4/2/16.
 */
class BTreeKeySet_JSR166_ConcurrentSkipListSubSetTest : ConcurrentSkipListSubSetTest(){
    override fun emptySet() = DBMaker
            .memoryDB().make().treeSet("aa")
            .serializer(Serializer.INTEGER).create()

}