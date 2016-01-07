package org.mapdb

import org.junit.Assert.*
import org.junit.Test


class IndexTreeListJavaTest{

    @Test fun treeGet(){
        val s = StoreDirect.make()

        val recid = s.put(longArrayOf(0L,0L), IndexTreeListJava.dirSer)


    }


}