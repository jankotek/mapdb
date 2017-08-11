package org.mapdb.issues

import org.junit.Test
import org.mapdb.*
import org.junit.Assert.*

class Issue841{

    @Test fun hashSet(){
        assertTrue(DBMaker.memoryDB().make().hashSet("aa",Serializer.STRING).create().add("aa"))
    }



    @Test fun treeSet(){
        assertTrue(DBMaker.memoryDB().make().treeSet("aa",Serializer.STRING).create().add("aa"))
    }

}