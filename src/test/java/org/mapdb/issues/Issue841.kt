package org.mapdb.issues

import org.junit.Test
import org.mapdb.*
import org.junit.Assert.*
import org.mapdb.serializer.Serializers

class Issue841{

    @Test fun hashSet(){
        assertTrue(DBMaker.memoryDB().make().hashSet("aa", Serializers.STRING).create().add("aa"))
    }



    @Test fun treeSet(){
        assertTrue(DBMaker.memoryDB().make().treeSet("aa", Serializers.STRING).create().add("aa"))
    }

}