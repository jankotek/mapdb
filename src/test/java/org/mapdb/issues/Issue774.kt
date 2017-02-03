package org.mapdb.issues

import org.junit.Test
import org.mapdb.DBMaker

class Issue774{
    @Test fun test(){
        val db = DBMaker.memoryDB().make()
        val map = db.hashMap("Test").createOrOpen()

        map.put("animal", "Dog")
        db.close()
    }
}