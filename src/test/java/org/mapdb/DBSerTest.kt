package org.mapdb

import org.junit.Test
import org.junit.Assert.*

/**
 * Tests Serialization abstraction in DB
 */
class DBSerTest{

    @Test fun named(){
        val f = TT.tempFile();
        var db = DBMaker.fileDB(f).make()

        var atom = db.atomicInteger("atom").create()
        atom.set(1111)

        var map = db.hashMap("map").create() as MutableMap<Any,Any>
        map.put(11, atom)
        db.close()

        db = DBMaker.fileDB(f).make()

        map = db.hashMap("map").open() as MutableMap<Any,Any>
        val o = map[11]
        assertTrue(o is Atomic.Integer && o.get()==1111)

        atom = db.atomicInteger("atom").open()

        assertTrue(o===atom)
        db.close()
        f.delete()
    }

}
