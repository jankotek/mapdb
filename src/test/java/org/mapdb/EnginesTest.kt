package org.mapdb

import org.junit.*
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import org.junit.Assert.*

/**
 * Tests contract of various implementations of Engine interface
 */
abstract class EnginesTest(val e:Engine){

    Test fun casNulls(){
        val recid = e.put(null, Serializer.BASIC_SERIALIZER);
        fun check(v:Any?) = assertTrue(v == e.get(recid, Serializer.BASIC_SERIALIZER))
        fun cas(old:Any?, new:Any?) = e.compareAndSwap(recid, old, new, Serializer.BASIC_SERIALIZER)

        check(null)
        assertFalse(cas("aa",null))
        check(null)
        assertTrue(cas(null, "aa"))
        check("aa")
        assertFalse(cas(null, "bb"))
        check("aa")
        assertTrue(cas("aa",null))
        check(null)
    }

}

class EnginesDefault: EnginesTest(
        DBMaker.newMemoryDB().makeEngine()
){}

class EnginesDefault_noJournal: EnginesTest(
        DBMaker.newMemoryDB().journalDisable().makeEngine()
){}

