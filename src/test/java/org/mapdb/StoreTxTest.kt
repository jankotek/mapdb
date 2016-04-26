package org.mapdb

import org.junit.Test
import org.junit.Assert.*

abstract class StoreTxTest{

    abstract fun open():StoreTx

    @Test fun rollback_void(){
        val s = open()
        val recid = s.put("aaa", Serializer.STRING)
        s.rollback()
        TT.assertFailsWith(DBException.GetVoid::class.java){
            s.get(recid, Serializer.STRING)
        }
        s.close()
    }


    @Test fun rollback_change(){
        val s = open()
        val recid = s.put("aaa", Serializer.STRING)
        s.commit()
        s.update(recid, "bbb", Serializer.STRING)
        s.rollback()
        assertEquals("aaa", s.get(recid, Serializer.STRING))
        s.close()
    }
}