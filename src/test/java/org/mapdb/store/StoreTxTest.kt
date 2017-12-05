package org.mapdb.store

import org.mapdb.*
import org.junit.Assert.assertEquals
import org.junit.Test
import org.mapdb.serializer.Serializers

abstract class StoreTxTest{

    abstract fun open():StoreTx

    @Test fun rollback_void(){
        val s = open()
        val recid = s.put("aaa", Serializers.STRING)
        s.rollback()
        TT.assertFailsWith(DBException.GetVoid::class.java){
            s.get(recid, Serializers.STRING)
        }
        s.close()
    }


    @Test fun rollback_change(){
        val s = open()
        val recid = s.put("aaa", Serializers.STRING)
        s.commit()
        s.update(recid, "bbb", Serializers.STRING)
        s.rollback()
        assertEquals("aaa", s.get(recid, Serializers.STRING))
        s.close()
    }
}