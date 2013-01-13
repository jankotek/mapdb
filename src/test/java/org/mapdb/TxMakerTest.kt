package org.mapdb

import org.junit.*
import kotlin.test.assertEquals
import kotlin.test.fail
import kotlin.test.assertTrue

class TxMakerTest{

    fun tx() = DBMaker.newMemoryDB().makeTxMaker();

    Test fun simple_commit(){
        val tx = tx();
        val db =tx.makeTx();
        db.getHashMap<String,String>("test").put("aa","bb");
        db.commit();
        assertEquals("bb", tx.makeTx().getHashMap<String,String>("test").get("aa"))
    }

    Test fun simple_rollback(){
        val tx = tx();
        val db =tx.makeTx();
        db.getHashMap<String,String>("test").put("aa","bb");
        db.rollback();
        assertEquals(null, tx.makeTx().getHashMap<String,String>("test").get("aa"))
    }

    Test fun commit_conflict(){
        val tx = tx();
        val db0 = tx.makeTx();
        val recid = db0.getEngine().put(111, Serializer.INTEGER_SERIALIZER);
        db0.commit();
        val db1 = tx.makeTx()
        db1.getEngine().update(recid, 222, Serializer.INTEGER_SERIALIZER);
        try{
            tx.makeTx().getEngine().update(recid, 333, Serializer.INTEGER_SERIALIZER);
            fail("should throw exception")
        }catch(e:TxRollbackException){
            //expected
        }

        //original transaction should complete well
        db1.commit();

        assertTrue(222 == tx.makeTx().getEngine().get(recid, Serializer.INTEGER_SERIALIZER))

    }
}
