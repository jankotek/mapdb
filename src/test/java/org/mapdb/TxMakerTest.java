package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TxMakerTest{

    TxMaker tx = DBMaker.newMemoryDB().makeTxMaker();

    @Test public void simple_commit(){
        DB db =tx.makeTx();
        db.getHashMap("test").put("aa", "bb");
        db.commit();
        assertEquals("bb", tx.makeTx().getHashMap("test").get("aa"));
    }

    @Test public void simple_rollback(){
        DB db =tx.makeTx();
        db.getHashMap("test").put("aa", "bb");
        db.rollback();
        assertEquals(null, tx.makeTx().getHashMap("test").get("aa"));
    }

    @Test public void commit_conflict(){
        DB db0 = tx.makeTx();
        long recid = db0.getEngine().put(111, Serializer.INTEGER_SERIALIZER);
        db0.commit();
        DB db1 = tx.makeTx();
        db1.getEngine().update(recid, 222, Serializer.INTEGER_SERIALIZER);
        try{
            tx.makeTx().getEngine().update(recid, 333, Serializer.INTEGER_SERIALIZER);
            fail("should throw exception");
        }catch(TxRollbackException e){
            //expected
        }

        //original transaction should complete well
        db1.commit();

        assertEquals(Integer.valueOf(222), tx.makeTx().getEngine().get(recid, Serializer.INTEGER_SERIALIZER));

    }
}
