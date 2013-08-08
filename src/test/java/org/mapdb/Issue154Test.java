package org.mapdb;


import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class Issue154Test {

    @Test
    public void HTreeMap(){
        TxMaker txMaker = DBMaker.newMemoryDB().asyncWriteDisable().makeTxMaker();

        /* Add the item */

        DB db1 = txMaker.makeTx();
        Map<Object, Object> map1 = db1.getHashMap("simple");
        map1.put("a", "b");
        db1.commit();

        /* Remove the item */

        DB db2 = txMaker.makeTx();
        Map<Object, Object> map2 = db2.getHashMap("simple");

        // Make sure the item is still there
        assertEquals("b",map2.get("a"));
        map2.remove("a");
        assertEquals(null,map2.get("a"));
        // ROLLBACK the removal (in theory)
        db2.rollback();

        /* Check for the rolled back item */

        DB db3 = txMaker.makeTx();
        Map<Object, Object> map3 = db3.getHashMap("simple");

        // ***************
        // THIS IS WHERE IT FAILS, but the object should be the same, since it the remove was rolled back
        // ***************

        assertEquals("b",map3.get("a"));

        db3.close();
    }

    @Test public void simple(){
        TxMaker txMaker = DBMaker.newMemoryDB().makeTxMaker();
        Engine engine = txMaker.makeTx().getEngine();
        long recid = engine.put("aa",Serializer.STRING_NOSIZE);
        engine.commit();
        engine = txMaker.makeTx().getEngine();
        assertEquals("aa",engine.get(recid,Serializer.STRING_NOSIZE));
        engine.delete(recid,Serializer.STRING_NOSIZE);
        assertEquals(null,engine.get(recid,Serializer.STRING_NOSIZE));
        engine.rollback();
        engine = txMaker.makeTx().getEngine();
        assertEquals("aa",engine.get(recid,Serializer.STRING_NOSIZE));

    }

    @Test
    public void BTreeMap(){
        TxMaker txMaker = DBMaker.newMemoryDB().makeTxMaker();

        /* Add the item */

        DB db1 = txMaker.makeTx();
        Map<Object, Object> map1 = db1.getTreeMap("simple");
        map1.put("a", "b");
        db1.commit();

        /* Remove the item */

        DB db2 = txMaker.makeTx();
        Map<Object, Object> map2 = db2.getTreeMap("simple");

        // Make sure the item is still there
        assertEquals("b",map2.get("a"));
        map2.remove("a");
        assertEquals(null,map2.get("a"));
        // ROLLBACK the removal (in theory)
        db2.rollback();

        /* Check for the rolled back item */

        DB db3 = txMaker.makeTx();
        Map<Object, Object> map3 = db3.getTreeMap("simple");

        // ***************
        // THIS IS WHERE IT FAILS, but the object should be the same, since it the remove was rolled back
        // ***************

        assertEquals("b",map3.get("a"));

        db3.close();
    }
}
