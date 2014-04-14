package org.mapdb;


import org.junit.Test;

import java.util.Map;

/**
 *
 * @author Alan Franzoni
 */
public class TestTransactions {

    @Test
    public void testSameCollectionInsertDifferentValuesInDifferentTransactions() throws Exception {

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .makeTxMaker();

        DB txInit = txMaker.makeTx();
        Map<Object, Object> mapInit = txInit.getTreeMap("testMap");

        for (int i=0; i<1e4 ; i++ ) {
            mapInit.put(i, String.format("%d", i));

        }
        txInit.commit();

        DB tx1 = txMaker.makeTx();
        DB tx2 = txMaker.makeTx();


        Map map1 = tx1.getTreeMap("testMap");

        map1.put(1, "asd");

        tx1.commit();
        System.out.println("tx1 commit succeeded, map size after tx1 commits: " + txMaker.makeTx().getTreeMap("testMap").size());

        Map map2 = tx2.getTreeMap("testMap");
        map2.put(10001, "somevalue");

        // the following line throws a TxRollbackException
        tx2.commit();
        txMaker.close();
    }

    @Test
    public void testDifferentCollectionsInDifferentTransactions() throws Exception {

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .makeTxMaker();

        DB txInit = txMaker.makeTx();
        Map<Object, Object> mapInit = txInit.getTreeMap("testMap");
        Map<Object, Object> otherMapInit = txInit.getTreeMap("otherMap");

        for (int i=0; i<1e4 ; i++ ) {
            mapInit.put(i, String.format("%d", i));
            otherMapInit.put(i, String.format("%d", i));

        }

        txInit.commit();

        DB tx1 = txMaker.makeTx();
        DB tx2 = txMaker.makeTx();


        Map map1 = tx1.getTreeMap("testMap");

        map1.put(2, "asd");

        tx1.commit();

        Map map2 = tx2.getTreeMap("otherMap");
        map2.put(20, "somevalue");

        // the following line throws a TxRollbackException
        tx2.commit();
        txMaker.close();
    }

    @Test
    public void testSameCollectionModifyDifferentValuesInDifferentTransactions() throws Exception {

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .makeTxMaker();

        DB txInit = txMaker.makeTx();
        Map<Object, Object> mapInit = txInit.getTreeMap("testMap");

        for (int i=0; i<1e4 ; i++ ) {
            mapInit.put(i, String.format("%d", i));

        }
        txInit.commit();

        DB tx1 = txMaker.makeTx();
        DB tx2 = txMaker.makeTx();


        Map map1 = tx1.getTreeMap("testMap");

        map1.put(1, "asd");


        tx1.commit();
        System.out.println("tx1 commit succeeded, map size after tx1 commits: " + txMaker.makeTx().getTreeMap("testMap").size());

        Map map2 = tx2.getTreeMap("testMap");
        map2.put(100, "somevalue");

        // the following line throws a TxRollbackException
        tx2.commit();
        txMaker.close();
    }

    @Test
    public void testTransactionsDoingNothing() throws Exception {

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .makeTxMaker();

        DB txInit = txMaker.makeTx();
        Map<Object, Object> mapInit = txInit.getTreeMap("testMap");

        for (int i=0; i<1e4 ; i++ ) {
            mapInit.put(i, String.format("%d", i));

        }
        txInit.commit();


        DB tx1 = txMaker.makeTx();
        DB tx2 = txMaker.makeTx();


        Map map1 = tx1.getTreeMap("testMap");

        tx1.commit();

        Map map2 = tx2.getTreeMap("testMap");

        // the following line throws a TxRollbackException
        tx2.commit();
        txMaker.close();
    }

}