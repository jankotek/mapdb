package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TxMakerTest{

    TxMaker tx;

    @Before public void init(){
        tx  =
                //new TxMaker(new TxEngine(new DB(new StoreHeap()).getEngine(),true));
                DBMaker.memoryDB().makeTxMaker();
    }

    @After public void destroy(){
        if(tx!=null){
            tx.close();
        }
    }

    @Test public void simple_commit(){
        DB db =tx.makeTx();
        db.hashMap("test").put("aa", "bb");
        db.commit();
        assertEquals("bb", tx.makeTx().hashMap("test").get("aa"));
    }

    @Test public void simple_rollback(){
        DB db =tx.makeTx();
        db.hashMap("test").put("aa", "bb");
        db.rollback();
        assertEquals(null, tx.makeTx().hashMap("test").get("aa"));
    }

    @Test public void commit_conflict(){
        DB db0 = tx.makeTx();
        long recid = db0.getEngine().put(111, Serializer.INTEGER);
        db0.commit();
        DB db1 = tx.makeTx();
        db1.getEngine().update(recid, 222, Serializer.INTEGER);
        DB db2= tx.makeTx();
        db2.getEngine().update(recid, 333, Serializer.INTEGER);
        db2.commit();

        try{
            //will fail, record was already modified
            db1.commit();

            fail("should throw exception");
        }catch(TxRollbackException e){
            //expected
        }


        assertEquals(Integer.valueOf(333), tx.makeTx().getEngine().get(recid, Serializer.INTEGER));

    }

    @Test
    public void concurrent_tx() throws Throwable {
        int scale = TT.scale();
        if(scale==0)
            return;
        final int threads = scale*4;
        final long items = 100000*scale;
        final AtomicInteger ii = new AtomicInteger();
        final Collection s = new ConcurrentSkipListSet();
        Exec.execNTimes(threads, new Callable() {
            @Override
            public Object call() throws Exception {
                final long t = ii.incrementAndGet() * items * 10000;
                for (long index = t; index < t + items; index++) {
                    final long temp = index;
                    s.add(temp);
                    tx.execute(new TxBlock() {

                        @Override
                        public void tx(DB db) throws TxRollbackException {
//							Queue<String> queue = db.getQueue(index + "");
//							queue.offer(temp + "");
                            Map map = db.hashMap("ha");
                            if (temp != t)
                                assertEquals(temp - 1, map.get(temp - 1));
                            map.put(temp, temp);
                        }
                    });
                }
                return null;
            }
        });

        Map m = tx.makeTx().hashMap("ha");
        assertEquals(s.size(),m.size());
        for(Object i:s){
            assertEquals(i, m.get(i));
        }

    }


    @Test
    public void single_tx() throws Throwable {
        final int items = 1000;
        final AtomicInteger ii = new AtomicInteger();
        final Collection s = new ConcurrentSkipListSet();
        final int t=ii.incrementAndGet()*items*10000;
        for (int index = t; index < t+items; index++) {
            final int temp = index;
            s.add(temp);
            tx.execute(new TxBlock() {

                @Override
                public void tx(DB db) throws TxRollbackException {
                    Map map = db.hashMap("ha");
                    if(temp!=t)
                        assertEquals(temp-1,map.get(temp-1));
                    map.put(temp, temp );
                }
            });
        }

        Map m = tx.makeTx().hashMap("ha");
        assertEquals(s.size(),m.size());
        for(Object i:s){
            assertEquals(i, m.get(i));
        }

    }



    @Test
    public void increment() throws Throwable {
        int scale = TT.scale();
        if(scale==0)
            return;
        final int threads = scale*4;
        final long items = 100000*scale;
        DB db = tx.makeTx();
        final long recid = db.getEngine().put(1L,Serializer.LONG);
        db.commit();
        final List<Throwable> ex = Collections.synchronizedList(new ArrayList<Throwable>());
        final CountDownLatch l = new CountDownLatch(threads);
        for(int i=0;i<threads;i++){
            new Thread(){
                @Override
                public void run() {
                    try{
                        for (int j = 0;j<items;j++) {
                            tx.execute(new TxBlock() {
                                @Override
                                public void tx(DB db) throws TxRollbackException {
                                    long old = db.getEngine().get(recid, Serializer.LONG);
                                    db.getEngine().update(recid,old+1,Serializer.LONG);
                                }
                            });
                        }
                    }catch(Throwable e){
                        e.printStackTrace();
                        ex.add(e);
                    }finally{
                        l.countDown();
                    }
                }
            }.start();
        }
        while(!l.await(100, TimeUnit.MILLISECONDS) && ex.isEmpty()){}

        if(!ex.isEmpty())
            throw ex.get(0);

        assertEquals(Long.valueOf(threads * items + 1), tx.makeTx().getEngine().get(recid, Serializer.LONG));


    }


    @Test
    public void cas() throws Throwable {
        int scale = TT.scale();
        if(scale==0)
            return;
        final int threads = scale*4;
        final long items = 100000*scale;
        DB db = tx.makeTx();
        final long recid = db.getEngine().put(1L,Serializer.LONG);
        db.commit();
        final List<Throwable> ex = Collections.synchronizedList(new ArrayList<Throwable>());
        final CountDownLatch l = new CountDownLatch(threads);
        for(int i=0;i<threads;i++){
            new Thread(){
                @Override
                public void run() {
                    try{
                        for (int j = 0;j<items;j++) {
                            tx.execute(new TxBlock() {
                                @Override
                                public void tx(DB db) throws TxRollbackException {
                                    long old = db.getEngine().get(recid, Serializer.LONG);
                                    while(!db.getEngine().compareAndSwap(recid,old,old+1,Serializer.LONG)){

                                    }
                                }
                            });
                        }
                    }catch(Throwable e){
                        e.printStackTrace();
                        ex.add(e);
                    }finally{
                        l.countDown();
                    }
                }
            }.start();
        }
        while(!l.await(100, TimeUnit.MILLISECONDS) && ex.isEmpty()){}

        if(!ex.isEmpty())
            throw ex.get(0);

        assertEquals(Long.valueOf(threads * items + 1), tx.makeTx().getEngine().get(recid, Serializer.LONG));

    }

    @Test @Ignore
    public void txSnapshot(){

        TxMaker txMaker = DBMaker
                .memoryDB()
                .snapshotEnable()
                .makeTxMaker();

        DB db = txMaker.makeTx();
        long recid = db.getEngine().put("aa", Serializer.STRING);
        DB snapshot = db.snapshot();
        db.getEngine().update(recid, "bb", Serializer.STRING);
        assertEquals("aa",snapshot.getEngine().get(recid,Serializer.STRING));
        assertEquals("bb",db.getEngine().get(recid,Serializer.STRING));
        txMaker.close();

    }

    @Test @Ignore
    public void txSnapshot2(){

        TxMaker txMaker = DBMaker
                .memoryDB()
                .snapshotEnable()
                .makeTxMaker();

        DB db = txMaker.makeTx();
        long recid = db.getEngine().put("aa", Serializer.STRING);
        db.commit();
        db = txMaker.makeTx();
        DB snapshot = db.snapshot();
        db.getEngine().update(recid, "bb", Serializer.STRING);
        assertEquals("aa", snapshot.getEngine().get(recid, Serializer.STRING));
        assertEquals("bb",db.getEngine().get(recid,Serializer.STRING));
        txMaker.close();

    }


    @Test @Ignore //TODO reenable test
    public void testMVCC() {
        TxMaker txMaker =
                DBMaker.memoryDB().makeTxMaker();
        {
// set up the initial state of the database
            DB tx = txMaker.makeTx();
            BTreeMap<Object, Object> map = tx.createTreeMap("MyMap").valuesOutsideNodesEnable().make();
            map.put("Value1", 1234);
            map.put("Value2", 1000);
            tx.commit();
        }

// Transaction A: read-only; used to check isolation level
        DB txA = txMaker.makeTx();
        BTreeMap<Object, Object> mapTxA = txA.getTreeMap("MyMap");

// Transaction B: will set Value1 to 47
        DB txB = txMaker.makeTx();
        BTreeMap<Object, Object> mapTxB = txB.getTreeMap("MyMap");

// Transaction C: will set Value2 to 2000
        DB txC = txMaker.makeTx();
        BTreeMap<Object, Object> mapTxC = txC.getTreeMap("MyMap");

// perform the work in C (while B is open)
        mapTxC.put("Value2", 2000);
        txC.commit();

// make sure that isolation level of Transaction A is not violated
        assertEquals(1234, mapTxA.get("Value1"));
        assertEquals(1000, mapTxA.get("Value2"));

// perform work in B (note that we change different keys than in C)
        mapTxB.put("Value1", 47);
        txB.commit();  // FAILS with TxRollbackException

// make sure that isolation level of Transaction A is not violated
        assertEquals(1234, mapTxA.get("Value1"));
        assertEquals(1000, mapTxA.get("Value2"));

// Transaction D: read-only; used to check that commits were successful
        DB txD = txMaker.makeTx();
        BTreeMap<Object, Object> mapTxD = txD.getTreeMap("MyMap");

// ensure that D sees the results of B and C
        assertEquals(47, mapTxD.get("Value1"));
        assertEquals(2000, mapTxD.get("Value2"));
        txMaker.close();
    }

    @Test
    public void testMVCCHashMap() {
        TxMaker txMaker =
                DBMaker.memoryDB().makeTxMaker();
        {
// set up the initial state of the database
            DB tx = txMaker.makeTx();
            Map<Object, Object> map = tx.createHashMap("MyMap").make();
            map.put("Value1", 1234);
            map.put("Value2", 1000);
            tx.commit();
        }

// Transaction A: read-only; used to check isolation level
        DB txA = txMaker.makeTx();
        Map<Object, Object> mapTxA = txA.hashMap("MyMap");

// Transaction B: will set Value1 to 47
        DB txB = txMaker.makeTx();
        Map<Object, Object> mapTxB = txB.hashMap("MyMap");

// Transaction C: will set Value2 to 2000
        DB txC = txMaker.makeTx();
        Map<Object, Object> mapTxC = txC.hashMap("MyMap");

// perform the work in C (while B is open)
        mapTxC.put("Value2", 2000);
        txC.commit();

// make sure that isolation level of Transaction A is not violated
        assertEquals(1234, mapTxA.get("Value1"));
        assertEquals(1000, mapTxA.get("Value2"));

// perform work in B (note that we change different keys than in C)
        mapTxB.put("Value1", 47);
        txB.commit();  // FAILS with TxRollbackException

// make sure that isolation level of Transaction A is not violated
        assertEquals(1234, mapTxA.get("Value1"));
        assertEquals(1000, mapTxA.get("Value2"));

// Transaction D: read-only; used to check that commits were successful
        DB txD = txMaker.makeTx();
        Map<Object, Object> mapTxD = txD.hashMap("MyMap");

// ensure that D sees the results of B and C
        assertEquals(47, mapTxD.get("Value1"));
        assertEquals(2000, mapTxD.get("Value2"));
        txMaker.close();
    }


    @Test public void cas_null(){
        TxMaker txMaker =
                DBMaker.memoryDB().makeTxMaker();

        DB tx = txMaker.makeTx();
        Atomic.Var v = tx.atomicVar("aa");
        tx.commit();

        tx = txMaker.makeTx();
        v = tx.atomicVar("aa");
        assertTrue(v.compareAndSet(null, "bb"));
        tx.commit();

        tx = txMaker.makeTx();
        v = tx.atomicVar("aa");
        assertEquals("bb",v.get());
        tx.commit();

        txMaker.close();
    }

    @Test public void testDuplicateClose() {
        tx.close();
        tx.close();
    }
}
