package org.mapdb;

import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TxMakerTest{

    TxMaker tx =
            //new TxMaker(new TxEngine(new DB(new StoreHeap()).getEngine(),true));
            DBMaker.newMemoryDB().makeTxMaker();

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

    @Test(timeout = 60000)
    public void concurrent_tx() throws Throwable {
        final int threads = 10;
        final int items = 1000;
        final CountDownLatch l = new CountDownLatch(threads);
        final List<Throwable> ex = new CopyOnWriteArrayList<Throwable>();
        final Collection s = Collections.synchronizedCollection(new HashSet());
        for(int i=0;i<threads;i++){
            final int t=i*items*10000;
            new Thread(){
                @Override
                public void run() {
                    try{
                    for (int index = t; index < t+items; index++) {
                        final int temp = index;
                        s.add(temp);
                          tx.execute(new TxBlock() {

                            @Override
                            public void tx(DB db) throws TxRollbackException {
//							Queue<String> queue = db.getQueue(index + "");
//							queue.offer(temp + "");
                                Map map = db.getHashMap("ha");
                                if(temp!=t)
                                    assertEquals(temp-1,map.get(temp-1));
                                map.put(temp, temp );
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

        Map m = tx.makeTx().getHashMap("ha");
        assertEquals(s.size(),m.size());
        for(Object i:s){
            assertEquals(i, m.get(i));
        }

    }


    @Test(timeout = 60000)
    public void increment() throws Throwable {
        final int threads = 10;
        final int items = 1000;
        DB db = tx.makeTx();
        final long recid = db.getEngine().put(1L,Serializer.LONG);
        db.commit();
        final List<Throwable> ex = new CopyOnWriteArrayList<Throwable>();
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

        assertEquals(Long.valueOf(threads*items+1), tx.makeTx().getEngine().get(recid,Serializer.LONG));


    }


    @Test(timeout = 60000)
    public void cas() throws Throwable {
        final int threads = 10;
        final int items = 1000;
        DB db = tx.makeTx();
        final long recid = db.getEngine().put(1L,Serializer.LONG);
        db.commit();
        final List<Throwable> ex = new CopyOnWriteArrayList<Throwable>();
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

        assertEquals(Long.valueOf(threads*items+1), tx.makeTx().getEngine().get(recid,Serializer.LONG));

    }

    @Test @Ignore
    public void txSnapshot(){

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .snapshotEnable()
                .makeTxMaker();

        DB db = txMaker.makeTx();
        long recid = db.getEngine().put("aa",Serializer.STRING);
        DB snapshot = db.snapshot();
        db.getEngine().update(recid,"bb",Serializer.STRING);
        assertEquals("aa",snapshot.getEngine().get(recid,Serializer.STRING));
        assertEquals("bb",db.getEngine().get(recid,Serializer.STRING));

    }

    @Test
    public void txSnapshot2(){

        TxMaker txMaker = DBMaker
                .newMemoryDB()
                .snapshotEnable()
                .makeTxMaker();

        DB db = txMaker.makeTx();
        long recid = db.getEngine().put("aa",Serializer.STRING);
        db.commit();
        db = txMaker.makeTx();
        DB snapshot = db.snapshot();
        db.getEngine().update(recid,"bb",Serializer.STRING);
        assertEquals("aa",snapshot.getEngine().get(recid,Serializer.STRING));
        assertEquals("bb",db.getEngine().get(recid,Serializer.STRING));

    }
}
