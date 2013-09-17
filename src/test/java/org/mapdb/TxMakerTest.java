package org.mapdb;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TxMakerTest{

    TxMaker tx =
            //new TxMaker(new SnapshotEngine(new DB(new StoreHeap()).getEngine()));
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
        try{
            tx.makeTx().getEngine().update(recid, 333, Serializer.INTEGER);
            fail("should throw exception");
        }catch(TxRollbackException e){
            //expected
        }

        //original transaction should complete well
        db1.commit();

        assertEquals(Integer.valueOf(222), tx.makeTx().getEngine().get(recid, Serializer.INTEGER));

    }

    @Test public void concurrent_tx() throws Throwable {
        final int threads = 10;
        final int items = 1000;
        final CountDownLatch l = new CountDownLatch(threads);
        final List<Throwable> ex = new CopyOnWriteArrayList<Throwable>();
        final Collection s = Collections.synchronizedCollection(new HashSet());
        for(int i=0;i<threads;i++){
            final int t=i*items*100;
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
        assertEquals(s.size(),tx.counter.get());
        assertEquals(s.size(),m.size());
        for(Object i:s){
            assertEquals(i, m.get(i));
        }

    }
}
