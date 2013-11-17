package org.mapdb;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public abstract class CacheStressTest {


    public static class TransactionsHashTable extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().make();
        }
    }
    public static class AsyncHashTable extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable().make();
        }
    }


    public static class SmallHashTable extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheSize(3).make();
        }
    }
    public static class HashTable extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable().make();
        }
    }

    public static class NoCache extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheDisable()
                    .make();
        }
    }

    public static class LRU extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheLRUEnable()
                    .make();
        }
    }

    public static class Ref extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheHardRefEnable()
                    .make();
        }
    }

    public static class SoftRef extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheSoftRefEnable()
                    .make();
        }
    }


    public static class WeakRef extends CacheStressTest{
        @Override DB create() {
            return DBMaker.newMemoryDB().transactionDisable()
                    .cacheWeakRefEnable()
                    .make();
        }
    }



    final int threadNum = 30;
    final long incNum = 100000;

    abstract DB create();

    @Test public void testCompareAndSwap() throws InterruptedException {
        final DB db = create();
        final long[] recids = new long[10];
        for(int i=0;i<recids.length;i++){
            recids[i] = db.engine.put(0L, Serializer.LONG);
        }


        ExecutorService e = Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            e.submit(new Runnable() {
                @Override public void run() {
                    for(;;){
                        boolean allDone = true;
                        for(int i=0;i<recids.length;i++){
                            for(;;){
                                long v = db.getEngine().get(recids[i],Serializer.LONG);
                                if(v<incNum*threadNum){
                                    allDone = false;
                                    if(db.getEngine().compareAndSwap(recids[i],v,v+1, Serializer.LONG))
                                        break;
                                }else{
                                    break;
                                }
                            }
                        }
                        if(allDone)
                            return;
                    }
                }
            });
        }

        e.shutdown();
        while(!e.awaitTermination(1000, TimeUnit.MILLISECONDS));

        for(int i=0;i<recids.length;i++){
            assertEquals(Long.valueOf(incNum*threadNum), db.engine.get(recids[i], Serializer.LONG));
        }
    }

    @Test public void testMultiLinearIncrement() throws InterruptedException {
        final DB db = create();
        final long[] recids = new long[threadNum];
        for(int i=0;i<recids.length;i++){
            recids[i] = db.engine.put(0L, Serializer.LONG);
        }


        ExecutorService e = Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            final long recid = recids[i];
            e.submit(new Runnable() {
                @Override public void run() {
                    for(long i=0;i<incNum;i++){
                        long old = db.getEngine().get(recid, Serializer.LONG);
                        assertEquals(i,old);
                        db.getEngine().update(recid,old+1,Serializer.LONG);
                    }
                }
            });
        }

        e.shutdown();
        while(!e.awaitTermination(1000, TimeUnit.MILLISECONDS));

        for(int i=0;i<recids.length;i++){
            assertEquals(Long.valueOf(incNum), db.engine.get(recids[i], Serializer.LONG));
        }
    }

    @Test public void testAtomicIncrement() throws InterruptedException {
        final DB db = create();
        final Atomic.Long l = db.getAtomicLong("t");

        ExecutorService e = Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            e.submit(new Runnable() {
                @Override public void run() {
                    for(long j=0;j<incNum;j++){
                        l.incrementAndGet();
                    }
                }
            });
        }

        e.shutdown();
        while(!e.awaitTermination(1000, TimeUnit.MILLISECONDS));

        assertEquals(incNum*threadNum,l.get());
    }

    @Test public void testLinearIncrement() throws InterruptedException {
        final DB db = create();
        final Atomic.Long l = db.getAtomicLong("t");

        for(long i=0;i<incNum;i++){
            l.incrementAndGet();
        }

        assertEquals(incNum,l.get());
    }

}
