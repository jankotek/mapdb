package org.mapdb;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

/**
 * @author Jan Kotek
 */
public class AsyncWriteEngineTest extends TestFile{
    AsyncWriteEngine engine;

    @Before public void reopenStore() throws IOException {
        assertNotNull(index);
        if(engine !=null)
           engine.close();
        engine =  new AsyncWriteEngine(new StorageDirect(index), true);
    }


    @Test public void write_fetch_update_delete() throws IOException {
        long recid = engine.recordPut("aaa",Serializer.STRING_SERIALIZER);
        assertEquals("aaa", engine.recordGet(recid, Serializer.STRING_SERIALIZER));
        reopenStore();
        assertEquals("aaa", engine.recordGet(recid, Serializer.STRING_SERIALIZER));
        engine.recordUpdate(recid, "bbb", Serializer.STRING_SERIALIZER);
        assertEquals("bbb", engine.recordGet(recid, Serializer.STRING_SERIALIZER));
        reopenStore();
        assertEquals("bbb", engine.recordGet(recid, Serializer.STRING_SERIALIZER));

    }


    @Test(timeout = 0xFFFF)
     public void concurrent_updates_test() throws InterruptedException, IOException {


        final int threadNum = 16;
        final int updates = 1000;
        final CountDownLatch latch = new CountDownLatch(threadNum);
        final Map<Integer,Long> recids = new ConcurrentHashMap<Integer, Long>();


        for(int i = 0;i<threadNum; i++){
            final int num = i;
            new Thread(new Runnable() {
                @Override public void run() {
                    long recid = engine.recordPut("START-",Serializer.STRING_SERIALIZER);
                    recids.put(num, recid);
                    for(int i = 0;i<updates; i++){
                        String str= engine.recordGet(recid, Serializer.STRING_SERIALIZER);
                        str +=num+",";
                        engine.recordUpdate(recid, str, Serializer.STRING_SERIALIZER);
                    }
                    latch.countDown();
                }
            }).start();
        }


        latch.await();

        reopenStore();


        assertEquals(recids.size(),threadNum);
        for(int i = 0;i<threadNum; i++){
            long recid = recids.get(i);

            String expectedStr ="START-";
            for(int j=0;j<updates;j++)
                expectedStr +=i+",";

            String v = engine.recordGet(recid, Serializer.STRING_SERIALIZER);
            assertEquals(expectedStr, v);
        }
    }
    
    @Test public void async_commit(){
        final AtomicLong putCounter = new AtomicLong();
        StorageTrans t = new StorageTrans(index){
            @Override
            public <A> long recordPut(A value, Serializer<A> serializer) {
                putCounter.incrementAndGet();
                return super.recordPut(value, serializer);
            }
        };
        AsyncWriteEngine a = new AsyncWriteEngine(t, true);
        byte[] b = new byte[124];

        long max = 100;

        ArrayList<Long> l = new ArrayList<Long>();
        for(int i=0;i<max;i++){
            long recid = a.recordPut(b,Serializer.BASIC_SERIALIZER);
            l.add(recid);
        }
        //make commit just after bunch of records was added,
        // we need to test that all records made it into transaction log
        a.commit();
        assertEquals(max, putCounter.longValue() - a.newRecids.size());
        assertTrue(a.writes.isEmpty());
        //'crash' async engine, destroy its write queue
        a.engine = null;
        t.close();

        //now reopen db and check ths
        t = new StorageTrans(index);
        a = new AsyncWriteEngine(t, true);
        for(Integer i=0;i<max;i++){
            long recid = l.get(i);
            assertArrayEquals(b, (byte[]) a.recordGet(recid, Serializer.BASIC_SERIALIZER));
        }
    }

    @Test
    public void trans_deadlock_issue5() {
        assumeTrue(CC.FULL_TEST);

        int COUNT = 10000000;
        DB db = DBMaker.newTempFileDB().make();
        Map map = db.getTreeMap("treemap");

        for (int i = 0; i < COUNT; i++) {
            if (i % 100000 == 0) {
                db.commit();
            }
            map.put(i, (double) i);
        }
        for (int i = 0; i < COUNT; i++) {
            if (i % 100000 == 0) {
                db.commit();
            }
            assertEquals(map.get(i), (double) i);
        }
    }
}
