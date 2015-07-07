package org.mapdb;


import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mapdb.Serializer.BYTE_ARRAY_NOSIZE;
import static org.mapdb.Serializer.STRING;

/**
 * Tests contract of various implementations of Engine interface
 */
public abstract class EngineTest<ENGINE extends Engine>{

    protected abstract ENGINE openEngine();

    void reopen(){
        if(!canReopen()) return;
        e.close();
        e=openEngine();
    }

    boolean canReopen(){return true;}
    boolean canRollback(){return true;}

    ENGINE e;

    @After
    public void close(){
        if(e!=null && !e.isClosed())
            e.close();
    }
    @Test public void put_get(){
        e = openEngine();
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        assertEquals(l, e.get(recid, Serializer.LONG));
    }

    @Test public void put_reopen_get(){
        e = openEngine();
        if(!canReopen()) return;
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        e.commit();
        reopen();
        assertEquals(l, e.get(recid, Serializer.LONG));
    }

    @Test public void put_get_large(){
        e = openEngine();
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }

    @Test public void put_reopen_get_large(){
        e = openEngine();
        if(!canReopen()) return;
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        reopen();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }


    @Test public void first_recid(){
        e = openEngine();
        assertEquals(Store.LAST_RESERVED_RECID+1, e.put(1,Serializer.INTEGER));
    }


    @Test public void compact0(){
        e = openEngine();
        Long v1 = 129031920390121423L;
        Long v2 = 909090901290129990L;
        Long v3 = 998898989L;
        long recid1 = e.put(v1, Serializer.LONG);
        long recid2 = e.put(v2, Serializer.LONG);

        e.commit();
        e.compact();

        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        long recid3 = e.put(v3, Serializer.LONG);
        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        assertEquals(v3, e.get(recid3,Serializer.LONG));
        e.commit();
        assertEquals(v1, e.get(recid1,Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        assertEquals(v3, e.get(recid3,Serializer.LONG));

    }


    @Test public void compact(){
        e = openEngine();
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG));
        }

        e.commit();
        e.compact();

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG));
        }

    }


    @Test public void compact2(){
        e = openEngine();
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<1000;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG));
        }

        e.commit();
        e.compact();
        for(Long l=1000L;l<2000;l++){
            recids.put(l, e.put(l, Serializer.LONG));
        }

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG));
        }
    }


    @Test public void compact_large_record(){
        e = openEngine();
        byte[] b = new byte[100000];
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.compact();
        assertArrayEquals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE));
    }


    @Test public void testSetGet(){
        e = openEngine();
        long recid  = e.put((long) 10000, Serializer.LONG);
        Long  s2 = e.get(recid, Serializer.LONG);
        assertEquals(s2, Long.valueOf(10000));
    }



    @Test
    public void large_record(){
        e = openEngine();
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_update(){
        e = openEngine();
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        Arrays.fill(b, (byte)222);
        e.update(recid, b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
    }

    @Test public void large_record_delete(){
        e = openEngine();
        byte[] b = new byte[100000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        e.delete(recid, BYTE_ARRAY_NOSIZE);
    }


    @Test public void large_record_larger(){
        e = openEngine();
        byte[] b = new byte[10000000];
        Arrays.fill(b, (byte) 111);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertArrayEquals(b,b2);

    }


    @Test public void test_store_reopen(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        reopen();

        String aaa = e.get(recid, Serializer.STRING_NOSIZE);
        assertEquals("aaa",aaa);
    }

    @Test public void test_store_reopen_nocommit(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid,"bbb",Serializer.STRING_NOSIZE);
        reopen();

        String expected = canRollback()&&canReopen()?"aaa":"bbb";
        assertEquals(expected, e.get(recid, Serializer.STRING_NOSIZE));
    }


    @Test public void rollback(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));

    }

    @Test public void rollback_reopen(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));
        reopen();
        assertEquals("aaa",e.get(recid, Serializer.STRING_NOSIZE));

    }


    public static void execNTimes(int n, final Callable r){

        ExecutorService s = Executors.newFixedThreadPool(n);
        final CountDownLatch wait = new CountDownLatch(n);

        List<Future> f = new ArrayList();

        Runnable r2 = new Runnable(){

            @Override
            public void run() {
                wait.countDown();
                try {
                    wait.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    r.call();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };

        for(int i=0;i<n;i++){
            f.add(s.submit(r2));
        }

        s.shutdown();

        for(Future ff:f){
            try {
                ff.get();
            } catch (Exception e) {
                throw new Error(e);
            }
        }

    }

    @Test(timeout = 1000*100) @Ignore
    public void par_update_get() throws InterruptedException {
        e = openEngine();
        int threadNum = 32;
        final long end = (long) (System.currentTimeMillis()+20000);
        e = openEngine();
        final BlockingQueue<Fun.Tuple2<Long,String>> q = new ArrayBlockingQueue(threadNum*10);
        for(int i=0;i<threadNum;i++){
            String b = UtilsTest.randomString(new Random().nextInt(10000));
            long recid = e.put(b,STRING);
            q.put(new Fun.Tuple2(recid,b));
        }


        execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                Random r = new Random();
                while (System.currentTimeMillis() < end) {
                    Fun.Tuple2<Long, String> t = q.take();
                    assertEquals(t.b, e.get(t.a, Serializer.STRING));
                    String b = UtilsTest.randomString(r.nextInt(100000));
                    e.update(t.a, b, Serializer.STRING);
                    q.put(new Fun.Tuple2<Long, String>(t.a, b));
                }
                return null;
            }
        });

        for( Fun.Tuple2<Long,String> t :q){
            assertEquals(t.b, e.get(t.a,Serializer.STRING));
        }

    }


    @Test(timeout = 1000*100) @Ignore
    public void par_cas() throws InterruptedException {
        int threadNum = 32;
        final long end = (long) (System.currentTimeMillis()+20000);
        e = openEngine();
        final BlockingQueue<Fun.Tuple2<Long,String>> q = new ArrayBlockingQueue(threadNum*10);
        for(int i=0;i<threadNum;i++){
            String b = UtilsTest.randomString(new Random().nextInt(10000));
            long recid = e.put(b,STRING);
            q.put(new Fun.Tuple2(recid,b));
        }


        execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                Random r = new Random();
                while (System.currentTimeMillis() < end) {
                    Fun.Tuple2<Long, String> t = q.take();
                    String b = UtilsTest.randomString(r.nextInt(100000));
                    assertTrue(e.compareAndSwap(t.a, t.b, b, Serializer.STRING));
                    q.put(new Fun.Tuple2<Long, String>(t.a, b));
                }
                return null;
            }
        });

        for( Fun.Tuple2<Long,String> t :q){
            assertEquals(t.b, e.get(t.a,Serializer.STRING));
        }

    }

    @Test public void empty_commit(){
        e = openEngine();
        long recid = e.put("aa",Serializer.STRING);
        e.commit();
        e.commit();
        e.close();
        e = openEngine();
        e.close();
    }
}
