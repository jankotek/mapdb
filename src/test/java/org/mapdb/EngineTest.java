package org.mapdb;


import org.junit.After;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mapdb.Serializer.BYTE_ARRAY_NOSIZE;

/*
 * Tests contract of various implementations of Engine interface
 */
public abstract class EngineTest<ENGINE extends Engine>{

    protected abstract ENGINE openEngine();

    void reopen(){
        if(!canReopen())
            return;
        e.close();
        e=openEngine();
    }

    boolean canReopen(){return true;}
    boolean canRollback(){return true;}

    ENGINE e;

    @After
    public void close(){
        if(e!=null && !e.isClosed()){
            e.close();
            e = null;
        }
    }

    @Test public void put_get(){
        e = openEngine();
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        assertEquals(l, e.get(recid, Serializer.LONG));
    }

    @Test public void put_reopen_get(){
        e = openEngine();
        if(!canReopen())
            return;
        Long l = 11231203099090L;
        long recid = e.put(l, Serializer.LONG);
        e.commit();
        reopen();
        assertEquals(l, e.get(recid, Serializer.LONG));
        e.close();
    }

    @Test public void put_get_large(){
        e = openEngine();
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
        e.close();
    }

    @Test public void put_reopen_get_large(){
        e = openEngine();
        if(!canReopen()) return;
        byte[] b = new byte[(int) 1e6];
        new Random().nextBytes(b);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        reopen();
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
        e.close();
    }


    @Test public void first_recid(){
        e = openEngine();
        assertEquals(Store.RECID_LAST_RESERVED + 1, e.put(1, Serializer.INTEGER));
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
        assertEquals(v1, e.get(recid1, Serializer.LONG));
        assertEquals(v2, e.get(recid2,Serializer.LONG));
        assertEquals(v3, e.get(recid3,Serializer.LONG));
        e.close();
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
        e.close();
    }


    @Test public void compact2(){
        long max = TT.scale()*10000;
        e = openEngine();
        Map<Long,Long> recids = new HashMap<Long, Long>();
        for(Long l=0L;l<max;l++){
            recids.put(l,
                    e.put(l, Serializer.LONG));
        }

        e.commit();
        e.compact();
        for(Long l=max;l<max*2;l++){
            recids.put(l, e.put(l, Serializer.LONG));
        }

        for(Map.Entry<Long,Long> m:recids.entrySet()){
            Long recid= m.getValue();
            Long value = m.getKey();
            assertEquals(value, e.get(recid, Serializer.LONG));
        }
        e.close();
    }


    @Test public void compact_large_record(){
        e = openEngine();
        byte[] b = TT.randomByteArray(100000);
        long recid = e.put(b, Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();
        e.compact();
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
        e.close();
    }


    @Test public void testSetGet(){
        e = openEngine();
        long recid  = e.put((long) 10000, Serializer.LONG);
        Long  s2 = e.get(recid, Serializer.LONG);
        assertEquals(s2, Long.valueOf(10000));
        e.close();
    }



    @Test
    public void large_record(){
        e = openEngine();
        byte[] b = new byte[100000];
        new Random().nextBytes(b);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, b2));
        e.close();
    }

    @Test public void large_record_update(){
        e = openEngine();
        byte[] b = new byte[100000];
        new Random().nextBytes(b);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        new Random().nextBytes(b);
        e.update(recid, b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b,b2));
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b,b2));
        e.close();
    }

    @Test public void large_record_delete(){
        e = openEngine();
        byte[] b = new byte[100000];
        new Random().nextBytes(b);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        e.delete(recid, BYTE_ARRAY_NOSIZE);
        e.close();
    }


    @Test public void large_record_larger(){
        e = openEngine();
        byte[] b = new byte[10000000];
        new Random().nextBytes(b);
        long recid = e.put(b, BYTE_ARRAY_NOSIZE);
        byte[] b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b,b2));
        e.commit();
        reopen();
        b2 = e.get(recid, BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, b2));
        e.close();
    }

    @Test public void empty_update_commit(){
        if(TT.scale()==0)
            return;

        e = openEngine();
        long recid = e.put("", Serializer.STRING_NOSIZE);
        assertEquals("", e.get(recid, Serializer.STRING_NOSIZE));

        for(int i=0;i<10000;i++) {
            String s = TT.randomString(80000);
            e.update(recid, s, Serializer.STRING_NOSIZE);
            assertEquals(s, e.get(recid, Serializer.STRING_NOSIZE));
            e.commit();
            assertEquals(s, e.get(recid, Serializer.STRING_NOSIZE));
        }
        e.close();
    }


    @Test public void test_store_reopen(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        reopen();

        String aaa = e.get(recid, Serializer.STRING_NOSIZE);
        assertEquals("aaa", aaa);
        e.close();
    }

    @Test public void test_store_reopen_nocommit(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);
        reopen();

        String expected = canRollback()&&canReopen()?"aaa":"bbb";
        assertEquals(expected, e.get(recid, Serializer.STRING_NOSIZE));
        e.close();
    }


    @Test public void rollback(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa", e.get(recid, Serializer.STRING_NOSIZE));
        e.close();
    }

    @Test public void rollback_reopen(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);

        if(!canRollback())return;
        e.rollback();

        assertEquals("aaa", e.get(recid, Serializer.STRING_NOSIZE));
        reopen();
        assertEquals("aaa", e.get(recid, Serializer.STRING_NOSIZE));
        e.close();
    }

    /* after deletion it enters preallocated state */
    @Test public void delete_and_get(){
        e = openEngine();
        long recid = e.put("aaa", Serializer.STRING);
        e.delete(recid, Serializer.STRING);
        assertNull(e.get(recid, Serializer.ILLEGAL_ACCESS));
        e.commit();
        reopen();
        long recid2 = e.put("bbb", Serializer.STRING);
        if(e instanceof StoreHeap || e instanceof StoreAppend)
            return; //TODO implement it at those two
        assertEquals(recid, recid2);
        e.close();
    }

    @Test(expected=DBException.EngineGetVoid.class)
    public void get_non_existent(){
        e = openEngine();
        long recid = Engine.RECID_FIRST;
        e.get(recid, Serializer.ILLEGAL_ACCESS);
        e.close();
    }

    @Test
    public void get_non_existent_after_delete_and_compact(){
        e = openEngine();
        long recid = e.put(1L,Serializer.LONG);
        e.delete(recid,Serializer.LONG);
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        e.commit();
        e.compact();
        try{
            e.get(recid, Serializer.STRING);
            if(!(e instanceof StoreAppend)) //TODO remove after compact on StoreAppend
                fail();
        }catch(DBException.EngineGetVoid e){
        }
        e.close();
    }

    @Test public void preallocate_cas(){
        e = openEngine();
        long recid = e.preallocate();
        assertFalse(e.compareAndSwap(recid, 1L, 2L, Serializer.ILLEGAL_ACCESS));
        assertTrue(e.compareAndSwap(recid, null, 2L, Serializer.LONG));
        assertEquals((Long) 2L, e.get(recid, Serializer.LONG));
    }


    @Test public void preallocate_get_update_delete_update_get(){
        e = openEngine();
        long recid = e.preallocate();
        assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        e.update(recid, 1L, Serializer.LONG);
        assertEquals((Long) 1L, e.get(recid, Serializer.LONG));
        e.delete(recid, Serializer.LONG);
        assertNull(e.get(recid, Serializer.ILLEGAL_ACCESS));
        e.update(recid, 1L, Serializer.LONG);
        assertEquals((Long) 1L, e.get(recid, Serializer.LONG));
        e.close();
    }

    @Test public void cas_delete(){
        e = openEngine();
        long recid = e.put(1L, Serializer.LONG);
        assertTrue(e.compareAndSwap(recid, 1L, null, Serializer.LONG));
        assertNull(e.get(recid, Serializer.ILLEGAL_ACCESS));
        e.close();
    }

    @Test public void reserved_recid_exists(){
        e = openEngine();
        for(long recid=1;recid<Engine.RECID_FIRST;recid++){
            assertNull(e.get(recid,Serializer.ILLEGAL_ACCESS));
        }
        try{
            e.get(Engine.RECID_FIRST,Serializer.ILLEGAL_ACCESS);
            fail();
        }catch(DBException.EngineGetVoid e){
        }
        e.close();
    }



    @Test(expected = NullPointerException.class)
    public void NPE_get(){
        e = openEngine();
        e.get(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_put(){
        e = openEngine();
        e.put(1L, null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_update(){
        e = openEngine();
        e.update(1, 1L, null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_cas(){
        e = openEngine();
        e.compareAndSwap(1, 1L, 1L, null);
    }

    @Test(expected = NullPointerException.class)
    public void NPE_delete(){
        e = openEngine();
        e.delete(1L, null);
    }

    @Test public void putGetUpdateDelete(){
        e = openEngine();
        String s = "aaaad9009";
        long recid = e.put(s,Serializer.STRING);

        assertEquals(s, e.get(recid, Serializer.STRING));

        s = "da8898fe89w98fw98f9";
        e.update(recid, s, Serializer.STRING);
        assertEquals(s, e.get(recid, Serializer.STRING));

        e.delete(recid, Serializer.STRING);
        assertNull(e.get(recid, Serializer.STRING));
        e.close();
    }


    @Test public void zero_size_serializer(){
        Serializer s = new Serializer<String>() {

            @Override
            public void serialize(DataOutput out, String value) throws IOException {
                if("".equals(value))
                    return;
                Serializer.STRING.serialize(out,value);
            }

            @Override
            public String deserialize(DataInput in, int available) throws IOException {
                if(available==0)
                    return "";
                return Serializer.STRING.deserialize(in,available);
            }
        };

        e = openEngine();
        long recid = e.put("", s);
        assertEquals("",e.get(recid,s));

        e.update(recid, "a", s);
        assertEquals("a", e.get(recid, s));

        e.compareAndSwap(recid, "a", "", s);
        assertEquals("", e.get(recid, s));


        e.update(recid, "a", s);
        assertEquals("a", e.get(recid, s));

        e.update(recid, "", s);
        assertEquals("", e.get(recid, s));
        e.close();
    }

    @Test
    public void par_update_get() throws InterruptedException {
        int scale = TT.scale();
        if(scale==0)
            return;
        int threadNum = Math.min(4,scale*4);
        final long end = TT.nowPlusMinutes(10);
        e = openEngine();
        final BlockingQueue<Fun.Pair<Long,byte[]>> q = new ArrayBlockingQueue(threadNum*10);
        for(int i=0;i<threadNum;i++){
            byte[] b = TT.randomByteArray(new Random().nextInt(10000));
            long recid = e.put(b,BYTE_ARRAY_NOSIZE);
            q.put(new Fun.Pair(recid,b));
        }


        Exec.execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                Random r = new Random();
                while (System.currentTimeMillis() < end) {
                    Fun.Pair<Long, byte[]> t = q.take();
                    assertTrue(Serializer.BYTE_ARRAY.equals(t.b, e.get(t.a, Serializer.BYTE_ARRAY_NOSIZE)));
                    int size = r.nextInt(1000);
                    if (r.nextInt(10) == 1)
                        size = size * 100;
                    byte[] b = TT.randomByteArray(size);
                    e.update(t.a, b, Serializer.BYTE_ARRAY_NOSIZE);
                    q.put(new Fun.Pair<Long, byte[]>(t.a, b));
                }
                return null;
            }
        });

        for( Fun.Pair<Long,byte[]> t :q){
            assertTrue(Serializer.BYTE_ARRAY.equals(t.b, e.get(t.a, Serializer.BYTE_ARRAY_NOSIZE)));
        }
        e.close();
    }


    @Test
    public void par_cas() throws InterruptedException {
        int scale = TT.scale();
        if(scale==0)
            return;
        int threadNum = 8*scale;
        final long end = TT.nowPlusMinutes(10);
        e = openEngine();
        final BlockingQueue<Fun.Pair<Long,byte[]>> q = new ArrayBlockingQueue(threadNum*10);
        for(int i=0;i<threadNum;i++){
            byte[] b = TT.randomByteArray(new Random().nextInt(10000));
            long recid = e.put(b,BYTE_ARRAY_NOSIZE);
            q.put(new Fun.Pair(recid,b));
        }


        Exec.execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                Random r = new Random();
                while(System.currentTimeMillis()<end){
                    Fun.Pair<Long,byte[]> t = q.take();
                    int size = r.nextInt(10000);
                    if(r.nextInt(10)==1)
                        size = size*100;
                    byte[] b = TT.randomByteArray(size);
                    assertTrue(e.compareAndSwap(t.a, t.b, b, Serializer.BYTE_ARRAY_NOSIZE));
                    q.put(new Fun.Pair<Long,byte[]>(t.a,b));
                }
                return null;
            }
        });

        for( Fun.Pair<Long,byte[]> t :q){
            assertTrue(Serializer.BYTE_ARRAY.equals(t.b, e.get(t.a, Serializer.BYTE_ARRAY_NOSIZE)));
        }
        e.close();
    }

    @Test
    public void par_update_get_compact() throws InterruptedException {
        int scale = TT.scale();
        if(scale==0)
            return;
        int threadNum = Math.min(4,scale*4);
        final long end = TT.nowPlusMinutes(10);
        e = openEngine();
        final BlockingQueue<Fun.Pair<Long,byte[]>> q = new ArrayBlockingQueue(threadNum*10);
        for(int i=0;i<threadNum;i++){
            byte[] b = TT.randomByteArray(new Random().nextInt(10000));
            long recid = e.put(b,BYTE_ARRAY_NOSIZE);
            q.put(new Fun.Pair(recid,b));
        }

        final CountDownLatch l = new CountDownLatch(2);
        Thread tt = new Thread(){
            @Override
            public void run() {
                try {
                    while (l.getCount() > 1)
                        e.compact();
                }finally {
                    l.countDown();
                }
            }
        };
        tt.setDaemon(true);
        tt.run();

        Exec.execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                Random r = new Random();
                while (System.currentTimeMillis() < end) {
                    Fun.Pair<Long, byte[]> t = q.take();
                    assertTrue(Serializer.BYTE_ARRAY.equals(t.b, e.get(t.a, Serializer.BYTE_ARRAY_NOSIZE)));
                    int size = r.nextInt(1000);
                    if (r.nextInt(10) == 1)
                        size = size * 100;
                    byte[] b = TT.randomByteArray(size);
                    e.update(t.a, b, Serializer.BYTE_ARRAY_NOSIZE);
                    q.put(new Fun.Pair<Long, byte[]>(t.a, b));
                }
                return null;
            }
        });
        l.countDown();
        l.await();

        for( Fun.Pair<Long,byte[]> t :q){
            assertTrue(Serializer.BYTE_ARRAY.equals(t.b, e.get(t.a, Serializer.BYTE_ARRAY_NOSIZE)));
        }
        e.close();
    }


    @Test public void update_reserved_recid(){
        e = openEngine();
        e.update(Engine.RECID_NAME_CATALOG,111L,Serializer.LONG);
        assertEquals(new Long(111L), e.get(Engine.RECID_NAME_CATALOG, Serializer.LONG));
        e.commit();
        assertEquals(new Long(111L), e.get(Engine.RECID_NAME_CATALOG, Serializer.LONG));
        e.close();
    }



    @Test public void update_reserved_recid_large(){
        e = openEngine();
        byte[] data = TT.randomByteArray((int) 1e7);
        e.update(Engine.RECID_NAME_CATALOG,data,Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(data, e.get(Engine.RECID_NAME_CATALOG, Serializer.BYTE_ARRAY_NOSIZE)));
        e.commit();
        assertTrue(Serializer.BYTE_ARRAY.equals(data, e.get(Engine.RECID_NAME_CATALOG, Serializer.BYTE_ARRAY_NOSIZE)));
        e.close();
    }

    @Test public void cas_uses_serializer(){
        Random r = new Random();
        byte[] data = new byte[1024];
        r.nextBytes(data);

        e = openEngine();
        long recid = e.put(data, Serializer.BYTE_ARRAY);

        byte[] data2 = new byte[100];
        r.nextBytes(data2);
        assertTrue(e.compareAndSwap(recid, data.clone(), data2.clone(), Serializer.BYTE_ARRAY));

        assertTrue(Serializer.BYTE_ARRAY.equals(data2, e.get(recid, Serializer.BYTE_ARRAY)));
        e.close();
    }

    @Test public void nosize_array(){
        e = openEngine();
        byte[] b = new byte[0];
        long recid = e.put(b,Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));

        b = new byte[]{1,2,3};
        e.update(recid,b,Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));

        b = new byte[]{};
        e.update(recid,b,Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(b, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));

        e.delete(recid, Serializer.BYTE_ARRAY_NOSIZE);
        assertTrue(Serializer.BYTE_ARRAY.equals(null, e.get(recid, Serializer.BYTE_ARRAY_NOSIZE)));
        e.close();
    }

    @Test public void compact_double_recid_reuse(){
        e = openEngine();
        if(e instanceof StoreAppend)
            return; //TODO reenable once StoreAppend has compaction
        long recid1 = e.put("aa",Serializer.STRING);
        long recid2 = e.put("bb",Serializer.STRING);
        e.compact();
        e.delete(recid1, Serializer.STRING);
        e.compact();
        e.delete(recid2, Serializer.STRING);
        e.compact();

        TT.sortAndEquals(
                new long[]{recid1, recid2},
                new long[]{e.preallocate(),e.preallocate()});

        e.close();
    }

    @Test public void snapshot(){
        e = openEngine();
        if(!e.canSnapshot())
            return;

        long recid = e.put("a",Serializer.STRING);
        Engine snapshot = e.snapshot();
        e.update(recid, "b", Serializer.STRING);
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.close();
    }

    @Test public void snapshot_after_rollback(){
        e = openEngine();
        if(!e.canSnapshot() || !e.canRollback())
            return;

        long recid = e.put("a",Serializer.STRING);
        Engine snapshot = e.snapshot();
        e.update(recid,"b",Serializer.STRING);
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.rollback();
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.close();
    }

    @Test public void snapshot_after_commit(){
        e = openEngine();
        if(!e.canSnapshot())
            return;

        long recid = e.put("a",Serializer.STRING);
        Engine snapshot = e.snapshot();
        e.update(recid,"b",Serializer.STRING);
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.commit();
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.close();
    }

    @Test public void snapshot_after_commit2(){
        e = openEngine();
        if(!e.canSnapshot())
            return;

        long recid = e.put("a",Serializer.STRING);
        e.commit();
        Engine snapshot = e.snapshot();
        e.update(recid,"b",Serializer.STRING);
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.commit();
        assertEquals("a", snapshot.get(recid, Serializer.STRING));
        e.close();
    }


    // double close should not fail, but other operation are allowed to throw exceptions
    @Test public void double_close(){
        e = openEngine();
        e.close();
        e.close();
    }

    @Test public void insert_many_reopen_check() throws InterruptedException {
        e = openEngine();
        int max = 1000;
        int size = 100000;
        Random r = new Random(0);
        List<Long> recids = new ArrayList<Long>();
        for(int j=0;j<max;j++){
            byte[] b = new byte[r.nextInt(size)];
            r.nextBytes(b);
            long recid = e.put(b,Serializer.BYTE_ARRAY_NOSIZE);
            recids.add(recid);
        }
        e.commit();

        reopen();

        r = new Random(0);
        for (long recid : recids) {
            byte[] b = new byte[r.nextInt(size)];
            r.nextBytes(b);
            byte[] b2 = e.get(recid, Serializer.BYTE_ARRAY_NOSIZE);
            assertTrue("Data were not commited recid="+recid, Arrays.equals(b, b2));
        }
    }

    @Test public void recover_with_interrupt() throws InterruptedException {
        int scale = TT.scale();
        if(scale==0)
            return;
        e = openEngine();
        if(!e.canRollback() || e instanceof StoreHeap) //TODO engine might have crash recovery, but no rollbacks
            return;


        //fill recids
        final int max = scale*1000;
        final ArrayList<Long> recids = new ArrayList<Long>();
        final AtomicLong a = new AtomicLong(10);
        final long counterRecid = e.put(a.get(), Serializer.LONG);
        Random r = new Random(a.get());
        for(int j=0;j<max;j++){
            byte[] b = new byte[r.nextInt(100000)];
            r.nextBytes(b);
            long recid = e.put(b,Serializer.BYTE_ARRAY_NOSIZE);
            recids.add(recid);
        }

        e.commit();

        long endTime = TT.nowPlusMinutes(10);

        while(endTime>System.currentTimeMillis()) {

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        for (; ; ) {
                            long A = a.incrementAndGet();
                            Random r = new Random(A);
                            e.update(counterRecid, A, Serializer.LONG);

                            for (long recid : recids) {
                                byte[] b = new byte[r.nextInt(100000)];
                                r.nextBytes(b);
                                e.update(recid, b, Serializer.BYTE_ARRAY_NOSIZE);
                            }
                            e.commit();
                        }
                    }finally {
                        latch.countDown();
                    }
                }
            };
            t.start();
            Thread.sleep(5000);
            t.stop();
            latch.await();
            if(!e.isClosed()){
                close();
            }

            //reopen and check the content
            e = openEngine();

            //check if A-1 was commited
            long A = e.get(counterRecid, Serializer.LONG);
            assertTrue(""+A+" - "+a.get(), A == a.get() || A == a.get() - 1);
            r = new Random(A);
            for (long recid : recids) {
                byte[] b = new byte[r.nextInt(100000)];
                r.nextBytes(b);
                byte[] b2 = e.get(recid, Serializer.BYTE_ARRAY_NOSIZE);
                assertTrue("Data were not commited recid="+recid, Arrays.equals(b, b2));
            }
            a.set(A);
        }
        e.close();

    }

    @Test public void commit_huge(){
        if(TT.shortTest())
            return;
        e = openEngine();
        long recid = e.put(new byte[1000 * 1000 * 1000], Serializer.BYTE_ARRAY_NOSIZE);
        e.commit();

        reopen();

        byte[] b = e.get(recid, Serializer.BYTE_ARRAY_NOSIZE);
        assertEquals(1000*1000*1000, b.length);
        for(byte bb:b){
            assertEquals(0,bb);
        }
        e.close();
    }

    @Test public void dirty_compact(){
        e = openEngine();
        long recid1 = e.put(new byte[1000 * 1000 ], Serializer.BYTE_ARRAY_NOSIZE);
        long recid2 = e.put(new byte[1000 * 1000], Serializer.BYTE_ARRAY_NOSIZE);
        e.delete(recid1, Serializer.BYTE_ARRAY_NOSIZE);
        e.compact();
        e.commit();
        assertArrayEquals(new byte[1000*1000], e.get(recid2, Serializer.BYTE_ARRAY_NOSIZE));
        e.close();
    }
}
