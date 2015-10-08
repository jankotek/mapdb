package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class HTreeMap2Test {

    Engine engine;

    DB db;

    @Before public void init2(){
        engine = DBMaker.memoryDB().transactionDisable().makeEngine();
        db = new DB(engine);
    }


    @After
    public void close(){
        db.close();
    }





    @Test public void testDirSerializer() throws IOException {


        Object dir = new int[4];

        for(int slot=1;slot<127;slot+=1 +slot/5){
            dir = HTreeMap.dirPut(dir,slot,slot*1111);
        }

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        HTreeMap.DIR_SERIALIZER.serialize(out,dir);

        DataIO.DataInputByteBuffer in = swap(out);

        int[] dir2 = (int[]) HTreeMap.DIR_SERIALIZER.deserialize(in, -1);
        assertTrue(Arrays.equals((int[])dir,dir2));

        for(int slot=1;slot<127;slot+=1 +slot/5){
            int offset = HTreeMap.dirOffsetFromSlot(dir2,slot);
            assertEquals(slot*1111, HTreeMap.dirGet(dir2, offset));
        }
    }

    DataIO.DataInputByteBuffer swap(DataIO.DataOutputByteArray d){
        byte[] b = d.copyBytes();
        return new DataIO.DataInputByteBuffer(ByteBuffer.wrap(b),0);
    }


    @Test public void ln_serialization() throws IOException {
        HTreeMap.LinkedNode n = new HTreeMap.LinkedNode(123456, 1111L, 123L, 456L);

        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        HTreeMap m = db.hashMapCreate("test").make();

        m.LN_SERIALIZER.serialize(out, n);

        DataIO.DataInputByteBuffer in = swap(out);

        HTreeMap.LinkedNode n2  = (HTreeMap.LinkedNode) m.LN_SERIALIZER.deserialize(in, -1);

        assertEquals(123456, n2.next);
        assertEquals(0L, n2.expireLinkNodeRecid);
        assertEquals(123L,n2.key);
        assertEquals(456L, n2.value);
    }

    @Test public void test_simple_put(){

        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC, Serializer.BASIC,0,0,0,0,0,0,null,null,null, null, 0L,false, null);

        m.put(111L, 222L);
        m.put(333L, 444L);
        assertTrue(m.containsKey(111L));
        assertTrue(!m.containsKey(222L));
        assertTrue(m.containsKey(333L));
        assertTrue(!m.containsKey(444L));

        assertEquals(222L, m.get(111L));
        assertEquals(null, m.get(222L));
        assertEquals(444l, m.get(333L));
    }

    @Test public void test_hash_collision(){
        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC, Serializer.BASIC,0,0,0,0,0,0,null,null,null,null, 0L,false, null){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i<20;i++){
            m.put(i,i+100);
        }

        for(long i = 0;i<20;i++){
            assertTrue(m.containsKey(i));
            assertEquals(i+100, m.get(i));
        }

        m.put(11L, 1111L);
        assertEquals(1111L,m.get(11L) );
    }

    @Test public void test_hash_dir_expand(){
        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC, Serializer.BASIC,0,0,0,0,0,0,null,null,null,null, 0L,false, null){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i< HTreeMap.BUCKET_OVERFLOW;i++){
            m.put(i,i);
        }

        //segment should not be expanded
        int[] l = (int[]) engine.get(m.segmentRecids[0], HTreeMap.DIR_SERIALIZER);
        assertEquals(4+1, l.length);
        long recid = l[4];
        assertEquals(1, recid&1);  //last bite indicates leaf
        assertEquals(1,l[0]);
        //all others should be null
        for(int i=1;i<4;i++)
            assertEquals(0,l[i]);

        recid = recid>>>1;

        for(long i = HTreeMap.BUCKET_OVERFLOW -1; i>=0; i--){
            assertTrue(recid!=0);
            HTreeMap.LinkedNode  n = (HTreeMap.LinkedNode) engine.get(recid, m.LN_SERIALIZER);
            assertEquals(i, n.key);
            assertEquals(i, n.value);
            recid = n.next;
        }

        //adding one more item should trigger dir expansion to next level
        m.put((long) HTreeMap.BUCKET_OVERFLOW, (long) HTreeMap.BUCKET_OVERFLOW);

        recid = m.segmentRecids[0];

        l = (int[]) engine.get(recid, HTreeMap.DIR_SERIALIZER);
        assertEquals(4+1, l.length);
        recid = l[4];
        assertEquals(0, recid&1);  //last bite indicates leaf
        assertEquals(1,l[0]);

        //all others should be null
        for(int i=1;i<4;i++)
            assertEquals(0,l[i]);

        recid = recid>>>1;

        l = (int[]) engine.get(recid, HTreeMap.DIR_SERIALIZER);

        assertEquals(4+1, l.length);
        recid = l[4];
        assertEquals(1, recid&1);  //last bite indicates leaf
        assertEquals(1,l[0]);

        //all others should be null
        for(int i=1;i<4;i++)
            assertEquals(0,l[i]);

        recid = recid>>>1;


        for(long i = 0; i<= HTreeMap.BUCKET_OVERFLOW; i++){
            assertTrue(recid!=0);
            HTreeMap.LinkedNode n = (HTreeMap.LinkedNode) engine.get(recid, m.LN_SERIALIZER);

            assertNotNull(n);
            assertEquals(i, n.key);
            assertEquals(i, n.value);
            recid = n.next;
        }

    }


    @Test public void test_delete(){
        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC, Serializer.BASIC,0,0,0,0,0,0,null,null,null,null,0L, false,null){
            @Override
            protected int hash(Object key) {
                return 0;
            }
        };

        for(long i = 0;i<20;i++){
            m.put(i,i+100);
        }

        for(long i = 0;i<20;i++){
            assertTrue(m.containsKey(i));
            assertEquals(i+100, m.get(i));
        }


        for(long i = 0;i<20;i++){
            m.remove(i);
        }

        for(long i = 0;i<20;i++){
            assertTrue(!m.containsKey(i));
            assertEquals(null, m.get(i));
        }
    }

    @Test public void clear(){
        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC, Serializer.BASIC,0,0,0,0,0,0,null,null,null,null, 0L,false,null);
        for(Integer i=0;i<100;i++){
            m.put(i,i);
        }
        m.clear();
        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
    }

    @Test //(timeout = 10000)
    public void testIteration(){
        if(HTreeMap.SEG==1)
            return;

        Engine[] engines = HTreeMap.fillEngineArray(engine);
        HTreeMap m = new HTreeMap(engines,
                false, null,0,HTreeMap.preallocateSegments(engines),Serializer.BASIC,Serializer.BASIC,0,0,0,0,0,0,null,null,null,null,0L, false,null){
            @Override
            protected int hash(Object key) {
                return (Integer) key;
            }
        };

        final int max = 140;
        final int inc = 111111;

        for(Integer i=0;i<max;i++){
            m.put(i,i+inc);
        }

        Iterator<Integer> keys = m.keySet().iterator();
        for(Integer i=0;i<max;i++){
            assertTrue(keys.hasNext());
            assertEquals(i, keys.next());
        }
        assertTrue(!keys.hasNext());

        Iterator<Integer> vals = m.values().iterator();
        for(Integer i=inc;i<max+inc;i++){
            assertTrue(vals.hasNext());
            assertEquals(i, vals.next());
        }
        assertTrue(!vals.hasNext());


        //great it worked, test stuff spread across segments
        m.clear();
        assertTrue(m.isEmpty());

        for(int i = 0;i<max;i++){
            m.put((1<<30)+i, i+inc);
            m.put((2<<30)+i, i+inc);
            m.put((3<<30)+i, i+inc);
        }

        assertEquals(max*3,m.size());

        int countSegments = 0;
        for(long segmentRecid:m.segmentRecids){
            int[] segment = (int[]) engine.get(segmentRecid, HTreeMap.DIR_SERIALIZER);
            if(segment!=null && segment.length>4){
                countSegments++;
            }
        }

        assertEquals(3, countSegments);

        keys = m.keySet().iterator();
        for(int i=1;i<=3;i++){
            for(int j=0;j<max;j++){
                assertTrue(keys.hasNext());
                assertEquals(Integer.valueOf((i<<30)+j), keys.next());
            }
        }
        assertTrue(!keys.hasNext());

    }

    static final Long ZERO  = 0L;

    @Test public void expire_link_simple_add_remove(){
        HTreeMap m = db.hashMapCreate("test").expireMaxSize(100).make();
        m.segmentLocks[0].writeLock().lock();
        assertEquals(ZERO, engine.get(m.expireHeads[0], Serializer.LONG));
        assertEquals(ZERO, engine.get(m.expireTails[0], Serializer.LONG));

        m.expireLinkAdd(0,m.engines[0].put(HTreeMap.ExpireLinkNode.EMPTY, HTreeMap.ExpireLinkNode.SERIALIZER),  111L,222);

        Long recid = engine.get(m.expireHeads[0], Serializer.LONG);
        assertFalse(ZERO.equals(recid));
        assertEquals(recid, engine.get(m.expireTails[0], Serializer.LONG));

        HTreeMap.ExpireLinkNode n = engine.get(recid, HTreeMap.ExpireLinkNode.SERIALIZER);
        assertEquals(0, n.prev);
        assertEquals(0, n.next);
        assertEquals(111L, n.keyRecid);
        assertEquals(222, n.hash);

        assertTrue(Arrays.equals(new int[]{222},getExpireList(m,0)));

        n = m.expireLinkRemoveLast(0);
        assertEquals(0, n.prev);
        assertEquals(0, n.next);
        assertEquals(111L, n.keyRecid);
        assertEquals(222L, n.hash);

        assertEquals(ZERO, engine.get(m.expireHeads[0], Serializer.LONG));
        assertEquals(ZERO, engine.get(m.expireTails[0], Serializer.LONG));
        assertTrue(Arrays.equals(new int[]{},getExpireList(m,0)));
        m.segmentLocks[0].writeLock().unlock();
    }

    @Test public void expire_link_test(){
        final int s = HTreeMap.SEG==1?0:2;

        HTreeMap m = db.hashMapCreate("test").expireMaxSize(100).make();
        m.segmentLocks[s].writeLock().lock();

        long[] recids = new long[10];
        for(int i=1;i<10;i++){
            recids[i] = m.engines[0].put(HTreeMap.ExpireLinkNode.EMPTY, HTreeMap.ExpireLinkNode.SERIALIZER);
            m.expireLinkAdd(s, recids[i],i*10,i*100);
        }

        assertTrue(Arrays.equals(new int[]{100, 200, 300, 400, 500, 600, 700, 800, 900}, getExpireList(m, s)));

        m.expireLinkBump(s, recids[8], true);
        assertTrue(Arrays.equals(new int[]{100, 200, 300, 400, 500, 600, 700, 900, 800}, getExpireList(m, s)));

        m.expireLinkBump(s, recids[5], true);
        assertTrue(Arrays.equals(new int[]{100, 200, 300, 400, 600, 700, 900, 800, 500}, getExpireList(m, s)));

        m.expireLinkBump(s, recids[1], true);
        assertTrue(Arrays.equals(new int[]{200, 300, 400, 600, 700, 900, 800, 500, 100}, getExpireList(m, s)));

        assertEquals(200, m.expireLinkRemoveLast(s).hash);
        assertTrue(Arrays.equals(new int[]{300,400,600,700,900,800,500,100},getExpireList(m,s)));

        assertEquals(300, m.expireLinkRemoveLast(s).hash);
        assertTrue(Arrays.equals(new int[]{400,600,700,900,800,500,100},getExpireList(m,s)));

        assertEquals(600, m.expireLinkRemove(s, recids[6]).hash);
        assertTrue(Arrays.equals(new int[]{400, 700, 900, 800, 500, 100}, getExpireList(m, s)));

        assertEquals(400, m.expireLinkRemove(s,recids[4]).hash);
        assertTrue(Arrays.equals(new int[]{700,900,800,500,100},getExpireList(m,s)));

        assertEquals(100, m.expireLinkRemove(s,recids[1]).hash);
        assertTrue(Arrays.equals(new int[]{700,900,800,500},getExpireList(m,s)));
        m.segmentLocks[s].writeLock().unlock();

    }

    int[] getExpireList(HTreeMap m, int segment){
        int[] ret = new int[0];
        long recid = engine.get(m.expireTails[segment], Serializer.LONG);
        long prev = 0;
        //System.out.println("--");
        while(recid!=0){
            HTreeMap.ExpireLinkNode n = engine.get(recid, HTreeMap.ExpireLinkNode.SERIALIZER);
            //System.out.println(n.hash);
            assertEquals(prev,n.prev);
            prev = recid;
            recid = n.next;

            ret=Arrays.copyOf(ret,ret.length+1);
            ret[ret.length-1] = n.hash;

        }
        //System.out.println(Arrays.toString(ret));
        assertEquals(new Long(prev), engine.get(m.expireHeads[segment], Serializer.LONG));
        return ret;
    }



    @Test (timeout = 20000)
    public void expire_put() {
        HTreeMap m = db.hashMapCreate("test")
                .expireAfterWrite(100)
                .make();
        m.put("aa","bb");
        //should be removed in a moment or timeout
        while(m.get("aa")!=null){
        }
    }

    @Test(timeout = 20000)
    public void expire_max_size() throws InterruptedException {
        HTreeMap m = db.hashMapCreate("test")
                .expireMaxSize(1000)
                .make();
        for(int i=0;i<1100;i++){
            m.put(""+i,i);
        }
        //first should be removed soon
        while(m.size()>1050){
            m.get("aa"); //so internal tasks have change to run
            Thread.sleep(1);
        }

        Thread.sleep(500);
        m.get("aa"); //so internal tasks have change to run
        long size = m.size();
        assertTrue("" + size, size > 900 && size <= 1050);
    }


    @Test public void testSingleIter(){
        Map m = DBMaker.tempHashMap();
        m.put("aa","bb");

        Iterator iter = m.keySet().iterator();
        assertTrue(iter.hasNext());
        assertEquals("aa",iter.next());
        assertFalse(iter.hasNext());
    }

    @Test public void testMinMaxExpiryTime(){
        HTreeMap m = db.hashMapCreate("test")
                .expireAfterWrite(10000)
                .expireAfterAccess(100000)
                .make();
        long t = System.currentTimeMillis();
        assertEquals(0L, m.getMaxExpireTime());
        assertEquals(0L, m.getMinExpireTime());
        m.put("11","11");
        m.put("12","12");
        assertTrue(Math.abs(m.getMaxExpireTime()-t-10000)<300);
        assertTrue(Math.abs(m.getMinExpireTime()-t-10000)<300);

        m.get("11");
        assertTrue(Math.abs(m.getMaxExpireTime()-t-100000)<300);
        assertTrue(Math.abs(m.getMinExpireTime()-t-10000)<300);
        m.remove("11");
        m.remove("12");
        assertEquals(0L, m.getMaxExpireTime());
        assertEquals(0L, m.getMinExpireTime());
    }

    @Test (timeout = 20000)
    public void cache_load_time_expire(){
        if(TT.scale()==0)
            return;

        DB db =
                DBMaker.memoryDB()
                        .transactionDisable()
                        .make();

        HTreeMap m = db.hashMapCreate("test")
                //.expireMaxSize(11000000)
                .expireAfterWrite(100)
                .make();
        long time = System.currentTimeMillis();
        long counter = 0;
        while(time+5000>System.currentTimeMillis()){
            m.put(counter++,counter++);
        }
        m.clear();
    }

    @Test(timeout = 20000)
    public void cache_load_size_expire(){
        if(TT.scale()==0)
            return;

        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();

        HTreeMap m = db.hashMapCreate("test")
                //.expireMaxSize(11000000)
                .expireMaxSize(10000)
                .make();
        long time = System.currentTimeMillis();
        long counter = 0;
        while(time+5000>System.currentTimeMillis()){
            m.put(counter++,counter++);
//            if(counter%1000<2) System.out.println(m.size());
        }
        m.clear();
    }

    @Test public void divMod8(){
        for(int i= 0;i<1000000;i++){
            assertEquals(i/8,i>>HTreeMap.DIV8);
            assertEquals(i%8,i&HTreeMap.MOD8);
        }
    }


    @Test public void hasher(){
        HTreeMap m =
                DBMaker.memoryDB().transactionDisable().make()
                        .hashMapCreate("test")
                        .keySerializer(Serializer.INT_ARRAY)
                        .make();

        for(int i=0;i<1e5;i++){
            m.put(new int[]{i,i,i},i);
        }
        for(Integer i=0;i<1e5;i++){
            assertEquals(i,m.get(new int[]{i,i,i}));
        }

    }

    @Test public void mod_listener_lock(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        final HTreeMap m = db.hashMap("name");

        final int seg =  m.hash("aa")>>>28;
        final AtomicInteger counter = new AtomicInteger();

        m.modificationListenerAdd(new Bind.MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                for (int i = 0; i < m.segmentLocks.length; i++) {
                    assertEquals(seg == i, m.segmentLocks[i].isWriteLockedByCurrentThread());
                }
                counter.incrementAndGet();
            }
        });


        m.put("aa","aa");
        m.put("aa", "bb");
        m.remove("aa");


        m.put("aa","aa");
        m.remove("aa","aa");
        m.putIfAbsent("aa","bb");
        m.replace("aa","bb","cc");
        m.replace("aa","cc");

        assertEquals(8, counter.get());
    }

    @Test
    public void test_iterate_and_remove(){
        final long max= (long) 1e5;

        Set m = DBMaker.memoryDB().transactionDisable().make().hashSet("test");

        for(long i=0;i<max;i++){
            m.add(i);
        }


        Set control = new HashSet();
        Iterator iter = m.iterator();

        for(long i=0;i<max/2; i++){
            assertTrue(iter.hasNext());
            control.add(iter.next());
        }

        m.clear();

        while(iter.hasNext()){
            control.add(iter.next());
        }

    }

    /*
        Hi jan,

        Today i found another problem.

        my code is

        HTreeMap<Object, Object>  map = db.createHashMap("cache").expireMaxSize(MAX_ITEM_SIZE).counterEnable()
                .expireAfterWrite(EXPIRE_TIME, TimeUnit.SECONDS).expireStoreSize(MAX_GB_SIZE).make();

        i set EXPIRE_TIME = 216000

        but the data was expired right now,the expire time is not 216000s, it seems there is a bug for expireAfterWrite.

        if i call expireAfterAccess ,everything seems ok.

    */
    @Test (timeout=100000)
    public void expireAfterWrite() throws InterruptedException {
        if(TT.scale()==0)
            return;
        //NOTE this test has race condition and may fail under heavy load.
        //TODO increase timeout and move into integration tests.

        DB db = DBMaker.memoryDB().transactionDisable().make();

        int MAX_ITEM_SIZE = (int) 1e7;
        int EXPIRE_TIME = 3;
        double MAX_GB_SIZE = 1e7;

        Map m = db.hashMapCreate("cache").expireMaxSize(MAX_ITEM_SIZE).counterEnable()
                .expireAfterWrite(EXPIRE_TIME, TimeUnit.SECONDS).expireStoreSize(MAX_GB_SIZE).make();

        for(int i=0;i<1000;i++){
            m.put(i,i);
        }
        Thread.sleep(2000);

        for(int i=0;i<500;i++){
            m.put(i,i+1);
        }
        //wait until size is 1000
        while(m.size()!=1000){
            m.get("aa"); //so internal tasks have change to run
            Thread.sleep(10);
        }

        Thread.sleep(2000);

        //wait until size is 1000
        while(m.size()!=500){
            m.get("aa"); //so internal tasks have change to run
            Thread.sleep(10);
        }
    }


    public static class AA implements Serializable{
        final int val;

        public AA(int val) {
            this.val = val;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AA && ((AA)obj).val == val;
        }
    }


    @Test(expected = IllegalArgumentException.class)
    public void inconsistentHash(){
        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();

        HTreeMap m = db.hashMapCreate("test")

                .make();

        for(int i=0;i<1e5;i++){
            m.put(new AA(i), i);
        }
    }

    @Test
    public void test()
    {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Map<String, Integer> map = db.hashMap("map", null, null, new Fun.Function1<Integer, String>() {
            @Override
            public Integer run(String s) {
                return Integer.MIN_VALUE;
            }
        });
        Integer v1 = map.get("s1");
        assertEquals(Integer.valueOf(Integer.MIN_VALUE), v1);
    }

    @Test public void pump(){
        int max = 100+ TT.scale()*1000000;

        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set<Long> s = new HashSet();

        for(long i=0;i<max;i++){
            s.add(i);
        }

        HTreeMap<Long,Long> m = db.hashMapCreate("a")
                .pumpSource(s.iterator(), new Fun.Function1<Long,Long>() {
                    @Override
                    public Long run(Long l) {
                        return l*l;
                    }
                })
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG)
                .make();

        assertEquals(s.size(),m.size());
        assertTrue(m.keySet().containsAll(s));

        for(Long o:s){
            assertEquals((Long)(o*o),m.get(o));
        }

    }

    @Test public void pump_duplicates(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        List<Long> s = new ArrayList();
        int max = (int) (TT.scale()*1e6);
        for(long i=0;i<max;i++){
            s.add(i);
        }

        s.add(-1L);
        s.add(-1L);


        HTreeMap<Long,Long> m = db.hashMapCreate("a")
                .pumpSource(s.iterator(), new Fun.Function1<Long, Long>() {
                    @Override
                    public Long run(Long l) {
                        return l * l;
                    }
                })
                .pumpIgnoreDuplicates()
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG)

                .make();

        assertEquals(s.size()-1,m.size());
        assertTrue(m.keySet().containsAll(s));

        for(Long o:s){
            assertEquals((Long)(o*o),m.get(o));
        }

    }

    @Test(expected = IllegalArgumentException.class) //TODO better exception here
    public void pump_duplicates_fail(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        List<Long> s = new ArrayList();

        for(long i=0;i<1e6;i++){
            s.add(i);
        }

        s.add(-1L);
        s.add(-1L);


        HTreeMap<Long,Long> m = db.hashMapCreate("a")
                .pumpSource(s.iterator(), new Fun.Function1<Long,Long>() {
                    @Override
                    public Long run(Long l) {
                        return l*l;
                    }
                })
                .keySerializer(Serializer.LONG)
                .valueSerializer(Serializer.LONG)

                .make();

    }

    @Test public void pumpset(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        Set<Long> s = new HashSet();

        int max = 100+(int) (1e6* TT.scale());
        for(long i=0;i<max;i++){
            s.add(i);
        }

        Set<Long> m = db.hashSetCreate("a")
                .pumpSource(s.iterator())
                .serializer(Serializer.LONG)
                .make();

        assertEquals(s.size(), m.size());
        assertTrue(s.containsAll(m));

    }

    @Test public void pumpset_duplicates() {
        DB db = DBMaker.memoryDB().transactionDisable().make();
        List<Long> s = new ArrayList();
        int max = 100+(int) (1e6* TT.scale());
        for (long i = 0; i < max; i++) {
            s.add(i);
        }

        s.add(-1L);
        s.add(-1L);


        Set<Long> m = db.hashSetCreate("a")
                .pumpSource(s.iterator())
                .pumpIgnoreDuplicates()
                .serializer(Serializer.LONG)
                .make();

        assertEquals(s.size() - 1, m.size());
        assertTrue(m.containsAll(s));
    }

    @Test(expected = IllegalArgumentException.class) //TODO better exception here
    public void pumpset_duplicates_fail(){
        int max = 100+ TT.scale()*1000000;
        DB db = DBMaker.memoryDB().transactionDisable().make();
        List<Long> s = new ArrayList();

        for(long i=0;i<max;i++){
            s.add(i);
        }

        s.add(-1L);
        s.add(-1L);


        db.hashSetCreate("a")
                .pumpSource(s.iterator())
                .serializer(Serializer.LONG)
                .make();

    }


    @Test public void slot_to_offset_long(){
        Random r = new Random();
        for(int i=0;i<1000;i++){
            //fill array with random bites
            long[] l = new long[]{r.nextLong(), r.nextLong()};

            //turn bites into array pos
            List<Integer> b = new ArrayList();
            for(int j=0;j<l.length;j++){
                long v = l[j];
                for(int k=0;k<64;k++){
                    b.add((int)v&1);
                    v>>>=1;
                }
            }
            assertEquals(128,b.size());

            //iterate over an array, check if calculated pos equals

            int offset = 2;
            for(int slot=0;slot<128;slot++){
                int current = b.get(slot);

                int coffset = HTreeMap.dirOffsetFromSlot(l,slot);

                if(current==0)
                    coffset = -coffset;

                assertEquals(offset,coffset);
                offset+=current;
            }
        }
    }

    @Test public void slot_to_offset_int(){
        Random r = new Random();
        for(int i=0;i<1000;i++){
            //fill array with random bites
            int[] l = new int[]{r.nextInt(), r.nextInt(), r.nextInt(), r.nextInt()};

            //turn bites into array pos
            List<Integer> b = new ArrayList();
            for(int j=0;j<l.length;j++){
                long v = l[j];
                for(int k=0;k<32;k++){
                    b.add((int)v&1);
                    v>>>=1;
                }
            }
            assertEquals(128,b.size());

            //iterate over an array, check if calculated pos equals

            int offset = 4;
            for(int slot=0;slot<128;slot++){
                int current = b.get(slot);

                int coffset = HTreeMap.dirOffsetFromSlot(l,slot);

                if(current==0)
                    coffset = -coffset;

                assertEquals(offset,coffset);
                offset+=current;
            }
        }
    }

    @Test public void dir_put_long(){
        if(TT.scale()==0)
            return;

        for(int a=0;a<100;a++) {
            long[] reference = new long[127];
            Object dir = new int[4];
            Random r = new Random();
            for (int i = 0; i < 1e3; i++) {
                int slot = r.nextInt(127);
                long val = r.nextLong()&0xFFFFFFF;

                if (i % 3==0 && reference[slot]!=0){
                    //delete every 10th element
                    reference[slot] = 0;
                    dir = HTreeMap.dirRemove(dir, slot);
                }else{
                    reference[slot] = val;
                    dir = HTreeMap.dirPut(dir, slot, val);
                }

                //compare dir and reference
                long[] dir2 = new long[127];
                for (int j = 0; j < 127; j++) {
                    int offset = HTreeMap.dirOffsetFromSlot(dir, j);
                    if (offset > 0)
                        dir2[j] = HTreeMap.dirGet(dir, offset);
                }

                assertTrue(Arrays.equals(reference, dir2));

                if (dir instanceof int[])
                    assertTrue(Arrays.equals((int[]) dir, (int[]) TT.clone(dir, HTreeMap.DIR_SERIALIZER)));
                else
                    assertTrue(Arrays.equals((long[]) dir, (long[]) TT.clone(dir, HTreeMap.DIR_SERIALIZER)));
            }
        }
    }

    @Test public void dir_put_int(){
        if(TT.scale()==0)
            return;
        for(int a=0;a<100;a++) {
            long[] reference = new long[127];
            Object dir = new int[4];
            Random r = new Random();
            for (int i = 0; i < 1e3; i++) {
                int slot = r.nextInt(127);
                long val = r.nextInt((int) 1e6);

                if (i % 3==0 && reference[slot]!=0){
                    //delete every 10th element
                    reference[slot] = 0;
                    dir = HTreeMap.dirRemove(dir, slot);
                }else{
                    reference[slot] = val;
                    dir = HTreeMap.dirPut(dir, slot, val);
                }

                //compare dir and reference
                long[] dir2 = new long[127];
                for (int j = 0; j < 127; j++) {
                    int offset = HTreeMap.dirOffsetFromSlot(dir, j);
                    if (offset > 0)
                        dir2[j] = HTreeMap.dirGet(dir, offset);
                }

                assertTrue(Arrays.equals(reference, dir2));

                if (dir instanceof int[])
                    assertTrue(Arrays.equals((int[]) dir, (int[]) TT.clone(dir, HTreeMap.DIR_SERIALIZER)));
                else
                    assertTrue(Arrays.equals((long[]) dir, (long[]) TT.clone(dir, HTreeMap.DIR_SERIALIZER)));
            }
        }
    }


    @Test (timeout=20000L)
    public void expiration_notification() throws InterruptedException {
        if(TT.scale()==0)
            return;
        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();
        HTreeMap m = db
                .hashMapCreate("map")
                .expireAfterWrite(1000)
                .executorEnable()
                .make();

        final AtomicReference k = new AtomicReference();
        final AtomicReference oldval = new AtomicReference();
        final AtomicReference newval = new AtomicReference();

        m.put("one", "one2");

        //small chance of race condition, dont care
        m.modificationListenerAdd(new Bind.MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                k.set(key);
                oldval.set(oldVal);
                newval.set(newVal);
            }
        });

        while(k.get()==null){
            Thread.sleep(1);
        }

        assertEquals(0,m.size());

        assertEquals("one", k.get());
        assertEquals("one2",oldval.get());
        assertEquals(null, newval.get());
    }

    @Test (timeout=20000L)
    public void expiration_overflow() throws InterruptedException {
        if(TT.scale()==0)
            return;
        DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make();

        HTreeMap ondisk = db.hashMapCreate("onDisk")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.STRING)
                .make();

        HTreeMap inmemory = db
                .hashMapCreate("inmemory")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.STRING)
                .expireAfterWrite(1000)
                .expireOverflow(ondisk, true)
                .executorEnable()
                .executorPeriod(3000)
                .make();

        //fill on disk, inmemory should stay empty

        for(int i=0;i<1000;i++){
            ondisk.put(i,"aa"+i);
        }

        assertEquals(1000,ondisk.size());
        assertEquals(0, inmemory.size());

        //add stuff inmemory, ondisk should stay unchanged, until executor kicks in
        for(int i=1000;i<1100;i++){
            inmemory.put(i,"aa"+i);
        }
        assertEquals(1000, ondisk.size());
        assertEquals(100, inmemory.size());

        //wait until executor kicks in
        while(!inmemory.isEmpty()){
            Thread.sleep(100);
        }

        //stuff should be moved to indisk
        assertEquals(1100,ondisk.size());
        assertEquals(0, inmemory.size());

        //if value is not found in-memory it should get value from on-disk
        assertEquals("aa111",inmemory.get(111));
        assertEquals(1, inmemory.size());
    }

    @Test public void issue538_overflow_NPE1(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        HTreeMap m2 = db.hashMap("m2");
        HTreeMap m = db.hashMapCreate("m")
                .expireOverflow(m2,true)
                .make();

        assertNull(m.get("nonExistent"));
    }


    @Test public void issue538_overflow_NPE2(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        HTreeMap m2 = db.hashMap("m2");
        HTreeMap m = db.hashMapCreate("m")
                .expireOverflow(m2,true)
                .make();

        assertNull(m.get("nonExistent"));
    }

    @Test public void issue542_compaction_error_while_htreemap_used() throws IOException, ExecutionException, InterruptedException {
        long time = TT.scale() * 1000*60*5; //stress test 5 minutes
        if(time==0)
            return;
        final long endTime = System.currentTimeMillis()+time;

        File f = File.createTempFile("mapdbTest","mapdb");
        //TODO mutate to include other types of engines
        final DB db = DBMaker.fileDB(f).transactionDisable().deleteFilesAfterClose().make();

        //start background thread which will update HTreeMap
        Future<String> c = TT.fork(new Callable<String>() {
            @Override
            public String call() throws Exception {
                HTreeMap m = db.hashMapCreate("map")
                        .keySerializer(Serializer.INTEGER)
                        .valueSerializer(Serializer.BYTE_ARRAY)
                        .make();

                Random r = new Random();
                while (System.currentTimeMillis() < endTime) {
                    Integer key = r.nextInt(10000);
                    byte[] val = new byte[r.nextInt(10000)];
                    r.nextBytes(val);
                    m.put(key, val);
                }

                return "";
            }
        });

        while(System.currentTimeMillis()<endTime && !c.isDone()){
            db.compact();
        }

        c.get();
        db.close();
    }

    @Test public void setLong(){
        HTreeMap.KeySet k = (HTreeMap.KeySet) DBMaker.heapDB().transactionDisable().make().hashSet("test");
        k.add(11);
        assertEquals(1, k.sizeLong());
    }


    @Test public void serialize_clone() throws IOException, ClassNotFoundException {
        Map m = DBMaker.memoryDB().transactionDisable().make().hashMap("map");
        for(int i=0;i<1000;i++){
            m.put(i,i*10);
        }

        Map m2 = TT.cloneJavaSerialization(m);
        assertEquals(ConcurrentHashMap.class, m2.getClass());
        assertTrue(m2.entrySet().containsAll(m.entrySet()));
        assertTrue(m.entrySet().containsAll(m2.entrySet()));
    }


    @Test public void serialize_set_clone() throws IOException, ClassNotFoundException {
        Set m = DBMaker.memoryDB().transactionDisable().make().hashSet("map");
        for(int i=0;i<1000;i++){
            m.add(i);
        }

        Set m2 = TT.cloneJavaSerialization(m);
        assertFalse(HTreeMap.KeySet.class.equals(m2.getClass()));
        assertTrue(m2.containsAll(m));
        assertTrue(m.containsAll(m2));
    }


    @Test public void valueCreator(){
        Map<Integer,Integer> m = DBMaker.memoryDB().transactionDisable().make().hashMapCreate("map")
                .valueCreator(new Fun.Function1<Integer, Integer>() {
                    @Override
                    public Integer run(Integer integer) {
                        return integer * 100;
                    }
                }).make();

        m.put(1,1);
        m.put(2,2);
        m.put(3, 3);

        assertEquals(new Integer(1), m.get(1));
        assertEquals(new Integer(500), m.get(5));
    }

    @Test public void valueCreator_not_executed(){
        final AtomicLong c = new AtomicLong();

        Map<Integer,Integer> m = DBMaker.memoryDB().transactionDisable().make().hashMapCreate("map")
                .valueCreator(new Fun.Function1<Integer, Integer>() {
                    @Override
                    public Integer run(Integer integer) {
                        c.incrementAndGet();
                        return integer*100;
                    }
                }).make();

        m.put(1,1);
        m.put(2,2);
        m.put(3,3);

        assertEquals(0, c.get());
        assertEquals(new Integer(1), m.get(1));
        assertEquals(0, c.get());
        assertEquals(new Integer(500), m.get(5));
        assertEquals(1,c.get());
    }
}


