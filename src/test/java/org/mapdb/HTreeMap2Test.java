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


