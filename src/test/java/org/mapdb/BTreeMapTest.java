package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeMapTest{

    Engine engine;


    BTreeMap m;

    @Before public void init(){
        engine = new StoreDirect(Volume.memoryFactory(false,0L,CC.VOLUME_CHUNK_SHIFT));
        m = new BTreeMap(engine,BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,Serializer.BASIC,BTreeMap.COMPARABLE_COMPARATOR,0),
                6,false,0, BTreeKeySerializer.BASIC,Serializer.BASIC,
                BTreeMap.COMPARABLE_COMPARATOR,0);;
    }
    

    @Test public void test_leaf_node_serialization() throws IOException {


        BTreeMap.LeafNode n = new BTreeMap.LeafNode(new Object[]{null,1,2,3, null}, new Object[]{1,2,3}, 111);
        BTreeMap.LeafNode n2 = (BTreeMap.LeafNode) UtilsTest.clone(n, m.nodeSerializer);
        assertArrayEquals(n.keys(), n2.keys());
        assertEquals(n.next, n2.next);
    }

    
	@Test public void test_dir_node_serialization() throws IOException {


        BTreeMap.DirNode n = new BTreeMap.DirNode(new Object[]{1,2,3, null}, new long[]{4,5,6,7});
        BTreeMap.DirNode n2 = (BTreeMap.DirNode) UtilsTest.clone(n, m.nodeSerializer);

        assertArrayEquals(n.keys(), n2.keys());
        assertArrayEquals(n.child, n2.child);
    }

    @Test public void test_find_children(){

        assertEquals(8,m.findChildren(11, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(1, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(0,m.findChildren(0, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(7,m.findChildren(8, new Integer[]{1,2,3,4,5,6,7,8}));
        assertEquals(4,m.findChildren(49, new Integer[]{10,20,30,40,50}));
        assertEquals(4,m.findChildren(50, new Integer[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(40, new Integer[]{10,20,30,40,50}));
        assertEquals(3,m.findChildren(39, new Integer[]{10,20,30,40,50}));
    }


    @Test public void test_next_dir(){

        BTreeMap.DirNode d = new BTreeMap.DirNode(new Integer[]{44,62,68, 71}, new long[]{10,20,30,40});

        assertEquals(10, m.nextDir(d, 62));
        assertEquals(10, m.nextDir(d, 44));
        assertEquals(10, m.nextDir(d, 48));

        assertEquals(20, m.nextDir(d, 63));
        assertEquals(20, m.nextDir(d, 64));
        assertEquals(20, m.nextDir(d, 68));

        assertEquals(30, m.nextDir(d, 69));
        assertEquals(30, m.nextDir(d, 70));
        assertEquals(30, m.nextDir(d, 71));

        assertEquals(40, m.nextDir(d, 72));
        assertEquals(40, m.nextDir(d, 73));
    }

    @Test public void test_next_dir_infinity(){

        BTreeMap.DirNode d = new BTreeMap.DirNode(
                new Object[]{null,62,68, 71},
                new long[]{10,20,30,40});
        assertEquals(10, m.nextDir(d, 33));
        assertEquals(10, m.nextDir(d, 62));
        assertEquals(20, m.nextDir(d, 63));

        d = new BTreeMap.DirNode(
                new Object[]{44,62,68, null},
                new long[]{10,20,30,40});

        assertEquals(10, m.nextDir(d, 62));
        assertEquals(10, m.nextDir(d, 44));
        assertEquals(10, m.nextDir(d, 48));

        assertEquals(20, m.nextDir(d, 63));
        assertEquals(20, m.nextDir(d, 64));
        assertEquals(20, m.nextDir(d, 68));

        assertEquals(30, m.nextDir(d, 69));
        assertEquals(30, m.nextDir(d, 70));
        assertEquals(30, m.nextDir(d, 71));

        assertEquals(30, m.nextDir(d, 72));
        assertEquals(30, m.nextDir(d, 73));

    }

    @Test public void simple_root_get(){

        BTreeMap.LeafNode l = new BTreeMap.LeafNode(
                new Object[]{null, 10,20,30, null},
                new Object[]{10,20,30},
                0);
        long rootRecid = engine.put(l, m.nodeSerializer);
        engine.update(m.rootRecidRef, rootRecid, Serializer.LONG);

        assertEquals(null, m.get(1));
        assertEquals(null, m.get(9));
        assertEquals(10, m.get(10));
        assertEquals(null, m.get(11));
        assertEquals(null, m.get(19));
        assertEquals(20, m.get(20));
        assertEquals(null, m.get(21));
        assertEquals(null, m.get(29));
        assertEquals(30, m.get(30));
        assertEquals(null, m.get(31));
    }

    @Test public void root_leaf_insert(){

        m.put(11,12);
        final long rootRecid = engine.get(m.rootRecidRef, Serializer.LONG);
        BTreeMap.LeafNode n = (BTreeMap.LeafNode) engine.get(rootRecid, m.nodeSerializer);
        assertArrayEquals(new Object[]{null, 11, null}, n.keys);
        assertArrayEquals(new Object[]{12}, n.vals);
        assertEquals(0, n.next);
    }

    @Test public void batch_insert(){


        for(int i=0;i<1000;i++){
            m.put(i*10,i*10+1);
        }


        for(int i=0;i<10000;i++){
            assertEquals(i%10==0?i+1:null, m.get(i));
        }
    }

    @Test public void test_empty_iterator(){

        assertFalse(m.keySet().iterator().hasNext());
        assertFalse(m.values().iterator().hasNext());
    }

    @Test public void test_key_iterator(){

        for(int i = 0;i<20;i++){
            m.put(i,i*10);
        }

        Iterator iter = m.keySet().iterator();

        for(int i = 0;i<20;i++){
            assertTrue(iter.hasNext());
            assertEquals(i,iter.next());
        }
        assertFalse(iter.hasNext());
    }

    @Test public void test_size(){

        assertTrue(m.isEmpty());
        assertEquals(0,m.size());
        for(int i = 1;i<30;i++){
            m.put(i,i);
            assertEquals(i,m.size());
            assertFalse(m.isEmpty());
        }
    }

    @Test public void delete(){

        for(int i:new int[]{
                10, 50, 20, 42,
                //44, 68, 20, 93, 85, 71, 62, 77, 4, 37, 66
        }){
            m.put(i,i);
        }
        assertEquals(10, m.remove(10));
        assertEquals(20, m.remove(20));
        assertEquals(42, m.remove(42));

        assertEquals(null, m.remove(42999));
    }

    @Test public void issue_38(){
        Map<Integer, String[]> map = DBMaker
                .newMemoryDB()
                .make().getTreeMap("test");

        for (int i = 0; i < 50000; i++) {
            map.put(i, new String[5]);

        }


        for (int i = 0; i < 50000; i=i+1000) {
            assertArrayEquals(new String[5], map.get(i));
            assertTrue(map.get(i).toString().contains("[Ljava.lang.String"));
        }


    }



    @Test public void floorTestFill() {

        m.put(1, "val1");
        m.put(2, "val2");
        m.put(5, "val3");

        assertEquals(5,m.floorKey(5));
        assertEquals(1,m.floorKey(1));
        assertEquals(2,m.floorKey(2));
        assertEquals(2,m.floorKey(3));
        assertEquals(2,m.floorKey(4));
        assertEquals(5,m.floorKey(5));
        assertEquals(5,m.floorKey(6));
    }

    @Test public void submapToString() {


        for (int i = 0; i < 20; i++) {
            m.put(i, "aa"+i);

        }

        Map submap = m.subMap(10, true, 13, true);
        assertEquals("{10=aa10, 11=aa11, 12=aa12, 13=aa13}",submap.toString());
    }

    @Test public void findSmaller(){


        for(int i=0;i<10000; i+=3){
            m.put(i, "aa"+i);
        }

        for(int i=0;i<10000; i+=1){
            Integer s = i - i%3;
            Map.Entry e = m.findSmaller(i,true);
            assertEquals(s,e!=null?e.getKey():null);
        }

        assertEquals(9999, m.findSmaller(100000,true).getKey());

        assertNull(m.findSmaller(0,false));
        for(int i=1;i<10000; i+=1){
            Integer s = i - i%3;
            if(s==i) s-=3;
            Map.Entry e = m.findSmaller(i,false);
            assertEquals(s,e!=null?e.getKey():null);
        }
        assertEquals(9999, m.findSmaller(100000,false).getKey());

    }

    @Test public void NoSuchElem_After_Clear(){
//      bug reported by :	Lazaros Tsochatzidis
//        But after clearing the tree using:
//
//        public void Delete() {
//            db.getTreeMap("Names").clear();
//            db.compact();
//        }
//
//        every next call of getLastKey() leads to the exception "NoSuchElement". Not
//        only the first one...

        DB db = DBMaker.newTempFileDB().transactionDisable().make();
        NavigableMap m = db.getTreeMap("name");
        try{
            m.lastKey();
            fail();
        }catch(NoSuchElementException e){}
        m.put("aa","aa");
        assertEquals("aa",m.lastKey());
        m.put("bb","bb");
        assertEquals("bb",m.lastKey());
        db.getTreeMap("name").clear();
        db.compact();
        try{
            Object key=m.lastKey();
            fail(key.toString());
        }catch(NoSuchElementException e){}
        m.put("aa","aa");
        assertEquals("aa",m.lastKey());
        m.put("bb","bb");
        assertEquals("bb",m.lastKey());
    }

    @Test public void mod_listener_lock(){
        DB db = DBMaker.newMemoryDB().make();
        final BTreeMap m = db.getTreeMap("name");

        final long rootRecid = db.getEngine().get(m.rootRecidRef, Serializer.LONG);
        final AtomicInteger counter = new AtomicInteger();

        m.addModificationListener(new Bind.MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                assertTrue(m.nodeLocks.get(rootRecid)==Thread.currentThread());
                assertEquals(1,m.nodeLocks.size());
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


    @Test public void concurrent_last_key(){
        DB db = DBMaker.newMemoryDB().make();
        final BTreeMap m = db.getTreeMap("name");

        //fill
        final int c = 1000000;
        for(int i=0;i<=c;i++){
            m.put(c,c);
        }

        Thread t = new Thread(){
            @Override
            public void run() {
                for(int i=c;i>=0;i--){
                    m.remove(i);
                }
            }
        };
        t.run();
        while(t.isAlive()){
            assertNotNull(m.lastKey());
        }
    }

    @Test public void concurrent_first_key(){
        DB db = DBMaker.newMemoryDB().make();
        final BTreeMap m = db.getTreeMap("name");

        //fill
        final int c = 1000000;
        for(int i=0;i<=c;i++){
            m.put(c,c);
        }

        Thread t = new Thread(){
            @Override
            public void run() {
                for(int i=0;i<=c;i++){
                    m.remove(c);
                }
            }
        };
        t.run();
        while(t.isAlive()){
            assertNotNull(m.firstKey());
        }
    }

    @Test public void WriteDBInt_lastKey() {
        int numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        ConcurrentNavigableMap<Integer, Integer> map1 = db1.getTreeMap("column1");

        /** Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /** Inserts some values in maps */
        for (int i = 0; i < 10; i++) {
            map1.put(i, i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 9, map1.lastKey());
        assertEquals((Object) 9, map1.lastEntry().getValue());
        assertEquals((Object) 0, map1.firstKey());
        assertEquals((Object) 0, map1.firstEntry().getValue());
    }

    @Test public void WriteDBInt_lastKey_set() {
        int numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        NavigableSet<Integer> map1 = db1.getTreeSet("column1");

        /** Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.add(i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.last());

        map1.clear();

        /** Inserts some values in maps */
        for (int i = 0; i < 10; i++) {
            map1.add(i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 9, map1.last());
        assertEquals((Object) 0, map1.first());
    }

    @Test public void WriteDBInt_lastKey_middle() {
        int numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        ConcurrentNavigableMap<Integer, Integer> map1 = db1.getTreeMap("column1");

        /** Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /** Inserts some values in maps */
        for (int i = 100; i < 110; i++) {
            map1.put(i, i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 109, map1.lastKey());
        assertEquals((Object) 109, map1.lastEntry().getValue());
        assertEquals((Object) 100, map1.firstKey());
        assertEquals((Object) 100, map1.firstEntry().getValue());
    }

    @Test public void WriteDBInt_lastKey_set_middle() {
        int numberOfRecords = 1000;

        /** Creates connections to MapDB */
        DB db1 = DBMaker.newMemoryDB().make();


        /** Creates maps */
        NavigableSet<Integer> map1 = db1.getTreeSet("column1");

        /** Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.add(i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.last());

        map1.clear();

        /** Inserts some values in maps */
        for (int i = 100; i < 110; i++) {
            map1.add(i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 109, map1.last());
        assertEquals((Object) 100, map1.first());
    }

}



