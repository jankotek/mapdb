package org.mapdb;

import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeMapTest{

    Engine engine = new StoreDirect(Volume.memoryFactory(false));
    

    @Test public void test_leaf_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(engine, 32,true,false,false, null,null,null,null);

        BTreeMap.LeafNode n = new BTreeMap.LeafNode(new Object[]{1,2,3, null}, new Object[]{1,2,3,null}, 111);
        BTreeMap.LeafNode n2 = (BTreeMap.LeafNode) Utils.clone(n, m.nodeSerializer);
        assertArrayEquals(n.keys(), n2.keys());
        assertEquals(n.next, n2.next);
    }

    
	@Test public void test_dir_node_serialization() throws IOException {
        BTreeMap m = new BTreeMap(engine,32,true,false,false, null,null,null,null);

        BTreeMap.DirNode n = new BTreeMap.DirNode(new Object[]{1,2,3, null}, new long[]{4,5,6,7});
        BTreeMap.DirNode n2 = (BTreeMap.DirNode) Utils.clone(n, m.nodeSerializer);

        assertArrayEquals(n.keys(), n2.keys());
        assertArrayEquals(n.child, n2.child);
    }

    @Test public void test_find_children(){
        BTreeMap m = new BTreeMap(engine,32,true,false,false, null,null,null,null);
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
        BTreeMap m = new BTreeMap(engine,32,true,false,false, null,null,null,null);
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
        BTreeMap m = new BTreeMap(engine,32,true,false, false,null,null,null,null);
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
        BTreeMap m = new BTreeMap(engine,32,true,false,false, null,null,null,null);
        BTreeMap.LeafNode l = new BTreeMap.LeafNode(
                new Object[]{null, 10,20,30, null},
                new Object[]{null, 10,20,30, null},
                0);
        long rootRecid = engine.put(l, m.nodeSerializer);
        engine.update(m.rootRecidRef, rootRecid, Serializer.LONG_SERIALIZER);

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
        BTreeMap m = new BTreeMap(engine,6,true,false, false,null,null,null,null);
        m.put(11,12);
        final long rootRecid = engine.get(m.rootRecidRef, Serializer.LONG_SERIALIZER);
        BTreeMap.LeafNode n = (BTreeMap.LeafNode) engine.get(rootRecid, m.nodeSerializer);
        assertArrayEquals(new Object[]{null, 11, null}, n.keys);
        assertArrayEquals(new Object[]{null, 12, null}, n.vals);
        assertEquals(0, n.next);
    }

    @Test public void batch_insert(){
        BTreeMap m = new BTreeMap(engine,6,true,false,false, null,null,null,null);

        for(int i=0;i<1000;i++){
            m.put(i*10,i*10+1);
        }

        for(int i=0;i<10000;i++){
            assertEquals(i%10==0?i+1:null, m.get(i));
        }
    }

    @Test public void test_empty_iterator(){
        BTreeMap m = new BTreeMap(engine,6,true,false,false, null,null,null,null);
        assertFalse(m.keySet().iterator().hasNext());
        assertFalse(m.values().iterator().hasNext());
    }

    @Test public void test_key_iterator(){
        BTreeMap m = new BTreeMap(engine,6,true,false,false, null,null,null,null);
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
        BTreeMap m = new BTreeMap(engine,6,true,false,false, null,null,null,null);
        assertTrue(m.isEmpty());
        assertEquals(0,m.size());
        for(int i = 1;i<30;i++){
            m.put(i,i);
            assertEquals(i,m.size());
            assertFalse(m.isEmpty());
        }
    }

    @Test public void delete(){
        BTreeMap m = new BTreeMap(engine,6,true,false,false, null,null,null,null);
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
        BTreeMap map = new BTreeMap(engine,6,true,false, false,null,null,null,null);
        map.put(1, "val1");
        map.put(2, "val2");
        map.put(5, "val3");

        assertEquals(5,map.floorKey(5));
        assertEquals(1,map.floorKey(1));
        assertEquals(2,map.floorKey(2));
        assertEquals(2,map.floorKey(3));
        assertEquals(2,map.floorKey(4));
        assertEquals(5,map.floorKey(5));
        assertEquals(5,map.floorKey(6));
    }

    @Test public void submapToString() {
        BTreeMap map = new BTreeMap(engine,6,true,false,false, null,null,null,null);

        for (int i = 0; i < 20; i++) {
            map.put(i, "aa"+i);

        }

        Map submap = map.subMap(10, true, 13, true);
        assertEquals("{10=aa10, 11=aa11, 12=aa12, 13=aa13}",submap.toString());
    }

    @Test public void findSmaller(){
        BTreeMap map = new BTreeMap(engine,6,true,false,false, null,null,null,null);

        for(int i=0;i<10000; i+=3){
            map.put(i, "aa"+i);
        }

        for(int i=0;i<10000; i+=1){
            Integer s = i - i%3;
            Map.Entry e = map.findSmaller(i,true);
            assertEquals(s,e!=null?e.getKey():null);
        }

        assertEquals(9999, map.findSmaller(100000,true).getKey());

        assertNull(map.findSmaller(0,false));
        for(int i=1;i<10000; i+=1){
            Integer s = i - i%3;
            if(s==i) s-=3;
            Map.Entry e = map.findSmaller(i,false);
            assertEquals(s,e!=null?e.getKey():null);
        }
        assertEquals(9999, map.findSmaller(100000,false).getKey());

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

        DB db = DBMaker.newTempFileDB().writeAheadLogDisable().make();
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


}


