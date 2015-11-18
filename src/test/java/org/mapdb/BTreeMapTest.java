package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeMapTest{

    StoreDirect engine;


    BTreeMap m;

    boolean valsOutside = false;

    @Before public void init(){
        engine = new StoreDirect(null);
        engine.init();
        m = new BTreeMap(engine,false,
                BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,Serializer.BASIC,valsOutside,0),
                6,valsOutside,0, BTreeKeySerializer.BASIC,Serializer.BASIC,
                0);
    }

    @After
    public void close(){
        engine.close();
    }


    public static class Outside extends BTreeMapTest{
        {
            valsOutside=true;
        }
    }

    @Test public void test_leaf_node_serialization() throws IOException {

        if(valsOutside)
            return;

        BTreeMap.LeafNode n = new BTreeMap.LeafNode(
                new Object[]{1,2,3},
                true,true,false,
                new Object[]{1,2,3}, 0);
        BTreeMap.LeafNode n2 = (BTreeMap.LeafNode) TT.clone(n, m.nodeSerializer);
        assertTrue(Arrays.equals(nodeKeysToArray(n), nodeKeysToArray(n2)));
        assertEquals(n.next, n2.next);
    }


    int[] mkchild(int... args){
        return args;
    }
    
	@Test public void test_dir_node_serialization() throws IOException {


        BTreeMap.DirNode n = new BTreeMap.DirNode(
                new Object[]{1,2,3},
                false,true,false,
                mkchild(4,5,6,0));
        BTreeMap.DirNode n2 = (BTreeMap.DirNode) TT.clone(n, m.nodeSerializer);

        assertTrue(Arrays.equals(nodeKeysToArray(n), nodeKeysToArray(n2)));
        assertTrue(Arrays.equals((int[])n.child, (int[])n2.child));
    }

    @Test public void test_find_children(){
        int[] child = new int[8];
        for(int i=0;i<child.length;i++){
            child[i] = new Random().nextInt(1000)+1;
        }

        BTreeMap.BNode n1 = new BTreeMap.DirNode(new Integer[]{1,2,3,4,5,6,7,8},false,false,false,mkchild(child));
        assertEquals(8, BTreeKeySerializer.BASIC.findChildren(n1, 11));
        assertEquals(0,BTreeKeySerializer.BASIC.findChildren(n1, 1));
        assertEquals(0,BTreeKeySerializer.BASIC.findChildren(n1, 0));
        assertEquals(7,BTreeKeySerializer.BASIC.findChildren(n1, 8));

        child = new int[5];
        for(int i=0;i<child.length;i++){
            child[i] = new Random().nextInt(1000)+1;
        }

        BTreeMap.BNode n2 = new BTreeMap.DirNode(new Integer[]{10,20,30,40,50},false,false,false,mkchild(child));
        assertEquals(4,BTreeKeySerializer.BASIC.findChildren(n2, 49));
        assertEquals(4,BTreeKeySerializer.BASIC.findChildren(n2, 50));
        assertEquals(3, BTreeKeySerializer.BASIC.findChildren(n2, 40));
        assertEquals(3, BTreeKeySerializer.BASIC.findChildren(n2, 39));
    }


    @Test public void test_find_children_2(){
        for(boolean left:new boolean[]{true,false}){
            for(boolean right:new boolean[]{true,false}){
                List  keys = new ArrayList();
                for(int i=0;i<100;i+=10){
                    keys.add(i);
                }

                int[] child = new int[keys.size()+(right?1:0)+(left?1:0)];
                Arrays.fill(child,11);
                if(right)
                    child[child.length-1]=0;


                BTreeMap.BNode n = new BTreeMap.DirNode(keys.toArray(), left,right,false,mkchild(child));

                for(int i=-10;i<110;i++){
                    int pos = BTreeKeySerializer.BASIC.findChildren(n,i);
                    int expected = (i+(left?19:9))/10;
                    expected = Math.max(left?1:0,expected);
                    expected = Math.min(left?11:10,expected);
                    assertEquals("i:"+i+" - l:"+left+" - r:"+right,expected,pos);
                }
            }
        }


    }


    @Test public void test_next_dir(){
        BTreeMap.DirNode d = new BTreeMap.DirNode(new Integer[]{44,62,68, 71},false,false,false,mkchild(10,20,30,40));

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
                new Object[]{62,68, 71},
                true,false,false,
                mkchild(10,20,30,40));
        assertEquals(10, m.nextDir(d, 33));
        assertEquals(10, m.nextDir(d, 62));
        assertEquals(20, m.nextDir(d, 63));

        d = new BTreeMap.DirNode(
                new Object[]{44,62,68},
                false,true,false,
                mkchild(10,20,30,0));

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

        if(valsOutside)
            return;

        BTreeMap.LeafNode l = new BTreeMap.LeafNode(
                new Object[]{10,20,30},
                true,true,false,
                new Object[]{10,20,30},
                0);
        long rootRecid = engine.put(l, m.nodeSerializer);
        engine.update(m.rootRecidRef, rootRecid, Serializer.RECID);

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

    Object[] nodeKeysToArray(BTreeMap.BNode n){
        Object[] ret = new Object[n.keysLen(BTreeKeySerializer.BASIC)];
        for(int i=0;i<ret.length;i++){
            ret[i] = n.key(BTreeKeySerializer.BASIC,i);
        }
        return ret;
    }

    @Test public void root_leaf_insert(){
        if(valsOutside)
            return;

        m.put(11,12);
        final long rootRecid = engine.get(m.rootRecidRef, Serializer.RECID);
        BTreeMap.LeafNode n = (BTreeMap.LeafNode) engine.get(rootRecid, m.nodeSerializer);
        assertTrue(Arrays.equals(new Object[]{null, 11, null}, nodeKeysToArray(n)));
        assertTrue(Arrays.equals(new Object[]{12}, (Object[]) n.vals));
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
        assertEquals(0, m.size());
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
        int max = 50000* TT.scale();
        Map<Integer, String[]> map = DBMaker
                .memoryDB().transactionDisable()
                .make().treeMap("test");

        for (int i = 0; i < max; i++) {
            map.put(i, new String[5]);

        }


        for (int i = 0; i < max; i=i+1000) {
            assertTrue(Arrays.equals(new String[5], map.get(i)));
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
            assertEquals(s, e != null ? e.getKey() : null);
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

        DB db = DBMaker.memoryDB().transactionDisable().make();
        NavigableMap m = db.treeMap("name");
        try{
            m.lastKey();
            fail();
        }catch(NoSuchElementException e){}
        m.put("aa","aa");
        assertEquals("aa",m.lastKey());
        m.put("bb","bb");
        assertEquals("bb",m.lastKey());
        db.treeMap("name").clear();
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
        DB db = DBMaker.memoryDB().transactionDisable().make();
        final BTreeMap m = db.treeMap("name");

        final long rootRecid = db.getEngine().get(m.rootRecidRef, Serializer.RECID);
        final AtomicInteger counter = new AtomicInteger();

        m.modificationListenerAdd(new Bind.MapListener() {
            @Override
            public void update(Object key, Object oldVal, Object newVal) {
                assertTrue(m.nodeLocks.get(rootRecid) == Thread.currentThread());
                assertEquals(1, m.nodeLocks.size());
                counter.incrementAndGet();
            }
        });


        m.put("aa", "aa");
        m.put("aa", "bb");
        m.remove("aa");


        m.put("aa", "aa");
        m.remove("aa", "aa");
        m.putIfAbsent("aa", "bb");
        m.replace("aa", "bb", "cc");
        m.replace("aa", "cc");

        assertEquals(8, counter.get());
    }


    @Test public void concurrent_last_key(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        final BTreeMap m = db.treeMap("name");

        //fill
        final int c = 1000000* TT.scale();
        for(int i=0;i<=c;i++){
            m.put(i,i);
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
        DB db = DBMaker.memoryDB().transactionDisable().make();
        final BTreeMap m = db.treeMap("name");

        //fill
        final int c = 1000000* TT.scale();
        for(int i=0;i<=c;i++){
            m.put(i,i);
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

        /* Creates connections to MapDB */
        DB db1 = DBMaker.memoryDB().transactionDisable().make();


        /* Creates maps */
        ConcurrentNavigableMap<Integer, Integer> map1 = db1.treeMap("column1");

        /* Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /* Inserts some values in maps */
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

        /* Creates connections to MapDB */
        DB db1 = DBMaker.memoryDB().transactionDisable().make();


        /* Creates maps */
        NavigableSet<Integer> map1 = db1.treeSet("column1");

        /* Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.add(i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.last());

        map1.clear();

        /* Inserts some values in maps */
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

        /* Creates connections to MapDB */
        DB db1 = DBMaker.memoryDB().transactionDisable().make();


        /* Creates maps */
        ConcurrentNavigableMap<Integer, Integer> map1 = db1.treeMap("column1");

        /* Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.put(i, i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.lastKey());

        map1.clear();

        /* Inserts some values in maps */
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

        /* Creates connections to MapDB */
        DB db1 = DBMaker.memoryDB().transactionDisable().make();


        /* Creates maps */
        NavigableSet<Integer> map1 = db1.treeSet("column1");

        /* Inserts initial values in maps */
        for (int i = 0; i < numberOfRecords; i++) {
            map1.add(i);
        }


        assertEquals((Object) (numberOfRecords - 1), map1.last());

        map1.clear();

        /* Inserts some values in maps */
        for (int i = 100; i < 110; i++) {
            map1.add(i);
        }

        assertEquals(10,map1.size());
        assertFalse(map1.isEmpty());
        assertEquals((Object) 109, map1.last());
        assertEquals((Object) 100, map1.first());
    }

    @Test public void randomStructuralCheck(){
        Random r = new Random();
        BTreeMap map = DBMaker.memoryDB().transactionDisable().make().treeMapCreate("aa")
                .keySerializer(BTreeKeySerializer.INTEGER)
                .valueSerializer(Serializer.INTEGER)
                .make();

        int max =100000* TT.scale();

        for(int i=0;i<max*10;i++){
            map.put(r.nextInt(max),r.nextInt());
        }

        map.checkStructure();
    }



    @Test
    public void large_node_size(){
        if(TT.shortTest())
            return;
        for(int i :new int[]{10,200,6000}){

            int max = i*100;
            File f = TT.tempDbFile();
            DB db = DBMaker.fileDB(f)
                    .transactionDisable()
                    .fileMmapEnableIfSupported()
                    .make();
            Map m = db
                    .treeMapCreate("map")
                    .nodeSize(i)
                    .keySerializer(BTreeKeySerializer.INTEGER)
                    .valueSerializer(Serializer.INTEGER)
                    .make();

            for(int j=0;j<max;j++){
                m.put(j,j);
            }

            db.close();
            db = DBMaker.fileDB(f)
                    .deleteFilesAfterClose()
                    .transactionDisable()
                    .fileMmapEnableIfSupported()
                    .make();
            m = db.treeMap("map");

            for(Integer j=0;j<max;j++){
                assertEquals(j, m.get(j));
            }
            db.close();
        }
    }


    @Test public void findSmallerNodeLeaf(){
        BTreeMap.LeafNode n = new BTreeMap.LeafNode(
            new Object[]{2,4,6,8,10},
            true,true,false,
            new Object[]{"two","four","six","eight","ten"},
            0
        );

        assertNull(m.findSmallerNodeRecur(n,1,true));
        assertNull(m.findSmallerNodeRecur(n,1,false));
        assertNull(m.findSmallerNodeRecur(n,2,false));
        assertEquals(
                new Fun.Pair(1, n),
                m.findSmallerNodeRecur(n, 2, true));

        assertEquals(
                new Fun.Pair(1,n),
                m.findSmallerNodeRecur(n,3,true));
        assertEquals(
                new Fun.Pair(1,n),
                m.findSmallerNodeRecur(n,3,false));


        assertEquals(
                new Fun.Pair(2,n),
                m.findSmallerNodeRecur(n,4,true));
        assertEquals(
                new Fun.Pair(1,n),
                m.findSmallerNodeRecur(n,3,false));

        assertEquals(
                new Fun.Pair(5,n),
                m.findSmallerNodeRecur(n,10,true));
        assertEquals(
                new Fun.Pair(4,n),
                m.findSmallerNodeRecur(n,10,false));


        assertEquals(
                new Fun.Pair(5, n),
                m.findSmallerNodeRecur(n,12,true));
        assertEquals(
                new Fun.Pair(5, n),
                m.findSmallerNodeRecur(n,12,false));
    }


    @Test public void issue403_store_grows_with_values_outside_nodes(){
        File f = TT.tempDbFile();
        DB db = DBMaker.fileDB(f)
                .closeOnJvmShutdown()
                .transactionDisable()
                .make();

        BTreeMap<Long,byte[]> id2entry = db.treeMapCreate("id2entry")
                .valueSerializer(Serializer.BYTE_ARRAY)
                .keySerializer(Serializer.LONG)
                .valuesOutsideNodesEnable()
                .make();

        Store store = Store.forDB(db);
        byte[] b = TT.randomByteArray(10000);
        id2entry.put(11L, b);
        long size = store.getCurrSize();
        for(int i=0;i<100;i++) {
            byte[] b2 = TT.randomByteArray(10000);
            assertArrayEquals(b, id2entry.put(11L, b2));
            b = b2;
        }
        assertEquals(size, store.getCurrSize());

        for(int i=0;i<100;i++) {
            byte[] b2 = TT.randomByteArray(10000);
            assertArrayEquals(b, id2entry.replace(11L, b2));
            b = b2;
        }
        assertEquals(size,store.getCurrSize());

        for(int i=0;i<100;i++) {
            byte[] b2 = TT.randomByteArray(10000);
            assertTrue(id2entry.replace(11L, b, b2));
            b = b2;
        }
        assertEquals(size,store.getCurrSize());


        db.close();
        f.delete();
    }

    @Test public void setLong(){
        BTreeMap.KeySet k = (BTreeMap.KeySet) DBMaker.heapDB().transactionDisable().make().treeSet("test");
        k.add(11);
        assertEquals(1,k.sizeLong());
    }


    @Test public void serialize_clone() throws IOException, ClassNotFoundException {
        BTreeMap m = DBMaker.memoryDB().transactionDisable().make().treeMap("map");
        for(int i=0;i<1000;i++){
            m.put(i,i*10);
        }

        Map m2 = TT.cloneJavaSerialization(m);
        assertEquals(ConcurrentSkipListMap.class, m2.getClass());
        assertTrue(m2.entrySet().containsAll(m.entrySet()));
        assertTrue(m.entrySet().containsAll(m2.entrySet()));
    }


    @Test public void serialize_set_clone() throws IOException, ClassNotFoundException {
        Set m = DBMaker.memoryDB().transactionDisable().make().treeSet("map");
        for(int i=0;i<1000;i++){
            m.add(i);
        }

        Set m2 = TT.cloneJavaSerialization(m);
        assertEquals(ConcurrentSkipListSet.class, m2.getClass());
        assertTrue(m2.containsAll(m));
        assertTrue(m.containsAll(m2));
    }

    @Test public void findChildren2_next_link(){
        Object[] keys = new Object[]{10,20,30,40,50};
        BTreeMap.LeafNode n = new BTreeMap.LeafNode(
                keys,false,false,false,keys,111L
        );

        assertEquals(0, BTreeKeySerializer.BASIC.findChildren2(n,10));
        assertEquals(-1, BTreeKeySerializer.BASIC.findChildren2(n,9));
        assertEquals(4, BTreeKeySerializer.BASIC.findChildren2(n,50));
        assertEquals(-6, BTreeKeySerializer.BASIC.findChildren2(n,51));
    }

}



