package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;



@SuppressWarnings({ "unchecked", "rawtypes" })
public class DBTest {

    Store engine;
    DB db;


    @Before public void init(){
        engine = new StoreDirect(null);
        engine.init();
        db = new DB(engine);
    }


    @After
    public void close(){
        db = null;
    }

    @Test
    public void testGetHashMap() throws Exception {
        Map m1 = db.hashMap("test");
        m1.put(1,2);
        m1.put(3,4);
        assertTrue(m1 == db.hashMap("test"));
        assertEquals(m1, new DB(engine).hashMap("test"));
    }



    @Test
    public void testGetHashSet() throws Exception {
        Set m1 = db.hashSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.hashSet("test"));
        assertEquals(m1, new DB(engine).hashSet("test"));
    }

    @Test
    public void testGetTreeMap() throws Exception {
        Map m1 = db.treeMap("test");
        m1.put(1, 2);
        m1.put(3, 4);
        assertTrue(m1 == db.treeMap("test"));
        assertEquals(m1, new DB(engine).treeMap("test"));
    }

    @Test
    public void testGetTreeSet() throws Exception {
        Set m1 = db.treeSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.treeSet("test"));
        assertEquals(m1, new DB(engine).treeSet("test"));
    }

    @Test(expected = IllegalAccessError.class)
    public void testClose() throws Exception {
        db.close();
        db.hashMap("test");
    }


    @Test public void getAll(){
        db.atomicStringCreate("aa", "100");
        db.hashMap("zz").put(11,"12");
        Map all = db.getAll();

        assertEquals(2,all.size());
        assertEquals("100", ((Atomic.String) all.get("aa")).get());
        assertEquals("12", ((HTreeMap) all.get("zz")).get(11));

    }

    @Test public void rename(){
        db.hashMap("zz").put(11, "12");
        db.rename("zz", "aa");
        assertEquals("12", db.hashMap("aa").get(11));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCollectionExists(){
        db.hashMap("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueueExists(){
        db.getQueue("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAtomicExists(){
        db.atomicInteger("test");
        db.checkNameNotExists("test");
    }

    @Test
    public void test_issue_315() {
        DB db = DBMaker.memoryDB().make();

        final String item1 = "ITEM_ONE";
        final String item2 = "ITEM_ONE_TWO";
        final String item3 = "ITEM_ONETWO";
        final String item4 = "ITEM_ONE__TWO";
        final String item5 = "ITEM_ONE.TWO";
        final String item6 = "ITEM_ONE.__.TWO";


        db.treeMapCreate(item1).make();
        db.treeSetCreate(item2).make();
        db.treeSetCreate(item3).make();
        db.treeSetCreate(item4).make();
        db.treeSetCreate(item5).make();
        db.treeSetCreate(item6).make();


        db.delete(item1);

        assertTrue(db.get(item1) == null);
        assertTrue(db.get(item2) instanceof Set);
        assertTrue(db.get(item3) instanceof Set);
        assertTrue(db.get(item4) instanceof Set);
        assertTrue(db.get(item5) instanceof Set);
        assertTrue(db.get(item6) instanceof Set);

    }


    @Test public void basic_reopen(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f).make();
        Map map = db.treeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.fileDB(f).deleteFilesAfterClose().make();
        map = db.treeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void basic_reopen_notx(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f).transactionDisable().make();
        Map map = db.treeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.fileDB(f).deleteFilesAfterClose().transactionDisable().make();
        map = db.treeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void hashmap_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.memoryDB().make();

        HTreeMap m = db.hashMapCreate("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void hashset_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.memoryDB().make();

        HTreeMap.KeySet m = (HTreeMap.KeySet) db.hashSetCreate("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.getHTreeMap().executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void treemap_infer_key_serializer(){
        DB db = DBMaker.memoryDB().make();
        BTreeMap m = db.treeMapCreate("test")
                .keySerializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, m.keySerializer);

        BTreeMap m2 = db.treeMapCreate("test2")
                .keySerializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(m2.keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }


    @Test public void treeset_infer_key_serializer(){
        DB db = DBMaker.memoryDB().make();
        BTreeMap.KeySet m = (BTreeMap.KeySet) db.treeSetCreate("test")
                .serializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, ((BTreeMap)m.m).keySerializer);

        BTreeMap.KeySet m2 = (BTreeMap.KeySet) db.treeSetCreate("test2")
                .serializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(((BTreeMap)m2.m).keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }
}
