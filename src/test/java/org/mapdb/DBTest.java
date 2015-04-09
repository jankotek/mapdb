package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Queue;
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
        Map m1 = db.getHashMap("test");
        m1.put(1,2);
        m1.put(3,4);
        assertTrue(m1 == db.getHashMap("test"));
        assertEquals(m1, new DB(engine).getHashMap("test"));
    }



    @Test
    public void testGetHashSet() throws Exception {
        Set m1 = db.getHashSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.getHashSet("test"));
        assertEquals(m1, new DB(engine).getHashSet("test"));
    }

    @Test
    public void testGetTreeMap() throws Exception {
        Map m1 = db.getTreeMap("test");
        m1.put(1, 2);
        m1.put(3, 4);
        assertTrue(m1 == db.getTreeMap("test"));
        assertEquals(m1, new DB(engine).getTreeMap("test"));
    }

    @Test
    public void testGetTreeSet() throws Exception {
        Set m1 = db.getTreeSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.getTreeSet("test"));
        assertEquals(m1, new DB(engine).getTreeSet("test"));
    }

    @Test(expected = IllegalAccessError.class)
    public void testClose() throws Exception {
        db.close();
        db.getHashMap("test");
    }


    @Test public void getAll(){
        db.createAtomicString("aa","100");
        db.getHashMap("zz").put(11,"12");
        Map all = db.getAll();

        assertEquals(2,all.size());
        assertEquals("100", ((Atomic.String) all.get("aa")).get());
        assertEquals("12", ((HTreeMap) all.get("zz")).get(11));

    }

    @Test public void rename(){
        db.getHashMap("zz").put(11, "12");
        db.rename("zz", "aa");
        assertEquals("12", db.getHashMap("aa").get(11));
    }


    @Test(expected = IllegalArgumentException.class)
    public void testCollectionExists(){
        db.getHashMap("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueueExists(){
        db.getQueue("test");
        db.checkNameNotExists("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAtomicExists(){
        db.getAtomicInteger("test");
        db.checkNameNotExists("test");
    }

    @Test
    public void test_issue_315() {
        DB db = DBMaker.newMemoryDB().make();

        final String item1 = "ITEM_ONE";
        final String item2 = "ITEM_ONE_TWO";
        final String item3 = "ITEM_ONETWO";
        final String item4 = "ITEM_ONE__TWO";
        final String item5 = "ITEM_ONE.TWO";
        final String item6 = "ITEM_ONE.__.TWO";


        db.createTreeMap(item1).make();
        db.createTreeSet(item2).make();
        db.createTreeSet(item3).make();
        db.createTreeSet(item4).make();
        db.createTreeSet(item5).make();
        db.createTreeSet(item6).make();


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
        DB db = DBMaker.newFileDB(f).make();
        Map map = db.getTreeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.newFileDB(f).deleteFilesAfterClose().make();
        map = db.getTreeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void basic_reopen_notx(){
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).transactionDisable().make();
        Map map = db.getTreeMap("map");
        map.put("aa", "bb");

        db.commit();
        db.close();

        db = DBMaker.newFileDB(f).deleteFilesAfterClose().transactionDisable().make();
        map = db.getTreeMap("map");
        assertEquals(1, map.size());
        assertEquals("bb", map.get("aa"));
        db.close();
    }

    @Test public void hashmap_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.newMemoryDB().make();

        HTreeMap m = db.createHashMap("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void hashset_executor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.newMemoryDB().make();

        HTreeMap.KeySet m = (HTreeMap.KeySet) db.createHashSet("aa").executorPeriod(1111).executorEnable(s).make();
        assertTrue(s == m.getHTreeMap().executor);
        db.close();

        assertTrue(s.isTerminated());
    }

    @Test public void treemap_infer_key_serializer(){
        DB db = DBMaker.newMemoryDB().make();
        BTreeMap m = db.createTreeMap("test")
                .keySerializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, m.keySerializer);

        BTreeMap m2 = db.createTreeMap("test2")
                .keySerializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(m2.keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }


    @Test public void treeset_infer_key_serializer(){
        DB db = DBMaker.newMemoryDB().make();
        BTreeMap.KeySet m = (BTreeMap.KeySet) db.createTreeSet("test")
                .serializer(Serializer.LONG)
                .make();
        assertEquals(BTreeKeySerializer.LONG, ((BTreeMap)m.m).keySerializer);

        BTreeMap.KeySet m2 = (BTreeMap.KeySet) db.createTreeSet("test2")
                .serializer(Serializer.LONG)
                .comparator(Fun.REVERSE_COMPARATOR)
                .make();
        assertTrue(((BTreeMap)m2.m).keySerializer instanceof BTreeKeySerializer.BasicKeySerializer);
        assertEquals(m2.comparator(), Fun.REVERSE_COMPARATOR);
    }
}
