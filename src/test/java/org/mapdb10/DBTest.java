package org.mapdb10;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;



@SuppressWarnings({ "unchecked", "rawtypes" })
public class DBTest {

    Engine engine;
    DB db;


    @Before public void init(){
        engine = new StoreDirect(Volume.memoryFactory(false,0L,CC.VOLUME_CHUNK_SHIFT));
        db = new DB(engine);
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
        m1.put(1,2);
        m1.put(3,4);
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
        assertEquals("100",((Atomic.String)all.get("aa")).get());
        assertEquals("12",((HTreeMap)all.get("zz")).get(11));

    }

    @Test public void rename(){
        db.getHashMap("zz").put(11,"12");
        db.rename("zz","aa");
        assertEquals("12",db.getHashMap("aa").get(11));
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
        DB db = DBMaker.newMemoryDB().cacheDisable().make();

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

}
