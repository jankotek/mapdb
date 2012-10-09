package net.kotek.jdbm;

import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;


@SuppressWarnings("unchecked")
public class DBTest {

    RecordManager recman = new StorageDirect(null,true,true,false);
    DB db = new DB(recman);

    @Test
    public void testGetHashMap() throws Exception {
        Map m1 = db.getHashMap("test");
        m1.put(1,2);
        m1.put(3,4);
        assertTrue(m1 == db.getHashMap("test"));
        assertEquals(m1, new DB(recman).getHashMap("test"));
    }



    @Test
    public void testGetHashSet() throws Exception {
        Set m1 = db.getHashSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.getHashSet("test"));
        assertEquals(m1, new DB(recman).getHashSet("test"));
    }

    @Test
    public void testGetTreeMap() throws Exception {
        Map m1 = db.getTreeMap("test");
        m1.put(1,2);
        m1.put(3,4);
        assertTrue(m1 == db.getTreeMap("test"));
        assertEquals(m1, new DB(recman).getTreeMap("test"));
    }

    @Test
    public void testGetTreeSet() throws Exception {
        Set m1 = db.getTreeSet("test");
        m1.add(1);
        m1.add(2);
        assertTrue(m1 == db.getTreeSet("test"));
        assertEquals(m1, new DB(recman).getTreeSet("test"));
    }

    @Test(expected = IllegalAccessError.class)
    public void testClose() throws Exception {
        db.close();
        db.getHashMap("test");
    }
}
