package org.mapdb.issues;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

/*
 * https://github.com/jankotek/MapDB/issues/78
 *
 * @author Nandor Kracser
 */
public class Issue78Test {

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test(expected = DBException.ClassNotSerializable.class, timeout = 10000)
    public void testIssue() {
        DB db = DBMaker.memoryDB().make();
        HTreeMap<String, NotSerializable> usersMap = db.hashMap("values");
        usersMap.put("thisKillsTheAsyncWriteThread", new NotSerializable());
        db.commit();
        db.close();
    }

    @Test(expected = DBException.ClassNotSerializable.class, timeout = 10000)
    public void testIssueAsync() {
        DB db = DBMaker.memoryDB().asyncWriteEnable().make();
        HTreeMap<String, NotSerializable> usersMap = db.hashMap("values");
        usersMap.put("thisKillsTheAsyncWriteThread", new NotSerializable());
        db.commit();
        db.close();
    }


    class NotSerializable {
    }
}
