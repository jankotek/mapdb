package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Issue419Test {

    @Test public void isolate(){

        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        Set set = db.hashSetCreate("set").expireAfterAccess(30, TimeUnit.DAYS).make();
        for (int i = 0; i < 10000; i++)
            set.add(i);

        assertTrue(set.contains(1));
        assertEquals(10000, set.size());

        db.close();

        db = DBMaker.fileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        set = db.hashSet("set");
        for (int i = 0; i < 10000; i++)
            set.add(i);

        assertTrue(set.contains(1));
        assertEquals(10000, set.size());

        db.close();
    }

    @Test public void isolate_map(){

        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.fileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        Map set = db.hashMapCreate("set").expireAfterAccess(30, TimeUnit.DAYS).make();
        for (int i = 0; i < 10000; i++)
            set.put(i, "");

        assertTrue(set.containsKey(1));
        assertEquals(10000, set.size());

        db.close();

        db = DBMaker.fileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        set = db.hashMap("set");
        for (int i = 0; i < 10000; i++)
            set.put(i,"");

        assertTrue(set.containsKey(1));
        assertEquals(10000, set.size());

        db.close();
    }
}
