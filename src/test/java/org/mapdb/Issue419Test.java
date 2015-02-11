package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Issue419Test {

    @Test
    public void dbSetFile() {

        File f = UtilsTest.tempDbFile();


        {
// configure and open database using builder pattern.
// all options are available with code auto-completion.
            DB db = DBMaker.newFileDB(f)
                    .closeOnJvmShutdown().transactionDisable()
/* encryptionEnable("password") */.make();

            String setName = "hashSet2";
            Set<Integer> set = null;

            if (!db.exists(setName)) {
                DB.HTreeSetMaker treeSet = db.createHashSet(setName);
                set = treeSet.expireAfterAccess(30, TimeUnit.DAYS).make();
                System.out.println("create");
            } else {
                set = db.getHashSet(setName);
                System.out.println("read exists");
            }

            System.out.println(set.contains(1));
            for (int i = 0; i < 10000; i++)
                set.add(i);

            System.out.println(set.size());
            db.close();
        }


        {
// configure and open database using builder pattern.
// all options are available with code auto-completion.
            DB db = DBMaker.newFileDB(f)
                    .closeOnJvmShutdown().transactionDisable()
/* encryptionEnable("password") */.make();

            String setName = "hashSet2";
            Set<Integer> set = null;

            if (!db.exists(setName)) {
                DB.HTreeSetMaker treeSet = db.createHashSet(setName);
                set = treeSet.expireAfterAccess(30, TimeUnit.DAYS).make();
                System.out.println("create");
            } else {
                set = db.getHashSet(setName);
                System.out.println("read exists");
            }

            System.out.println(set.contains(1));
            for (int i = 0; i < 10000; i++)
                set.add(i);

            System.out.println(set.size());
            db.close();
        }
    }
    @Test public void isolate(){

        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        Set set = db.createHashSet("set").expireAfterAccess(30, TimeUnit.DAYS).make();
        for (int i = 0; i < 10000; i++)
            set.add(i);

        assertTrue(set.contains(1));
        assertEquals(10000, set.size());

        db.close();

        db = DBMaker.newFileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        set = db.getHashSet("set");
        for (int i = 0; i < 10000; i++)
            set.add(i);

        assertTrue(set.contains(1));
        assertEquals(10000, set.size());

        db.close();
    }

    @Test public void isolate_map(){

        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        Map set = db.createHashMap("set").expireAfterAccess(30, TimeUnit.DAYS).make();
        for (int i = 0; i < 10000; i++)
            set.put(i, "");

        assertTrue(set.containsKey(1));
        assertEquals(10000, set.size());

        db.close();

        db = DBMaker.newFileDB(f)
                .closeOnJvmShutdown().transactionDisable().make();

        set = db.getHashMap("set");
        for (int i = 0; i < 10000; i++)
            set.put(i,"");

        assertTrue(set.containsKey(1));
        assertEquals(10000, set.size());

        db.close();
    }
}
