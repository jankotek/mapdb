package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.TT;

import java.io.File;
import java.util.Set;

public class Issue418Test {

    @Test
    public void test(){
        final File tmp = TT.tempFile();

        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.fileDB(tmp).make();
            final HTreeMap map = db.hashMap("foo").expireMaxSize(100).createOrOpen();


            for (int i = 0; i < TT.testScale()*10000; i++)
                    map.put("foo" + i, "bar" + i);


            db.commit();
            db.close();
        }
    }


    @Test
    public void test_set(){
        final File tmp = TT.tempFile();

        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.fileDB(tmp).make();
            final Set map = db.hashSet("foo").expireMaxSize(100).createOrOpen();

            for (int i = 0; i < TT.testScale()*10000; i++)
                map.add("foo" + i);

            db.commit();
            db.close();
        }
    }
}
