package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;

public class Issue418Test {

    @Test
    public void test(){
        final File tmp = UtilsTest.tempDbFile();

        long[] expireHeads = null;
        long[] expireTails = null;
        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.newFileDB(tmp).transactionDisable().make();
            final HTreeMap<Object, Object> map = db.createHashMap("foo").expireMaxSize(100).makeOrGet();

            if(expireHeads!=null)
                assertArrayEquals(expireHeads, map.expireHeads);
            else
                expireHeads = map.expireHeads;

            if(expireTails!=null)
                assertArrayEquals(expireTails, map.expireTails);
            else
                expireTails = map.expireTails;



            for (int i = 0; i < 1000; i++)
                    map.put("foo" + i, "bar" + i);


            db.commit();
            db.close();
        }
    }


    @Test
    public void test_set(){
        final File tmp = UtilsTest.tempDbFile();

        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.newFileDB(tmp).transactionDisable().make();
            final Set<Object> map = db.createHashSet("foo").expireMaxSize(100).makeOrGet();

            for (int i = 0; i < 1000; i++)
                map.add("foo" + i);

            db.commit();
            db.close();
        }
    }
}
