package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.util.Set;

import static org.junit.Assert.*;

public class Issue418Test {

    @Test
    public void test(){
        final File tmp = TT.tempDbFile();

        long[] expireHeads = null;
        long[] expireTails = null;
        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.fileDB(tmp).transactionDisable().make();
            final HTreeMap<Object, Object> map = db.hashMapCreate("foo").expireMaxSize(100).makeOrGet();

            if(expireHeads!=null)
                assertTrue(Serializer.LONG_ARRAY.equals(expireHeads, map.expireHeads));
            else
                expireHeads = map.expireHeads;

            if(expireTails!=null)
                assertTrue(Serializer.LONG_ARRAY.equals(expireTails, map.expireTails));
            else
                expireTails = map.expireTails;



            for (int i = 0; i < TT.scale()*10000; i++)
                    map.put("foo" + i, "bar" + i);


            db.commit();
            db.close();
        }
    }


    @Test
    public void test_set(){
        final File tmp = TT.tempDbFile();

        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.fileDB(tmp).transactionDisable().make();
            final Set<Object> map = db.hashSetCreate("foo").expireMaxSize(100).makeOrGet();

            for (int i = 0; i < TT.scale()*10000; i++)
                map.add("foo" + i);

            db.commit();
            db.close();
        }
    }
}
