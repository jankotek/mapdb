package org.mapdb10;

import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class Issue400Test {

    @Test
    public void expire_maxSize_with_TTL() throws InterruptedException {
        File f = UtilsTest.tempDbFile();
        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.newFileDB(f).transactionDisable().make();
            final HTreeMap<Object, Object> map = db.createHashMap("foo")
                    .expireMaxSize(1000).expireAfterWrite(1, TimeUnit.DAYS)
                    .makeOrGet();

            map.put("foo", "bar");

            assertEquals("bar", map.get("foo"));

            Thread.sleep(1100);
            assertEquals("bar", map.get("foo"));

            db.commit();
            db.close();
            Thread.sleep(1100);
        }
    }

    @Test(timeout = 200000)
    public void expire_maxSize_with_TTL_short() throws InterruptedException {
        File f = UtilsTest.tempDbFile();
        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.newFileDB(f).transactionDisable().make();
            final HTreeMap<Object, Object> map = db.createHashMap("foo")
                    .expireMaxSize(1000).expireAfterWrite(3, TimeUnit.SECONDS)
                    .makeOrGet();

            map.put("foo", "bar");

            assertEquals("bar", map.get("foo"));

            while(map.get("foo")!=null){
                Thread.sleep(100);
            }

            db.commit();
            db.close();
            Thread.sleep(1100);
        }
    }

    @Test(timeout = 600000)
    public void expire_maxSize_with_TTL_get() throws InterruptedException {
        File f = UtilsTest.tempDbFile();
        for (int o = 0; o < 2; o++) {
            final DB db = DBMaker.newFileDB(f).transactionDisable().make();
            final HTreeMap<Object, Object> map = db.createHashMap("foo")
                    .expireMaxSize(1000).expireAfterAccess(3, TimeUnit.SECONDS)
                    .makeOrGet();

            map.put("foo", "bar");

            for(int i=0;i<10;i++)
                assertEquals("bar", map.get("foo"));

            Thread.sleep(6000);
            assertEquals(null, map.get("foo"));

            db.commit();
            db.close();
            Thread.sleep(1100);
        }
    }

}
