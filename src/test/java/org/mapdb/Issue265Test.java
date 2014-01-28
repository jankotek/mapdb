package org.mapdb;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class Issue265Test {

    @Test
    public void compact(){
        DBMaker dbMaker = DBMaker.newMemoryDB()
                .transactionDisable() // breaks functionality even in version 0.9.7
                .cacheDisable();
        DB db = dbMaker.make();
        try {
            Map<Integer, String> map = db.getHashMap("HashMap");
            map.put(1, "one");
            map.put(2, "two");
            map.remove(1);
            db.commit();
            db.compact();
            Assert.assertEquals(1, map.size());
        } finally {
            db.close();
        }
    }

    @Test
    public void compact_no_tx(){
        DBMaker dbMaker = DBMaker.newMemoryDB()
                .cacheDisable();
        DB db = dbMaker.make();
        try {
            Map<Integer, String> map = db.getHashMap("HashMap");
            map.put(1, "one");
            map.put(2, "two");
            map.remove(1);
            db.commit();
            db.compact();
            Assert.assertEquals(1, map.size());
        } finally {
            db.close();
        }
    }

}
