package org.mapdb.issues;

import org.junit.Assert;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Map;

public class Issue265Test {

    @Test
    public void compact(){
            DB db = DBMaker.memoryDB()
                .transactionDisable()
                .make(); // breaks functionality even in version 0.9.7

            Map<Integer, String> map = db.hashMap("HashMap");
            map.put(1, "one");
            map.put(2, "two");
            map.remove(1);
            db.commit();
            db.compact();
            Assert.assertEquals(1, map.size());

            db.close();

    }

    @Test
    public void compact_no_tx(){
            DB db = DBMaker.memoryDB().make();

            Map<Integer, String> map = db.hashMap("HashMap");
            map.put(1, "one");
            map.put(2, "two");
            map.remove(1);
            db.commit();
            db.compact();
            Assert.assertEquals(1, map.size());

            db.close();

    }

}
