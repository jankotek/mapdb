package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class Issue37Test {



    @Test public void test3(){

        DB db = DBMaker.memoryDirectDB().transactionDisable().asyncWriteFlushDelay(100).make();
        ConcurrentMap<Long, Long> orders = db.hashMapCreate("order").make();
        for(int i = 0; i < 10000; i++) {
            orders.put((long)i, (long)i);
        }
        assertEquals(10000, orders.size());


        int progress = 0;
        Set returned = new LinkedHashSet();
        Iterator iter = orders.keySet().iterator();
        while(iter.hasNext()) {
            Object key = iter.next();

            if(returned.contains(key))
                throw new AssertionError("already found: "+key);
            returned.add(key);
            progress++;
            assertTrue(progress <= 10000);
        }

        iter = orders.entrySet().iterator();
        progress=0;
        while(iter.hasNext()) {
            progress++;
            iter.next();
            assertTrue(progress <= 10000);
        }

        iter = orders.values().iterator();
        progress=0;
        while(iter.hasNext()) {
            progress++;
            iter.next();
            assertTrue(progress <= 10000);
        }

    }

}
