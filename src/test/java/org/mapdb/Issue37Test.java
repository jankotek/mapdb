package org.mapdb;


import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class Issue37Test {


    @Test
    public void test2() throws IOException{

        /**
         * Tijdelijke on-disk database gebruiken.
         */
        DB db = DBMaker.newTempFileDB().cacheDisable().make();
        ConcurrentMap<String,String> testData = db.createHashMap("test",false,
                null, null);

        BufferedReader in = new BufferedReader(new FileReader("./src/test/resources/Issue37Data.txt"));
        String line = null;

        while ((line = in.readLine()) != null){
            testData.put(line,line);
        }

        db.commit();

        int printCount = 0;

        for(String key : testData.keySet()){
            printCount++;

            String data = testData.get(key);

            if(printCount > 10000){
                fail();
            }
        }
    }

    @Test public void test3(){

        DB db = DBMaker.newDirectMemoryDB().writeAheadLogDisable().asyncFlushDelay(100).make();
        ConcurrentMap<Long, Long> orders = db.createHashMap("order", false, null, null);
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
