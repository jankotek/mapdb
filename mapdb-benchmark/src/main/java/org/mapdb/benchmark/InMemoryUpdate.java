package org.mapdb.benchmark;

import org.mapdb.DataIO;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Tests how long it takes to update 100M items
 */
public class InMemoryUpdate {

    static final int memUsage = 20;
    static final int max = (int) 1e6;
    
    static final Map<String, Callable<Map<Long,UUID>>> fabs = InMemoryCreate.fabs;

    public static void main(String[] args) throws Throwable {
        String name = args[0];
        Map map = fabs.get(name).call();
        //fill map
        for (long i=0;i<max;i++) {
            UUID val = new UUID(DataIO.longHash(i),DataIO.longHash(i+1)); //Random is too slow, so use faster hash
            map.put(i, val);
        }
        //now start measuring

        long time = System.currentTimeMillis();
        Random r = new Random();
        for (long i=0;i<max;i++) {
            long key = r.nextInt(max);
            UUID val = new UUID(DataIO.longHash(i),DataIO.longHash(i+1)); //Random is too slow, so use faster hash
            map.put(key, val);
        }
        System.out.println(System.currentTimeMillis()-time);
    }

}
