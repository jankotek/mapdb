package org.mapdb.benchmark;

import org.mapdb.DataIO;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Tests how long it takes to insert 100M items
 */
public class InMemoryGet {

    static final int memUsage = 20;
    static final int max = (int) 1e6;
    
    static final Map<String, Callable<Map<Long,UUID>>> fabs = InMemoryCreate.fabs;

    public static void main(String[] args) throws Throwable {

        String name = args[0];
        Map map = fabs.get(name).call();
        for (long i=0;i<max;i++) {
            UUID val = new UUID(DataIO.longHash(i),DataIO.longHash(i+1)); //Random is too slow, so use faster hash
            map.put(i, val);
        }
        long time = System.currentTimeMillis();
        Random r = new Random();
        int res = 0;
        for (long i=0;i<max;i++) {
            long key = r.nextInt(max);
            res = map.get(key).hashCode();
        }
        if(res==0)
            System.out.println();
        System.out.println(System.currentTimeMillis()-time);

    }

}
