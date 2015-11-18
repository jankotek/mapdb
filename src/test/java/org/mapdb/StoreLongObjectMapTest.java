package org.mapdb;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class StoreLongObjectMapTest {

    @Test public void sequentialUpdates(){
        Map<Long,Long> h = new HashMap<Long, Long>();
        Store.LongObjectMap<Long> m = new Store.LongObjectMap();


        for(long i=1;i<10000L;i++){
            h.put(i,i*2);
            m.put(i, i * 2);
        }

        for(Map.Entry<Long,Long> e:h.entrySet()){
            assertEquals(e.getValue(), new Long(m.get(e.getKey())));
        }

        assertEquals(m.size, h.size());

        long[] t = m.set;
        for(int i=0;i<t.length;i++){
            long key = t[i];
            if(key==0)
                continue;
            assertEquals(h.get(key), m.values[i]);
        }

    }


    @Test public void sequentialUpdates2(){
        Map<Long,Long> h = new HashMap<Long, Long>();
        Store.LongObjectMap<Long> m = new Store.LongObjectMap();


        for(long i=1;i<10000L;i++){
            h.put(i,i*2);
            m.put(i, i * 2);
        }
        for(long i=1;i<10000L;i++){
            h.put(i,i*3);
            m.put(i, i * 3);
        }



        for(Map.Entry<Long,Long> e:h.entrySet()){
            assertEquals(e.getValue(), new Long(m.get(e.getKey())));
        }

        assertEquals(m.size, h.size());

        long[] t = m.set;
        for(int i=0;i<t.length;i++){
            long key = t[i];
            if(key==0)
                continue;
            assertEquals(h.get(key), m.values[i]);
        }

    }

    @Test public void random(){
        Random r = new Random();



    }

}