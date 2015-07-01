package org.mapdb;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class StoreLongLongMapTest {

    @Test public void sequentialUpdates(){
        Map<Long,Long> h = new HashMap<Long, Long>();
        Store.LongLongMap m = new Store.LongLongMap();


        for(long i=1;i<10000L;i++){
            h.put(i,i*2);
            m.put(i, i * 2);
        }

        for(Map.Entry<Long,Long> e:h.entrySet()){
            assertEquals(e.getValue(), new Long(m.get(e.getKey())));
        }

        assertEquals(m.size(), h.size());

        long[] t = m.table;
        for(int i=0;i<t.length;i+=2){
            long key = t[i];
            if(key==0)
                continue;
            assertEquals(h.get(key), new Long(t[i+1]));
        }

    }


    @Test public void sequentialUpdates2(){
        Map<Long,Long> h = new HashMap<Long, Long>();
        Store.LongLongMap m = new Store.LongLongMap();


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

        assertEquals(m.size(), h.size());

        long[] t = m.table;
        for(int i=0;i<t.length;i+=2){
            long key = t[i];
            if(key==0)
                continue;
            assertEquals(h.get(key), new Long(t[i+1]));
        }

    }

    @Test public void random(){
        Random r = new Random();



    }

}