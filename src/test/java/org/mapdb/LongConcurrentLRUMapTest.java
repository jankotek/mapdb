package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class LongConcurrentLRUMapTest {


    @Test
    public void overfill(){
        final LongConcurrentLRUMap<Long> l = new LongConcurrentLRUMap(1000,1000-1);

        for(Long i=0L;i<1e5;i++) {
            l.put(i, i);
            if(i>0){
                Long other = l.get(i-1);
                assertTrue(other==null || (i-1==other));
            }
        }
    }
}