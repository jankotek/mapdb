package org.mapdb;

import org.junit.Assert;
import org.junit.Test;

public class CCTest {

    @Test public void  concurency(){
        long i = 2;
        while(i<Integer.MAX_VALUE){
            i = i*2;
            if(i==CC.CONCURRENCY) return;
        }
        Assert.fail("no power of two");
    }
}
