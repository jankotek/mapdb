package org.mapdb;

import org.junit.Test;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class BTreeMapParTest {


    final int threadNum = 6;
    final int max = (int) 1e6;

    @Test
    public void parInsert() throws InterruptedException {

        ExecutorService s = Executors.newCachedThreadPool();
        final ConcurrentMap m = DBMaker.newMemoryDB().transactionDisable().make()
                .createTreeMap("test")
                .valueSerializer(Serializer.LONG)
                .makeLongMap();

        long t = System.currentTimeMillis();

        for(int j=0;j<threadNum;j++){
            final long core = j;
            s.submit(new Runnable() {
                @Override public void run() {
                    for(Long n=core;n<max;n+=threadNum){
                        m.put(n,n);
                    }
                }
            });


        }

        s.shutdown();
        s.awaitTermination(10, TimeUnit.SECONDS);

        System.out.printf("  Threads %d, time %,d\n",threadNum,System.currentTimeMillis()-t);


        assertEquals(max,m.size());
    }
}
