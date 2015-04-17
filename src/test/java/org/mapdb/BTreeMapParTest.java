package org.mapdb;

import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class BTreeMapParTest {


    final int threadNum = 6;
    final int max = (int) 1e6;

    @Test
    public void parInsert() throws InterruptedException {


        final ConcurrentMap m = DBMaker.memoryDB().transactionDisable().make()
                .treeMapCreate("test")
                .valueSerializer(Serializer.LONG)
                .keySerializer(BTreeKeySerializer.LONG)
                .makeLongMap();

        long t = System.currentTimeMillis();
        final AtomicLong counter = new AtomicLong();

        Exec.execNTimes(threadNum, new Callable() {
            @Override
            public Object call() throws Exception {
                long core = counter.getAndIncrement();
                for (Long n = core; n < max; n += threadNum) {
                    m.put(n, n);
                }

                return null;
            }
        });

//        System.out.printf("  Threads %d, time %,d\n",threadNum,System.currentTimeMillis()-t);


        assertEquals(max,m.size());
    }
}
