package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@RunWith(value = Parameterized.class)
public class RecidLocksTest {

    final Locks.RecidLocks locks;
    final ExecutorService e = Executors.newFixedThreadPool(4);

    public RecidLocksTest(Locks.RecidLocks locks) {
        this.locks = locks;
    }

    @Parameterized.Parameters
    public static List<?> params(){
        return Arrays.asList(new Object[][]{{new Locks.LongHashMapRecidLocks()}});
    }




    @Test public void perf_multi_collisions() throws InterruptedException {
        run_multi("multi_collisions", 1111, 1111, 1111, 111);
    }

    @Test public void perf_multi() throws InterruptedException {
        run_multi("multi", 1, 2, 3, 4);
    }


    void run_multi(String rec, int... recids) throws InterruptedException {
        final int max = (int) 1e6;
        long t = System.currentTimeMillis();
        for(final int recid:recids){
            e.execute(new Runnable() {
                @Override public void run() {
                    for(int i=0;i<max;i++){
                        locks.lock(recid);
                        locks.unlock(recid);
                    }
                }
            });
        }

        e.shutdown();
        e.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        System.out.println(System.currentTimeMillis() - t);
    }
}
