package benchmark;


import com.carrotsearch.junitbenchmarks.AbstractBenchmark;
import com.sun.servicetag.SystemEnvironment;
import org.junit.Test;
import org.mapdb.CC;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

public class Synchronized_Versus_ReentrantLock extends AbstractBenchmark{

    final int threadNum = 32;
    final int max = 10000;

    long counter = 0;

    @Test
    public void test_synchronized() throws InterruptedException {
        assumeTrue(CC.FULL_TEST);
        counter = 0;
        final Object lock = new Object();
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for(int j=0;j<max;j++){
                        synchronized (lock){
                            counter++;
                        }
                    }
                }
            });
        }


        exec.shutdown();
        while(!exec.awaitTermination(1, TimeUnit.HOURS)){};
        assertEquals(threadNum * max, counter);
    }

    @Test
    public void test_reentrant_lock() throws InterruptedException {
        assumeTrue(CC.FULL_TEST);
        counter = 0;
        final ReentrantLock lock = new ReentrantLock();
        ExecutorService exec = Executors.newFixedThreadPool(threadNum);
        for(int i=0;i<threadNum;i++){
            exec.execute(new Runnable() {
                @Override
                public void run() {
                    for(int j=0;j<max;j++){
                        lock.lock();
                        counter++;
                        lock.unlock();
                    }
                }
            });
        }


        exec.shutdown();
        while(!exec.awaitTermination(1, TimeUnit.HOURS)){};
        assertEquals(threadNum * max, counter);
    }


}
