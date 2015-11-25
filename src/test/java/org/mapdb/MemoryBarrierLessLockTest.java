package org.mapdb;

import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

public class MemoryBarrierLessLockTest {

    final Store.MemoryBarrierLessLock lock = new Store.MemoryBarrierLessLock();

    @Test
    public void lock(){
        lock.lock();
        lock.unlock();
        lock.lock();
        lock.unlock();
        lock.lock();
        lock.unlock();
    }

    @Test public void par(){
        final AtomicLong counter = new AtomicLong();
        Exec.execNTimes(10, new Callable() {
            @Override
            public Object call() throws Exception {
                for(int i=0;i<1000000* TT.scale();i++){
                    lock.lock();
                    long c = counter.get();
                    counter.set(c+1);
                    lock.unlock();
                }
                return null;
            };
        });

        assertEquals(10L*1000000* TT.scale(),counter.get());
    }

    @Test(expected=IllegalMonitorStateException.class)
    public void unlock(){
        lock.unlock();
    }

}