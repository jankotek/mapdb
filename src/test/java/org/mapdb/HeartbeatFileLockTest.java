package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


public class HeartbeatFileLockTest {


    @Test
    public void testFutureModificationDate() throws Exception {
        if(TT.scale()==0)
            return;

        File f = File.createTempFile("mapdbTest","madpb");
        f.delete();
        f.createNewFile();
        f.setLastModified(System.currentTimeMillis() + 10000);
        DataIO.HeartbeatFileLock lock = new DataIO.HeartbeatFileLock(f,CC.FILE_LOCK_HEARTBEAT);
        lock.lock();
        lock.unlock();
    }

    @Test
    public void testSimple() throws IOException {
        if(TT.scale()==0)
            return;
        File f = File.createTempFile("mapdbTest","madpb");
        f.delete();

        DataIO.HeartbeatFileLock lock1 = new DataIO.HeartbeatFileLock(f,CC.FILE_LOCK_HEARTBEAT);
        DataIO.HeartbeatFileLock lock2 = new DataIO.HeartbeatFileLock(f,CC.FILE_LOCK_HEARTBEAT);
        f.delete();
        new DataIO.HeartbeatFileLock(f,CC.FILE_LOCK_HEARTBEAT);
        lock1.lock();
        //second lock should throw exception
        try{
            lock2.lock();
            fail();
        }catch(DBException.FileLocked e){
            //ignored;
        }

        lock1.unlock();
        lock2 =  new DataIO.HeartbeatFileLock(f,CC.FILE_LOCK_HEARTBEAT);
        lock2.lock();
        lock2.unlock();
    }


    @Test
    public void test_parallel() throws InterruptedException, IOException, ExecutionException {
        int count = 16* TT.scale();
        final long end = System.currentTimeMillis()+100000*count;
        if(count==0)
            return;

        final File f = File.createTempFile("mapdbTest","mapdb");
        f.delete();

        final AtomicInteger counter = new AtomicInteger();
        List<Future> futures = TT.fork(count, new Callable() {
            @Override
            public Object call() throws Exception {
                while (System.currentTimeMillis() < end) {
                    DataIO.HeartbeatFileLock lock = new DataIO.HeartbeatFileLock(f, CC.FILE_LOCK_HEARTBEAT);
                    try {
                        lock.lock();
                    } catch (DBException.FileLocked e) {
                        continue;
                    }
                    assertEquals(1, counter.incrementAndGet());
                    lock.unlock();
                    assertEquals(0, counter.decrementAndGet());
                }
                return null;
            }
        });


        //await termination
        TT.forkAwait(futures);
    }


}