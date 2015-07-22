package org.mapdb;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

/**
 * Test if DB will survive crash simulated by Thread.stop()
 */
@RunWith(Parameterized.class)
public class CrashWithInterruptTest {

    private static final boolean[] BOOLS = {true, false};

    final File file;
    final DBMaker.Maker dbMaker;
    final boolean clearMap;
    final boolean hashMap;
    final boolean largeVals;

    public CrashWithInterruptTest(File file, DBMaker.Maker dbMaker, boolean clearMap, boolean hashMap, boolean largeVals) throws IOException {
        this.file = file;
        this.dbMaker = dbMaker;
        this.clearMap = clearMap;
        this.hashMap = hashMap;
        this.largeVals = largeVals;
    }

    @Parameterized.Parameters
    public static Iterable params() throws IOException {
        List ret = new ArrayList();
        if(TT.shortTest())
            return ret;

        for(boolean notAppend:BOOLS){
            for(boolean tx:BOOLS){
                for(boolean mmap:BOOLS) {
                    for (boolean cache : BOOLS) {
                        for (boolean largeVals : BOOLS) {
                            for (boolean clearMap : BOOLS) {
                                for (boolean hashMap : BOOLS) {
                                    File f = File.createTempFile("mapdbTest", "mapdb");
                                    DBMaker.Maker maker = !notAppend ?
                                            DBMaker.appendFileDB(f) :
                                            DBMaker.fileDB(f);

                                    maker.fileLockDisable();

                                    if (mmap)
                                        maker.fileMmapEnableIfSupported();

                                    if (!tx)
                                        maker.transactionDisable();

                                    if (cache)
                                        maker.cacheHashTableEnable();

                                    ret.add(new Object[]{f, maker, clearMap, hashMap, largeVals});
                                }
                            }
                        }
                    }
                }
            }
        }

        return ret;
    }

    DB db;
    Atomic.Long counter;
    Map<Long,byte[]> map;

    @Test
    public void  crash_with_interrupt() throws InterruptedException {
        int scale = TT.scale();
        if(scale==0)
            return;

        final long endTime = TT.nowPlusMinutes(5);

        db = dbMaker.make();
        if(!db.engine.canRollback() || db.engine instanceof StoreHeap) //TODO engine might have crash recovery, but no rollbacks
            return;

        counter = db.atomicLong("counter");
        map = reopenMap();

        //fill recids
        final int max = scale*1000;
        for(long j=0;j<max;j++){
            map.put(j, new byte[0]);
        }

        final AtomicLong a = new AtomicLong(10);

        while(endTime>System.currentTimeMillis()) {

            final CountDownLatch latch = new CountDownLatch(1);
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        for (; ; ) {
                            if(clearMap)
                                map.clear();
                            long A = a.incrementAndGet();
                            Random r = new Random(A);
                            counter.set(A);

                            for(long j=0;j<max;j++){
                                int size = r.nextInt(largeVals?100000:100);
                                byte[] b = TT.randomByteArray(size, r.nextInt());
                                map.put(j,b);
                            }
                            db.commit();
                        }
                    }finally {
                        latch.countDown();
                    }
                }
            };
            t.start();
            Thread.sleep(5000);
            t.stop();
            latch.await();

            //reopen and check the content
            db = dbMaker.make();
            map = reopenMap();
            counter = db.atomicLong("counter");


            //check if A-1 was commited
            long A = counter.get();
            assertTrue(A == a.get() || A == a.get() - 1);
            Random r = new Random(A);
            for(long j=0;j<max;j++){
                int size = r.nextInt(largeVals?100000:100);
                byte[] b = TT.randomByteArray(size, r.nextInt());
                byte[] b2 = map.get(j);
                assertTrue("Data were not commited", Arrays.equals(b, b2));
            }
        }
        db.close();
        file.delete();

    }

    private Map<Long, byte[]> reopenMap() {
        return (Map) (hashMap?
                        db.hashMapCreate("map")
                                .keySerializer(Serializer.LONG)
                                .valueSerializer(Serializer.BYTE_ARRAY)
                                .makeOrGet():
                        db.treeMapCreate("map")
                                .keySerializer(Serializer.LONG)
                                .valueSerializer(Serializer.BYTE_ARRAY)
                                .valuesOutsideNodesEnable()
                                .makeOrGet());
    }
}
