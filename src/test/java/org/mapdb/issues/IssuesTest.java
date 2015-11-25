package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.*;

import java.io.File;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;

public class IssuesTest {

    @Test public void issue130(){
        File f = TT.tempDbFile();
        DB db = DBMaker.appendFileDB(f)
                .closeOnJvmShutdown()
                .make();

        Map store = db.treeMap("collectionName");

        db.close();
        f.delete();
    }


    @Test public void issue561(){
        final File file = TT.tempDbFile();
        final String queueName = "testqueue";
        DB db = DBMaker
                .fileDB(file)
                .fileMmapEnable()
                .fileMmapCleanerHackEnable()
                .transactionDisable()
                .cacheSize(128)
                .closeOnJvmShutdown()
                .make();
        BlockingQueue<String> queue = db.getQueue(queueName);
        String next = queue.poll();
        db.compact();
        db.commit();
        next = queue.poll();
        db.close();
        file.delete();
    }

    @Test public void issue468(){
        DB db = DBMaker.memoryDB().transactionDisable().make();
        db.createCircularQueue("recents", Serializer.STRING, 200);
        db.close();
    }

    @Test public void issue567(){
        File dbFile = TT.tempDbFile();
        DBMaker.Maker dbMaker = DBMaker.fileDB(dbFile).cacheHardRefEnable();
        TxMaker txMaker = dbMaker.makeTxMaker();

        DB db1 = txMaker.makeTx();
        db1.treeMapCreate("test1").makeOrGet();
        db1.commit();
        db1.close();

        DB db2 = txMaker.makeTx();
        db2.treeMapCreate("test2").makeOrGet();
        db2.commit();
        db2.close();
    }

    @Test public void issue570(){
        int scale = TT.scale();
        if(scale==0)
            return;
        File f = TT.tempDbFile();
        for(int j=0;j<100*scale;j++) {
            DB db = DBMaker.fileDB(f)
                    .checksumEnable()
                    .make();
            StoreWAL w = (StoreWAL) db.getEngine();
            Map<String, String> map = db.hashMap("testMap");

            for (int i = 0; i < 10; i++) {
                map.put(""+j, "someval");
                db.commit();
            }
            db.compact();
            db.close();
        }
        f.delete();
    }

    @Test public void issue581() throws Throwable {
        DB db = DBMaker.heapDB().make();
        final Map map = db.treeMap("map");
        int entries = 1000000;

        ExecutorService exec = Executors.newFixedThreadPool(20);
        final AtomicReference<Throwable> ex = new AtomicReference<Throwable>(null);
        for(int i=0;i<entries;i++){
            final String val = ""+i;
            final int id = val.hashCode();
            exec.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        map.put(id, val);
                        if (!map.containsKey(id))
                            ex.set(new AssertionError("does not contain key:" + id + " val:" + val));
                    }catch(Throwable e){
                        ex.set(e);
                    }
                }
            });
        }

        exec.shutdown();
        while (!exec.awaitTermination(10, TimeUnit.MILLISECONDS) && ex.get()==null) {
        }
        Throwable e = ex.get();
        if (e != null)
            throw new AssertionError(e);
    }

    @Test public void issue595(){
        BTreeMap m = DBMaker.heapDB().transactionDisable().make().treeMap("aa");

        for(int i=0;i<1000;i++){
            m.put(i,i);
        }
        m.descendingMap();
        for(int i=0;i<1000;i++) {
            m.tailMap(i).descendingMap();
            m.headMap(i).descendingMap();
        }
    }

    @Test public void issue634_1(){
        File f = TT.tempDbFile();

        for(int j=0;j<10;j++) {

            DB db = DBMaker.appendFileDB(f).checksumEnable().make();

            Map m = db.hashMapCreate("segment").makeOrGet();

            for (int i = 0; i < 10; i++) {
                if(j>0){
                    assertArrayEquals(TT.randomByteArray(100,(j-1)*i), (byte[])m.get(i));
                }
                m.put(i, TT.randomByteArray(100,j*i));
                db.commit();
            }
            db.commit();
            db.close();
        }

        f.delete();
    }



}
