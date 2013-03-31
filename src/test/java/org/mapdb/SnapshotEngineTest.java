package org.mapdb;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class SnapshotEngineTest{

    SnapshotEngine e = new SnapshotEngine(new StoreWAL(Volume.memoryFactory(false)));

    @Test public void update(){
        long recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        Engine snapshot = e.snapshot();
        e.update(recid, 222, Serializer.INTEGER_SERIALIZER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER_SERIALIZER));
    }

    @Test public void compareAndSwap(){
        long recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        Engine snapshot = e.snapshot();
        e.compareAndSwap(recid, 111, 222, Serializer.INTEGER_SERIALIZER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER_SERIALIZER));
    }

    @Test public void delete(){
        long recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        Engine snapshot = e.snapshot();
        e.delete(recid,Serializer.INTEGER_SERIALIZER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER_SERIALIZER));
    }

    @Test public void notExist(){
        Engine snapshot = e.snapshot();
        long recid = e.put(111, Serializer.INTEGER_SERIALIZER);
        assertNull(snapshot.get(recid, Serializer.INTEGER_SERIALIZER));
    }


    @Test public void create_snapshot(){
        Engine e = DBMaker.newMemoryDB().makeEngine();
        Engine snapshot = SnapshotEngine.createSnapshotFor(e);
        assertNotNull(snapshot);
    }

    @Test public void DB_snapshot(){
        DB db = DBMaker.newMemoryDB().asyncFlushDelay(100).writeAheadLogDisable().make();
        long recid = db.getEngine().put("aa",Serializer.STRING_SERIALIZER);
        DB db2 = db.snapshot();
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER));
        db.getEngine().update(recid, "bb",Serializer.STRING_SERIALIZER);
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER));
    }

    @Test public void DB_snapshot2(){
        DB db = DBMaker.newMemoryDB().writeAheadLogDisable().make();
        long recid = db.getEngine().put("aa",Serializer.STRING_SERIALIZER);
        DB db2 = db.snapshot();
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER));
        db.getEngine().update(recid, "bb",Serializer.STRING_SERIALIZER);
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_SERIALIZER));
    }


    @Test public void BTreeMap_snapshot(){
        BTreeMap map =
                DBMaker.newMemoryDB().writeAheadLogDisable()
                .make().getTreeMap("aaa");
        map.put("aa","aa");
        Map map2 = map.snapshot();
        map.put("aa","bb");
        assertEquals("aa",map2.get("aa"));
    }

    @Test public void HTreeMap_snapshot(){
        HTreeMap map =
                DBMaker.newMemoryDB().writeAheadLogDisable()
                .make().getHashMap("aaa");
        map.put("aa","aa");
        Map map2 = map.snapshot();
        map.put("aa","bb");
        assertEquals("aa",map2.get("aa"));
    }

//    @Test public void test_stress(){
//        ExecutorService ex = Executors.newCachedThreadPool();
//
//        TxMaker tx = DBMaker.newMemoryDB().writeAheadLogDisable().makeTxMaker();
//
//        DB db = tx.makeTx();
//        final long recid =
//
//        final int threadNum = 32;
//        for(int i=0;i<threadNum;i++){
//            ex.execute(new Runnable() { @Override public void run() {
//
//            }});
//        }
//    }

}
