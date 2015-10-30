package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class TxEngineTest {

    TxEngine e;


    @Before public void init(){
        Store store = new StoreWAL(null);
        store.init();
        e = new TxEngine(store,true, CC.DEFAULT_LOCK_SCALE);
    }

    @Test public void update(){
        long recid = e.put(111, Serializer.INTEGER);
        e.commit();
        Engine snapshot = e.snapshot();
        e.update(recid, 222, Serializer.INTEGER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER));
    }

    @Test public void compareAndSwap(){
        long recid = e.put(111, Serializer.INTEGER);
        e.commit();
        Engine snapshot = e.snapshot();
        e.compareAndSwap(recid, 111, 222, Serializer.INTEGER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER));
    }

    @Test public void delete(){
        long recid = e.put(111, Serializer.INTEGER);
        e.commit();
        Engine snapshot = e.snapshot();
        e.delete(recid, Serializer.INTEGER);
        assertEquals(Integer.valueOf(111), snapshot.get(recid, Serializer.INTEGER));
    }

    @Test public void notExist(){
        Engine snapshot = e.snapshot();
        long recid = e.put(111, Serializer.INTEGER);
        assertNull(snapshot.get(recid, Serializer.INTEGER));
    }


    @Test public void create_snapshot(){
        Engine e = DBMaker.memoryDB().snapshotEnable().makeEngine();
        Engine snapshot = TxEngine.createSnapshotFor(e);
        assertNotNull(snapshot);
    }

    @Test public void DB_snapshot(){
        DB db = DBMaker.memoryDB().snapshotEnable().asyncWriteFlushDelay(100).transactionDisable().make();
        long recid = db.getEngine().put("aa", Serializer.STRING_NOSIZE);
        DB db2 = db.snapshot();
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_NOSIZE));
        db.getEngine().update(recid, "bb",Serializer.STRING_NOSIZE);
        assertEquals("aa", db2.getEngine().get(recid, Serializer.STRING_NOSIZE));
    }

    @Test public void DB_snapshot2(){
        DB db = DBMaker.memoryDB().transactionDisable().snapshotEnable().make();
        long recid = db.getEngine().put("aa",Serializer.STRING_NOSIZE);
        DB db2 = db.snapshot();
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_NOSIZE));
        db.getEngine().update(recid, "bb",Serializer.STRING_NOSIZE);
        assertEquals("aa", db2.getEngine().get(recid,Serializer.STRING_NOSIZE));
    }


    @Test public void BTreeMap_snapshot(){
        BTreeMap map =
                DBMaker.memoryDB().transactionDisable().snapshotEnable()
                .make().treeMap("aaa");
        map.put("aa","aa");
        Map map2 = map.snapshot();
        map.put("aa","bb");
        assertEquals("aa",map2.get("aa"));
    }

    @Test public void HTreeMap_snapshot(){
        HTreeMap map =
                DBMaker.memoryDB().transactionDisable().snapshotEnable()
                .make().hashMap("aaa");
        map.put("aa","aa");
        Map map2 = map.snapshot();
        map.put("aa", "bb");
        assertEquals("aa",map2.get("aa"));
    }

//    @Test public void test_stress(){
//        ExecutorService ex = Executors.newCachedThreadPool();
//
//        TxMaker tx = DBMaker.memoryDB().transactionDisable().makeTxMaker();
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
