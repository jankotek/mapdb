package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
    
	@Test public void testCreateSnapshotFor_retruns_same_reference_when_readonly() throws IOException {
        File tmpFile = File.createTempFile("mapdbTest","mapdb");
        DB db = DBMaker.fileDB(tmpFile).make();
        db.close();
        Engine readonlyEngine = DBMaker.fileDB(tmpFile).readOnly().deleteFilesAfterClose().makeEngine();
		Engine snapshot = TxEngine.createSnapshotFor(readonlyEngine);
		assertSame("createSnapshotFor should return passed parameter itself if it is readonly", readonlyEngine,
				snapshot);
	}
    
	
	@Test(expected = UnsupportedOperationException.class)
	public void testCreateSnapshotFor_throws_exception_when_snapshots_disabled() throws IOException {
		Engine nonSnapshottableEngine = DBMaker.memoryDB().makeEngine();
		TxEngine.createSnapshotFor(nonSnapshottableEngine);
		fail("An UnsupportedOperationException should have occurred by now as snaphosts are disabled for the parameter");
	}

	@Test public void testCanSnapshot(){
		assertTrue("TxEngine should be snapshottable", e.canSnapshot());
	}
	
	@Test public void preallocate_get_update_delete_update_get() {
		//test similar to EngineTest#preallocate_get_update_delete_update_get
		long recid = e.preallocate();
		assertNull("There should be no value for preallocated record id", e.get(recid, Serializer.ILLEGAL_ACCESS));
		e.update(recid, 1L, Serializer.LONG);
		assertEquals("Update call should update value at preallocated record id", (Long) 1L,
				e.get(recid, Serializer.LONG));
		e.delete(recid, Serializer.LONG);
		assertNull("Get should return null for a record id whose value was deleted",
				e.get(recid, Serializer.ILLEGAL_ACCESS));
		e.update(recid, 1L, Serializer.LONG);
		assertEquals("Update call should update value at record id with deleted value", (Long) 1L,
				e.get(recid, Serializer.LONG));
	}
	
    @Test public void rollback(){
    	//test similar to EngineTest#rollback
        long recid = e.put("aaa", Serializer.STRING_NOSIZE);
        e.commit();
        e.update(recid, "bbb", Serializer.STRING_NOSIZE);
        e.rollback();
		assertEquals("Uncommitted changes should be rolled back when rollback is called", "aaa",
				e.get(recid, Serializer.STRING_NOSIZE));
    }

}
