package net.kotek.jdbm;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class DBMakerTest{

    @SuppressWarnings("unchecked")
    private void verifyDB(DB db) {
        Map m = db.getHashMap("test");
        m.put(1,2);
        assertEquals(2,m.get(1));
    }


    @Test
    public void testNewMemoryDB() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .make();
        verifyDB(db);
    }


    @Test
    public void testNewFileDB() throws Exception {
        //TODO test with file
    }

    @Test(expected = IllegalAccessError.class)
    public void testDisableTransactions() throws Exception {
        DBMaker.newMemoryDB().make();
    }

    @Test
    public void testDisableCache() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheDisable()
                .make();
        verifyDB(db);
        assertFalse(db.recman.getClass() == RecordHardCache.class);
        assertTrue(db.recman.getClass() == RecordStoreAsyncWrite.class);
    }

    @Test
    public void testDisableAsyncWrite() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .asyncWriteDisable()
                .make();
        verifyDB(db);
        assertTrue(db.recman.getClass() == RecordHardCache.class);
        assertTrue(((RecordHardCache)db.recman).recman.getClass() == RecordStore.class);

    }

    @Test
    public void testDisableAsyncSerialization() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .asyncSerializationDisable()
                .make();
        verifyDB(db);
        assertTrue(db.recman.getClass() == RecordHardCache.class);
        assertTrue(((RecordHardCache)db.recman).recman.getClass() == RecordStoreAsyncWrite.class);
        RecordStoreAsyncWrite r = (RecordStoreAsyncWrite) ((RecordHardCache)db.recman).recman;
        assertFalse(r.asyncSerialization);

    }

    @Test
    public void testMake() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .make();
        verifyDB(db);
        //check default values are set
        assertTrue(db.recman.getClass() == RecordHardCache.class);
        assertTrue(((RecordHardCache)db.recman).recman.getClass() == RecordStoreAsyncWrite.class);
        RecordStoreAsyncWrite r = (RecordStoreAsyncWrite) ((RecordHardCache)db.recman).recman;
        assertTrue(r.asyncSerialization);

    }
}
