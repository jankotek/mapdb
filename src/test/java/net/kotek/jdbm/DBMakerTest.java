package net.kotek.jdbm;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
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

    @Test
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
        assertFalse(db.recman.getClass() == CacheHashTable.class);
        assertTrue(db.recman.getClass() == AsyncWriteWrapper.class);
    }

    @Test
    public void testDisableAsyncWrite() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .asyncWriteDisable()
                .make();
        verifyDB(db);
        assertTrue(db.recman.getClass() == CacheHashTable.class);
        assertTrue(((CacheHashTable)db.recman).recman.getClass() == StorageDirect.class);

    }

    @Test
    public void testDisableAsyncSerialization() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .asyncSerializationDisable()
                .make();
        verifyDB(db);
        assertTrue(db.recman.getClass() == CacheHashTable.class);
        assertTrue(((CacheHashTable)db.recman).recman.getClass() == AsyncWriteWrapper.class);
        AsyncWriteWrapper r = (AsyncWriteWrapper) ((CacheHashTable)db.recman).recman;
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
        assertTrue(db.recman.getClass() == CacheHashTable.class);
        assertEquals(1024 * 32, ((CacheHashTable) db.recman).cacheMaxSize);
        assertTrue(((CacheHashTable)db.recman).recman.getClass() == AsyncWriteWrapper.class);
        AsyncWriteWrapper r = (AsyncWriteWrapper) ((CacheHashTable)db.recman).recman;
        assertTrue(r.asyncSerialization);

    }

    @Test
    public void testCacheHardRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheHardRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.recman.getClass() == CacheHardRef.class);
    }

    @Test
    public void testCacheSize() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        verifyDB(db);
        assertEquals(1000, ((CacheHashTable) db.recman).cacheMaxSize);
    }

    @Test public void read_only() throws IOException {
        File f = File.createTempFile("test", "test");
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .readOnly()
                .transactionDisable()
                .make();
        assertTrue(db.recman instanceof ReadOnlyWrapper);
        db.close();
    }


    @Test public void close_on_jvm_shutdown(){
        DBMaker
                .newTempFileDB()
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
                .make();
    }
}
