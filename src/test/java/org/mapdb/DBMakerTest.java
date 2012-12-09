package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

import static org.junit.Assert.*;
import org.mapdb.EngineWrapper.*;

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
                .journalDisable()
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
                .journalDisable()
                .cacheDisable()
                .make();
        verifyDB(db);
        assertFalse(db.engine.getClass() == CacheHashTable.class);
        assertTrue(db.engine.getClass() == AsyncWriteEngine.class);
    }

    @Test
    public void testDisableAsyncWrite() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .asyncWriteDisable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheHashTable.class);
        assertTrue(((CacheHashTable)db.engine).engine.getClass() == StorageDirect.class);

    }

    @Test
    public void testDisableAsyncSerialization() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .asyncSerializationDisable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheHashTable.class);
        assertTrue(((CacheHashTable)db.engine).engine.getClass() == AsyncWriteEngine.class);
        AsyncWriteEngine r = (AsyncWriteEngine) ((CacheHashTable)db.engine).engine;
        assertFalse(r.asyncSerialization);

    }

    @Test
    public void testMake() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .make();
        verifyDB(db);
        //check default values are set
        assertTrue(db.engine.getClass() == CacheHashTable.class);
        assertEquals(1024 * 32, ((CacheHashTable) db.engine).cacheMaxSize);
        assertTrue(((CacheHashTable)db.engine).engine.getClass() == AsyncWriteEngine.class);
        AsyncWriteEngine r = (AsyncWriteEngine) ((CacheHashTable)db.engine).engine;
        assertTrue(r.asyncSerialization);

    }

    @Test
    public void testCacheHardRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .cacheHardRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheHardRef.class);
    }

    @Test
    public void testCacheWeakRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .cacheWeakRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheWeakSoftRef.class);
        assertTrue(((CacheWeakSoftRef)db.engine).useWeakRef);
    }


    @Test
    public void testCacheSoftRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .cacheSoftRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheWeakSoftRef.class);
        assertFalse(((CacheWeakSoftRef)db.engine).useWeakRef);
    }

    @Test
    public void testCacheSize() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .journalDisable()
                .cacheSize(1000)
                .make();
        verifyDB(db);
        assertEquals(1000, ((CacheHashTable) db.engine).cacheMaxSize);
    }

    @Test public void read_only() throws IOException {
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .readOnly()
                .make();
        assertTrue(db.engine instanceof ReadOnlyEngine);
        db.close();
    }

    @Test public void crc32() throws IOException {
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .asyncWriteDisable()
                .cacheDisable()

                .checksumEnable()
                .make();
        assertTrue(db.engine instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)db.engine).blockSerializer == Serializer.CRC32_CHECKSUM);
        db.close();
    }

    @Test public void encrypt() throws IOException {
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()
                .asyncWriteDisable()

                .encryptionEnable("adqdqwd")
                .make();
        assertTrue(db.engine instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)db.engine).blockSerializer instanceof EncryptionXTEA);
        db.close();
    }

    @Test public void compress() throws IOException {
        File f = Utils.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .asyncWriteDisable()
                .cacheDisable()

                .compressionEnable()
                .make();
        assertTrue(db.engine instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)db.engine).blockSerializer instanceof CompressLZFSerializer);
        db.close();
    }




    @Test public void close_on_jvm_shutdown(){
        DBMaker
                .newTempFileDB()
                .closeOnJvmShutdown()
                .deleteFilesAfterClose()
                .make();
    }

    @Test public void tempTreeMap(){
        ConcurrentNavigableMap<Long,String> m = DBMaker.newTempTreeMap();
        m.put(111L,"wfjie");
        assertTrue(m.getClass().getName().contains("BTreeMap"));
    }

    @Test public void tempHashMap(){
        ConcurrentMap<Long,String> m = DBMaker.newTempHashMap();
        m.put(111L,"wfjie");
        assertTrue(m.getClass().getName().contains("HTreeMap"));
    }

    @Test public void tempHashSet(){
        Set<Long> m = DBMaker.newTempHashSet();
        m.add(111L);
        assertTrue(m.getClass().getName().contains("HTreeMap"));
    }

    @Test public void tempTreeSet(){
        NavigableSet<Long> m = DBMaker.newTempTreeSet();
        m.add(111L);
        assertTrue(m.getClass().getName().contains("BTreeMap"));
    }

}
