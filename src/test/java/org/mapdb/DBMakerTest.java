package org.mapdb;

import org.junit.Test;
import org.mapdb.EngineWrapper.ByteTransformEngine;
import org.mapdb.EngineWrapper.ReadOnlyEngine;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;

import static org.junit.Assert.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class DBMakerTest{

    
    private void verifyDB(DB db) {
        Map m = db.getHashMap("test");
        m.put(1,2);
        assertEquals(2,m.get(1));
    }


    @Test
    public void testNewMemoryDB() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
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
                .writeAheadLogDisable()
                .cacheDisable()
                .make();
        verifyDB(db);
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof SnapshotEngine);
        assertFalse(w.getWrappedEngine().getClass() == CacheHashTable.class);
        assertTrue(w.getWrappedEngine().getClass() == AsyncWriteEngine.class);
    }

    @Test
    public void testDisableAsyncWrite() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
                .asyncWriteDisable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheHashTable.class);
        assertTrue(((CacheHashTable)db.engine).getWrappedEngine().getClass() == SnapshotEngine.class);

    }

    @Test
    public void testMake() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
                .make();
        verifyDB(db);
        //check default values are set
        assertTrue(db.engine.getClass() == CacheHashTable.class);
        assertEquals(1024 * 32, ((CacheHashTable) db.engine).cacheMaxSize);
        EngineWrapper w = (EngineWrapper) ((CacheHashTable) db.engine).getWrappedEngine();
        assertTrue(w instanceof SnapshotEngine);
        assertTrue(w.getWrappedEngine().getClass() == AsyncWriteEngine.class);
        AsyncWriteEngine r = (AsyncWriteEngine) w.getWrappedEngine();
        assertTrue(r.getWrappedEngine() instanceof StoreDirect);

    }

    @Test
    public void testCacheHardRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
                .cacheHardRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheHardRef.class);
    }

    @Test
    public void testCacheWeakRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
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
                .writeAheadLogDisable()
                .cacheSoftRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheWeakSoftRef.class);
        assertFalse(((CacheWeakSoftRef)db.engine).useWeakRef);
    }

    @Test
    public void testCacheLRUEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
                .cacheLRUEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == CacheLRU.class);
        db.close();
    }

    @Test
    public void testCacheSize() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .writeAheadLogDisable()
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
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof SnapshotEngine);

        assertTrue(w.getWrappedEngine() instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)w.getWrappedEngine()).blockSerializer == Serializer.CRC32_CHECKSUM);
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
        ByteTransformEngine e = (ByteTransformEngine) ((EngineWrapper)db.engine).getWrappedEngine();
        assertTrue(e.blockSerializer instanceof EncryptionXTEA);
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
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof SnapshotEngine);
        assertTrue(w.getWrappedEngine() instanceof ByteTransformEngine);
        assertTrue(((ByteTransformEngine)w.getWrappedEngine()).blockSerializer == CompressLZF.SERIALIZER);
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

    @Test public void rafEnableKeepIndexMapped(){
        DB db = DBMaker.newFileDB(Utils.tempDbFile())
                .randomAccessFileEnableKeepIndexMapped()
                .make();
        Engine e = db.getEngine();
        while(e instanceof EngineWrapper)
            e = ((EngineWrapper)e).getWrappedEngine();
        StoreDirect d = (StoreDirect) e;
        assertTrue(d.index instanceof Volume.MappedFileVol);
        assertTrue(d.phys instanceof Volume.FileChannelVol);
    }

}
