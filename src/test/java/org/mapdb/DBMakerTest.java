package org.mapdb;

import org.junit.Test;
import org.mapdb.EngineWrapper.ReadOnlyEngine;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
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
                .transactionDisable()
                .make();
        verifyDB(db);
    }


    @Test
    public void testNewFileDB() throws Exception {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f)
                .transactionDisable().make();
        verifyDB(db);
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
        Store s = Store.forDB(db);
        assertEquals(s.getClass(), StoreDirect.class);
    }

    @Test
    public void testAsyncWriteEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .asyncWriteEnable()
                .make();
        verifyDB(db);
        assertEquals(db.engine.getClass(), Caches.HashTable.class);
        EngineWrapper w = (EngineWrapper) db.engine;
        assertEquals(w.getWrappedEngine().getClass(),AsyncWriteEngine.class);
    }

    @Test
    public void testMake() throws Exception {
        DB db = DBMaker
                .newFileDB(UtilsTest.tempDbFile())
                .transactionDisable()
                .make();
        verifyDB(db);
        //check default values are set
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof Caches.HashTable);
        assertEquals(1024 * 32, ((Caches.HashTable) w).cacheMaxSize);
        StoreDirect s = (StoreDirect) w.getWrappedEngine();
        assertTrue(s.index instanceof Volume.FileChannelVol);
        assertTrue(s.phys instanceof Volume.FileChannelVol);
    }

    @Test
    public void testCacheHardRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheHardRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == Caches.HardRef.class);
    }

    @Test
    public void testCacheWeakRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheWeakRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == Caches.WeakSoftRef.class);
        assertTrue(((Caches.WeakSoftRef)db.engine).useWeakRef);
    }


    @Test
    public void testCacheSoftRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheSoftRefEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == Caches.WeakSoftRef.class);
        assertFalse(((Caches.WeakSoftRef)db.engine).useWeakRef);
    }

    @Test
    public void testCacheLRUEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheLRUEnable()
                .make();
        verifyDB(db);
        assertTrue(db.engine.getClass() == Caches.LRU.class);
        db.close();
    }

    @Test
    public void testCacheSize() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheSize(1000)
                .make();
        verifyDB(db);
        assertEquals(1024, ((Caches.HashTable) db.engine).cacheMaxSize);
    }

    @Test public void read_only() throws IOException {
        File f = UtilsTest.tempDbFile();
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

    @Test(expected = IllegalArgumentException.class)
    public void reopen_wrong_checksum() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()

                .checksumEnable()
                .make();
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof TxEngine);

        Store s = Store.forEngine(w);
        assertTrue(s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.password==null);
        db.close();
    }


    @Test public void checksum() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()

                .checksumEnable()
                .make();

        Store s = Store.forDB(db);
        assertTrue(s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.password==null);
        db.close();
    }

    @Test public void encrypt() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()

                .encryptionEnable("adqdqwd")
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.password!=null);
        db.close();
    }


    @Test(expected = IllegalArgumentException.class)
    public void reopen_wrong_encrypt() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()

                .encryptionEnable("adqdqwd")
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.password!=null);
        db.close();
    }


    @Test public void compress() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()
                .compressionEnable()
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(s.compress);
        assertTrue(s.password==null);
        db.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void reopen_wrong_compress() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB(f).make();
        db.close();
        db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .cacheDisable()

                .compressionEnable()
                .make();
        EngineWrapper w = (EngineWrapper) db.engine;
        assertTrue(w instanceof TxEngine);
        Store s = Store.forEngine(w);
        assertTrue(!s.checksum);
        assertTrue(s.compress);
        assertTrue(s.password==null);

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
        DB db = DBMaker.newFileDB(UtilsTest.tempDbFile())
                .mmapFileEnablePartial()
                .make();
        Engine e = db.getEngine();
        while(e instanceof EngineWrapper)
            e = ((EngineWrapper)e).getWrappedEngine();
        StoreDirect d = (StoreDirect) e;
        assertTrue(d.index instanceof Volume.MappedFileVol);
        assertTrue(d.phys instanceof Volume.FileChannelVol);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void limitDisabledAppend(){
        DBMaker.newAppendFileDB(UtilsTest.tempDbFile()).sizeLimit(1).make();
    }

    @Test()
    public void sizeLimit(){
        long g = 1024*1024*1024;
        assertEquals(g/2,DBMaker.newMemoryDB().sizeLimit(0.5).propsGetLong(DBMaker.Keys.sizeLimit,0));
        assertEquals(g,DBMaker.newMemoryDB().sizeLimit(1).propsGetLong(DBMaker.Keys.sizeLimit,0));
    }


    @Test public void keys_value_matches() throws IllegalAccessException {
        Class c = DBMaker.Keys.class;
        Set<Integer> s = new TreeSet<Integer>();
        for (Field f : c.getDeclaredFields()) {
            f.setAccessible(true);
            String value = (String) f.get(null);

            String expected = f.getName().replaceFirst("^[^_]+_","");
            assertEquals(expected, value);
        }
    }

    File folderDoesNotExist = new File("folder-does-not-exit/db.aaa");

    @Test(expected = IOError.class)
    public void nonExistingFolder(){
        DBMaker.newFileDB(folderDoesNotExist).make();
    }

    public void nonExistingFolder3(){
        DBMaker.newFileDB(folderDoesNotExist).mmapFileEnable().make();
    }


    @Test(expected = IOError.class)
    public void nonExistingFolder2(){
        DBMaker
                .newFileDB(folderDoesNotExist)
                .snapshotEnable()
                .commitFileSyncDisable()
                .makeTxMaker();
    }
}
