package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

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
        Store store = Store.forDB(db);
        Engine w =  db.engine;
        //TODO reenalbe after async is finished
//        assertEquals(w.getWrappedEngine().getClass(),AsyncWriteEngine.class);
    }


    @Test
    public void testMake() throws Exception {
        DB db = DBMaker
                .newFileDB(UtilsTest.tempDbFile())
                .transactionDisable()
                .make();
        verifyDB(db);
        //check default values are set
        Engine w =  db.engine;
        Store store = Store.forDB(db);
        assertNull(store.caches);
        StoreDirect s = (StoreDirect) store;
        assertTrue(s.vol instanceof Volume.FileChannelVol);
    }

    @Test
    public void testCacheHashTableEnable() throws Exception {
        DB db = DBMaker
                .newFileDB(UtilsTest.tempDbFile())
                .cacheHashTableEnable()
                .transactionDisable()
                .make();
        verifyDB(db);
        //check default values are set
        Engine w =  db.engine;
        Store store = Store.forDB(db);
        assertTrue(store.caches[0] instanceof Store.Cache.HashTable);
        assertEquals(1024 * 2, ((Store.Cache.HashTable) store.caches[0]).items.length * store.caches.length);
        StoreDirect s = (StoreDirect) store;
        assertTrue(s.vol instanceof Volume.FileChannelVol);
    }

    @Test
    public void testMakeMapped() throws Exception {
        DB db = DBMaker
                .newFileDB(UtilsTest.tempDbFile())
                .transactionDisable()
                .mmapFileEnable()
                .make();
        verifyDB(db);
        //check default values are set
        Engine w = db.engine;
        Store store = Store.forDB(db);
        StoreDirect s = (StoreDirect) store;
        assertTrue(s.vol instanceof Volume.MappedFileVol);
    }

    @Test
    public void testCacheHardRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheHardRefEnable()
                .make();
        verifyDB(db);
        Store store = Store.forDB(db);
        assertTrue(store.caches[0].getClass() == Store.Cache.HardRef.class);
    }

    @Test
    public void testCacheWeakRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheWeakRefEnable()
                .make();
        verifyDB(db);
        Store store = Store.forDB(db);
        Store.Cache cache = store.caches[0];
        assertTrue(cache.getClass() == Store.Cache.WeakSoftRef.class);
        assertTrue(((Store.Cache.WeakSoftRef)cache).useWeakRef);
    }


    @Test
    public void testCacheSoftRefEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheSoftRefEnable()
                .make();
        verifyDB(db);
        Store store = Store.forDB(db);
        assertTrue(store.caches[0].getClass() == Store.Cache.WeakSoftRef.class);
        assertFalse(((Store.Cache.WeakSoftRef)store.caches[0]).useWeakRef);
    }

    @Test
    public void testCacheLRUEnable() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheLRUEnable()
                .make();
        verifyDB(db);
        Store store = Store.forDB(db);
        assertTrue(store.caches[0].getClass() == Store.Cache.LRU.class);
        db.close();
    }

    @Test
    public void testCacheSize() throws Exception {
        DB db = DBMaker
                .newMemoryDB()
                .transactionDisable()
                .cacheHashTableEnable()
                .cacheSize(1000)
                .make();
        verifyDB(db);
        Store store = Store.forDB(db);
        assertEquals(1024, ((Store.Cache.HashTable) store.caches[0]).items.length*store.caches.length);
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
        assertTrue(db.engine instanceof Engine.ReadOnly);
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

                .checksumEnable()
                .make();
        Engine w = db.engine;
        assertTrue(w instanceof TxEngine);

        Store s = Store.forEngine(w);
        assertTrue(s.checksum);
        assertTrue(!s.compress);
        assertTrue(!s.encrypt);
        db.close();
    }


    @Test public void checksum() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .checksumEnable()
                .make();

        Store s = Store.forDB(db);
        assertTrue(s.checksum);
        assertTrue(!s.compress);
        assertTrue(!s.encrypt);
        db.close();
    }

    @Test public void encrypt() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .encryptionEnable("adqdqwd")
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.encrypt);
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
                .encryptionEnable("adqdqwd")
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(!s.compress);
        assertTrue(s.encrypt);
        db.close();
    }


    @Test public void compress() throws IOException {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker
                .newFileDB(f)
                .deleteFilesAfterClose()
                .compressionEnable()
                .make();
        Store s = Store.forDB(db);
        assertTrue(!s.checksum);
        assertTrue(s.compress);
        assertTrue(!s.encrypt);
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
                .compressionEnable()
                .make();
        Engine w = db.engine;
        assertTrue(w instanceof TxEngine);
        Store s = Store.forEngine(w);
        assertTrue(!s.checksum);
        assertTrue(s.compress);
        assertTrue(!s.encrypt);

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

    @Test(expected = DBException.VolumeIOError.class)
    public void nonExistingFolder(){
        DBMaker.newFileDB(folderDoesNotExist).make();
    }

    @Test(expected = DBException.VolumeIOError.class)
    public void nonExistingFolder3(){
        DBMaker.newFileDB(folderDoesNotExist).mmapFileEnable().make();
    }


    @Test(expected = DBException.VolumeIOError.class)
    public void nonExistingFolder2(){
        DBMaker
                .newFileDB(folderDoesNotExist)
                .snapshotEnable()
                .commitFileSyncDisable()
                .makeTxMaker();
    }

    @Test public void treeset_pump_presert(){
        List unsorted = Arrays.asList(4,7,5,12,9,10,11,0);

        NavigableSet<Integer> s = DBMaker.newMemoryDB().transactionDisable().make()
                .createTreeSet("t")
                .pumpPresort(10)
                .pumpSource(unsorted.iterator())
                .make();

        assertEquals(Integer.valueOf(0),s.first());
        assertEquals(Integer.valueOf(12), s.last());
    }

    @Test public void treemap_pump_presert(){
        List unsorted = Arrays.asList(4,7,5,12,9,10,11,0);

        NavigableMap<Integer,Integer> s = DBMaker.newMemoryDB().transactionDisable().make()
                .createTreeMap("t")
                .pumpPresort(10)
                .pumpSource(unsorted.iterator(), Fun.extractNoTransform())
                .make();

        assertEquals(Integer.valueOf(0),s.firstEntry().getKey());
        assertEquals(Integer.valueOf(12), s.lastEntry().getKey());
    }

    @Test public void heap_store(){
        DB db = DBMaker.newHeapDB().make();
        Engine  s = Store.forDB(db);

        assertTrue(s instanceof StoreHeap);
    }

    @Test public void executor() throws InterruptedException {
        final DB db = DBMaker.newHeapDB().executorEnable().make();
        assertNotNull(db.executor);
        assertFalse(db.executor.isTerminated());

        final AtomicBoolean b = new AtomicBoolean(true);

        Runnable r = new Runnable() {
            @Override
            public void run() {
                while(b.get()) {
                    LockSupport.parkNanos(10);
                }
            }
        };

        db.executor.execute(r);

        final AtomicBoolean closed = new AtomicBoolean();
        new Thread(){
            @Override
            public void run() {
                db.close();
                closed.set(true);
            }
        }.start();

        Thread.sleep(1000);
        assertTrue(db.executor.isShutdown());

        //shutdown the task
        b.set(false);
        Thread.sleep(2000);
        assertTrue(closed.get());
        assertNull(db.executor);
    }

    @Test public void temp_HashMap_standalone(){
        HTreeMap m = DBMaker.newTempHashMap();
        assertTrue(m.closeEngine);
        m.close();
    }

    @Test public void temp_TreeMap_standalone(){
        BTreeMap m = DBMaker.newTempTreeMap();
        assertTrue(m.closeEngine);
        m.close();
    }

    @Test public void temp_HashSet_standalone() throws IOException {
        HTreeMap.KeySet m = (HTreeMap.KeySet) DBMaker.newTempHashSet();
        assertTrue(m.getHTreeMap().closeEngine);
        m.close();
    }

    @Test public void temp_TreeSet_standalone() throws IOException {
        BTreeMap.KeySet m = (BTreeMap.KeySet) DBMaker.newTempTreeSet();
        assertTrue(((BTreeMap)m.m).closeEngine);
        m.close();
    }


    @Test public void metricsLog(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.newMemoryDB()
                .metricsEnable(11111)
                .metricsExecutorEnable(s)
                .make();

        //TODO test task was scheduled with correct interval
        assertTrue(s==db.metricsExecutor);
        assertNull(db.executor);
        db.close();
    }

    @Test public void storeExecutor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.newMemoryDB()
                .storeExecutorPeriod(11111)
                .storeExecutorEnable(s)
                .make();

        //TODO test task was scheduled with correct interval
        assertTrue(s==db.storeExecutor);
        assertNull(db.executor);
        db.close();
    }


    @Test public void cacheExecutor(){
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor();
        DB db = DBMaker.newMemoryDB()
                .cacheExecutorPeriod(11111)
                .cacheExecutorEnable(s)
                .make();

        //TODO test task was scheduled with correct interval
        assertTrue(s==db.cacheExecutor);
        assertNull(db.executor);
        db.close();
    }

}
