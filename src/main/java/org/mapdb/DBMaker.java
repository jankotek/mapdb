/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb;


import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

/**
 * <p>
 * A builder class to creare and open new database and individual collections.
 * It has several static factory methods.
 * Method names depends on type of storage it opens.
 * {@code DBMaker}is typically used this way
 * </p>
 * <pre>
 *  DB db = DBMaker
 *      .memoryDB()          //static method
 *      .transactionDisable()   //configuration option
 *      .make()                 //opens db
 * </pre>
 *
 *
 *
 * @author Jan Kotek
 */
public final class DBMaker{

    protected static final Logger LOG = Logger.getLogger(DBMaker.class.getName());

    protected static final String TRUE = "true";

    protected interface Keys{
        String cache = "cache";

        String cacheSize = "cacheSize";
        String cache_disable = "disable";
        String cache_hashTable = "hashTable";
        String cache_hardRef = "hardRef";
        String cache_softRef = "softRef";
        String cache_weakRef = "weakRef";
        String cache_lru = "lru";
        String cacheExecutorPeriod = "cacheExecutorPeriod";

        String file = "file";

        String metrics = "metrics";
        String metricsLogInterval = "metricsLogInterval";

        String volume = "volume";
        String volume_fileChannel = "fileChannel";
        String volume_raf = "raf";
        String volume_mmapfIfSupported = "mmapfIfSupported";
        String volume_mmapf = "mmapf";
        String volume_byteBuffer = "byteBuffer";
        String volume_directByteBuffer = "directByteBuffer";
        String volume_unsafe = "unsafe";


        String lockScale = "lockScale";

        String lock = "lock";
        String lock_readWrite = "readWrite";
        String lock_single = "single";
        String lock_threadUnsafe = "threadUnsafe";

        String store = "store";
        String store_direct = "direct";
        String store_wal = "wal";
        String store_append = "append";
        String store_heap = "heap";
        String storeExecutorPeriod = "storeExecutorPeriod";

        String transactionDisable = "transactionDisable";

        String asyncWrite = "asyncWrite";
        String asyncWriteFlushDelay = "asyncWriteFlushDelay";
        String asyncWriteQueueSize = "asyncWriteQueueSize";

        String deleteFilesAfterClose = "deleteFilesAfterClose";
        String closeOnJvmShutdown = "closeOnJvmShutdown";

        String readOnly = "readOnly";

        String compression = "compression";
        String compression_lzf = "lzf";

        String encryptionKey = "encryptionKey";
        String encryption = "encryption";
        String encryption_xtea = "xtea";

        String checksum = "checksum";

        String freeSpaceReclaimQ = "freeSpaceReclaimQ";
        String commitFileSyncDisable = "commitFileSyncDisable";

        String snapshots = "snapshots";

        String strictDBGet = "strictDBGet";

        String fullTx = "fullTx";

    }



    /**
     * Creates new in-memory database which stores all data on heap without serialization.
     * This mode should be very fast, but data will affect Garbage Collector the same way as traditional Java Collections.
     */
    public static Maker heapDB(){
        return new Maker()._newHeapDB();
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#heapDB()} */
    public static Maker newHeapDB(){
        return heapDB();
    }



    /**
     * Creates new in-memory database. Changes are lost after JVM exits.
     * This will use HEAP memory so Garbage Collector is affected.
     */
    public static Maker memoryDB(){
        return new Maker()._newMemoryDB();
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#memoryDB()} */
    public static Maker newMemoryDB(){
        return memoryDB();
    }

    /**
     * <p>
     * Creates new in-memory database. Changes are lost after JVM exits.
     * </p><p>
     *
     * This will use DirectByteBuffer outside of HEAP, so Garbage Collector is not affected
     * </p>
     */
    public static Maker memoryDirectDB(){
        return new Maker()._newMemoryDirectDB();
    }


    /** @deprecated method renamed, prefix removed, use {@link DBMaker#memoryDirectDB()} */
    public static Maker newMemoryDirectDB(){
        return memoryDirectDB();
    }


    /**
     * <p>
     * Creates new in-memory database. Changes are lost after JVM exits.
     * </p><p>
     * This will use {@code sun.misc.Unsafe}. It uses direct-memory access and avoids boundary checking.
     * It is bit faster compared to {@code DirectByteBuffer}, but can cause JVM crash in case of error.
     * </p><p>
     * If {@code sun.misc.Unsafe} is not available for some reason, MapDB will log an warning and fallback into
     * {@code DirectByteBuffer} based in-memory store without throwing an exception.
     * </p>
     */
    public static Maker memoryUnsafeDB(){
        return new Maker()._newMemoryUnsafeDB();
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#memoryUnsafeDB()} */
    public static Maker newMemoryUnsafeDB(){
        return memoryUnsafeDB();
    }

    /**
     * Creates or open append-only database stored in file.
     * This database uses format other than usual file db
     *
     * @param file
     * @return maker
     */
    public static Maker appendFileDB(File file) {
        return new Maker()._newAppendFileDB(file);
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#appendFileDB(File)} */
    public static Maker newAppendFileDB(File file) {
        return appendFileDB(file);
    }


    /**
     * <p>
     * Create new BTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * </p><p>
     *
     * Storage is created in temp folder and deleted on JVM shutdown
     * </p>
     */
    public static <K,V> BTreeMap<K,V> tempTreeMap(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .treeMapCreate("temp")
                .closeEngine()
                .make();
    }


    /** @deprecated method renamed, prefix removed, use {@link DBMaker#tempTreeMap()} */
    public static <K,V> BTreeMap<K,V> newTempTreeMap(){
        return tempTreeMap();
    }

    /**
     * <p>
     * Create new HTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * </p><p>
     *
     * Storage is created in temp folder and deleted on JVM shutdown
     * </p>
     */
    public static <K,V> HTreeMap<K,V> tempHashMap(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .hashMapCreate("temp")
                .closeEngine()
                .make();
    }
    /** @deprecated method renamed, prefix removed, use {@link DBMaker#tempHashMap()} */
    public static <K,V> HTreeMap<K,V> newTempHashMap() {
        return tempHashMap();
    }

    /**
     * <p>
     * Create new TreeSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * </p><p>
     *
     * Storage is created in temp folder and deleted on JVM shutdown
     * </p>
     */
    public static <K> NavigableSet<K> tempTreeSet(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .treeSetCreate("temp")
                .standalone()
                .make();
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#tempTreeSet()} */
    public static <K> NavigableSet<K> newTempTreeSet(){
        return tempTreeSet();
    }


    /**
     * <p>
     * Create new HashSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * </p><p>
     *
     * Storage is created in temp folder and deleted on JVM shutdown
     * </p>
     */
    public static <K> Set<K> tempHashSet(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .hashSetCreate("temp")
                .closeEngine()
                .make();
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#tempHashSet()} */
    public static <K> Set<K> newTempHashSet(){
        return tempHashSet();
    }

    /**
     * Creates new database in temporary folder.
     */
    public static Maker tempFileDB() {
        try {
            return newFileDB(File.createTempFile("mapdb-temp","db"));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#tempFileDB()} */
    public static Maker newTempFileDB(){
        return tempFileDB();
    }

    /**
     * Creates new off-heap cache with maximal size in GBs.
     * Entries are removed from cache in most-recently-used fashion
     * if store becomes too big.
     *
     * This method uses off-heap direct ByteBuffers. See {@link java.nio.ByteBuffer#allocateDirect(int)}
     *
     * @param size maximal size of off-heap store in gigabytes.
     * @return map
     *
     * @deprecated TODO this method is going to be replaced by something
     */
    public static <K,V> HTreeMap<K,V> newCacheDirect(double size){
        return DBMaker
                .memoryDirectDB()
                .transactionDisable()
                .make()
                .hashMapCreate("cache")
                .expireStoreSize(size)
                .counterEnable()
                .make();
    }

    /**
     * Creates new cache with maximal size in GBs.
     * Entries are removed from cache in most-recently-used fashion
     * if store becomes too big.
     *
     * This cache uses on-heap {@code byte[]}, but does not affect GC since objects are serialized into binary form.
     * This method uses  ByteBuffers backed by on-heap byte[]. See {@link java.nio.ByteBuffer#allocate(int)}
     *
     * @param size maximal size of off-heap store in gigabytes.
     * @return map
     * @deprecated TODO this method is going to be replaced by something
     */
    public static <K,V> HTreeMap<K,V> newCache(double size){
        return DBMaker
                .memoryDB()
                .transactionDisable()
                .make()
                .hashMapCreate("cache")
                .expireStoreSize(size)
                .counterEnable()
                .make();
    }


    /** Creates or open database stored in file. */
    public static Maker fileDB(File file){
        return new Maker(file);
    }

    /** @deprecated method renamed, prefix removed, use {@link DBMaker#fileDB(File)} */
    public static Maker newFileDB(File file){
        return fileDB(file);
    }


    public static final class Maker {
        protected Fun.RecordCondition cacheCondition;
        protected ScheduledExecutorService executor;
        protected ScheduledExecutorService metricsExecutor;
        protected ScheduledExecutorService cacheExecutor;

        protected ScheduledExecutorService storeExecutor;

        protected Properties props = new Properties();

        /** use static factory methods, or make subclass */
        protected Maker(){}

        protected Maker(File file) {
            props.setProperty(Keys.file, file.getPath());
        }



        public Maker _newHeapDB(){
            props.setProperty(Keys.store,Keys.store_heap);
            return this;
        }

        public Maker _newMemoryDB(){
            props.setProperty(Keys.volume,Keys.volume_byteBuffer);
            return this;
        }

        public  Maker _newMemoryDirectDB() {
            props.setProperty(Keys.volume,Keys.volume_directByteBuffer);
            return this;
        }


        public  Maker _newMemoryUnsafeDB() {
            props.setProperty(Keys.volume,Keys.volume_unsafe);
            return this;
        }


        public Maker _newAppendFileDB(File file) {
            props.setProperty(Keys.file, file.getPath());
            props.setProperty(Keys.store, Keys.store_append);
            return this;
        }



        public Maker _newFileDB(File file){
            props.setProperty(Keys.file, file.getPath());
            return this;
        }



        /**
         * Enables background executor
         *
         * @return this builder
         */
        public Maker executorEnable(){
            executor = Executors.newScheduledThreadPool(4);
            return this;
        }


        /**
         * <p>
         * Transaction journal is enabled by default
         * You must call <b>DB.commit()</b> to save your changes.
         * It is possible to disable transaction journal for better write performance
         * In this case all integrity checks are sacrificed for faster speed.
         * </p><p>
         * If transaction journal is disabled, all changes are written DIRECTLY into store.
         * You must call DB.close() method before exit,
         * otherwise your store <b>WILL BE CORRUPTED</b>
         * </p>
         *
         * @return this builder
         */
        public Maker transactionDisable(){
            props.put(Keys.transactionDisable, TRUE);
            return this;
        }

        /**
         * Enable metrics, log at info level every 10 SECONDS
         *
         * @return this builder
         */
        public Maker metricsEnable(){
            return metricsEnable(CC.DEFAULT_METRICS_LOG_PERIOD);
        }

        public Maker metricsEnable(long metricsLogPeriod) {
            props.put(Keys.metrics, TRUE);
            props.put(Keys.metricsLogInterval, ""+metricsLogPeriod);
            return this;
        }

        /**
         * Enable separate executor for metrics.
         *
         * @return this builder
         */
        public Maker metricsExecutorEnable(){
            return metricsExecutorEnable(
                    Executors.newSingleThreadScheduledExecutor());
        }

        /**
         * Enable separate executor for metrics.
         *
         * @return this builder
         */
        public Maker metricsExecutorEnable(ScheduledExecutorService metricsExecutor){
            this.metricsExecutor = metricsExecutor;
            return this;
        }

        /**
         * Enable separate executor for cache.
         *
         * @return this builder
         */
        public Maker cacheExecutorEnable(){
            return cacheExecutorEnable(
                    Executors.newSingleThreadScheduledExecutor());
        }

        /**
         * Enable separate executor for cache.
         *
         * @return this builder
         */
        public Maker cacheExecutorEnable(ScheduledExecutorService metricsExecutor){
            this.cacheExecutor = metricsExecutor;
            return this;
        }

        /**
         * Sets interval in which executor should check cache
         *
         * @param period in ms
         * @return this builder
         */
        public Maker cacheExecutorPeriod(long period){
            props.put(Keys.cacheExecutorPeriod, ""+period);
            return this;
        }


        /**
         * Enable separate executor for store (async write, compaction)
         *
         * @return this builder
         */
        public Maker storeExecutorEnable(){
            return storeExecutorEnable(
                    Executors.newScheduledThreadPool(4));
        }

        /**
         * Enable separate executor for cache.
         *
         * @return this builder
         */
        public Maker storeExecutorEnable(ScheduledExecutorService metricsExecutor){
            this.storeExecutor = metricsExecutor;
            return this;
        }

        /**
         * Sets interval in which executor should check cache
         *
         * @param period in ms
         * @return this builder
         */
        public Maker storeExecutorPeriod(long period){
            props.put(Keys.storeExecutorPeriod, ""+period);
            return this;
        }


        /**
         * Install callback condition, which decides if some record is to be included in cache.
         * Condition should return {@code true} for every record which should be included
         *
         * This could be for example useful to include only BTree Directory Nodes and leave values and Leaf nodes outside of cache.
         *
         * !!! Warning:!!!
         *
         * Cache requires **consistent** true or false. Failing to do so will result in inconsitent cache and possible data corruption.

         * Condition is also executed several times, so it must be very fast
         *
         * You should only use very simple logic such as {@code value instanceof SomeClass}.
         *
         * @return this builder
         */
        public Maker cacheCondition(Fun.RecordCondition cacheCondition){
            this.cacheCondition = cacheCondition;
            return this;
        }

        /**

         /**
         * Disable cache if enabled. Cache is disabled by default, so this method has no longer purpose.
         *
         * @return this builder
         * @deprecated cache is disabled by default
         */

        public Maker cacheDisable(){
            props.put(Keys.cache,Keys.cache_disable);
            return this;
        }

        /**
         * <p>
         * Enables unbounded hard reference cache.
         * This cache is good if you have lot of available memory.
         * </p><p>
         *
         * All fetched records are added to HashMap and stored with hard reference.
         * To prevent OutOfMemoryExceptions MapDB monitors free memory,
         * if it is bellow 25% cache is cleared.
         * </p>
         *
         * @return this builder
         */
        public Maker cacheHardRefEnable(){
            props.put(Keys.cache, Keys.cache_hardRef);
            return this;
        }


        /**
         * <p>
         * Set cache size. Interpretations depends on cache type.
         * For fixed size caches (such as FixedHashTable cache) it is maximal number of items in cache.
         * </p><p>
         *
         * For unbounded caches (such as HardRef cache) it is initial capacity of underlying table (HashMap).
         * <p></p>
         *
         * Default cache size is 2048.
         * <p>
         *
         * @param cacheSize new cache size
         * @return this builder
         */
        public Maker cacheSize(int cacheSize){
            props.setProperty(Keys.cacheSize, "" + cacheSize);
            return this;
        }

        /**
         * <p>
         * Fixed size cache which uses hash table.
         * Is thread-safe and requires only minimal locking.
         * Items are randomly removed and replaced by hash collisions.
         * </p><p>
         *
         * This is simple, concurrent, small-overhead, random cache.
         * </p>
         *
         * @return this builder
         */
        public Maker cacheHashTableEnable(){
            props.put(Keys.cache, Keys.cache_hashTable);
            return this;
        }


        /**
         * <p>
         * Fixed size cache which uses hash table.
         * Is thread-safe and requires only minimal locking.
         * Items are randomly removed and replaced by hash collisions.
         * </p><p>
         *
         * This is simple, concurrent, small-overhead, random cache.
         * </p>
         *
         * @param cacheSize new cache size
         * @return this builder
         */
        public Maker cacheHashTableEnable(int cacheSize){
            props.put(Keys.cache, Keys.cache_hashTable);
            props.setProperty(Keys.cacheSize, "" + cacheSize);
            return this;
        }

        /**
         * Enables unbounded cache which uses <code>WeakReference</code>.
         * Items are removed from cache by Garbage Collector
         *
         * @return this builder
         */
        public Maker cacheWeakRefEnable(){
            props.put(Keys.cache, Keys.cache_weakRef);
            return this;
        }

        /**
         * Enables unbounded cache which uses <code>SoftReference</code>.
         * Items are removed from cache by Garbage Collector
         *
         * @return this builder
         */
        public Maker cacheSoftRefEnable(){
            props.put(Keys.cache,Keys.cache_softRef);
            return this;
        }

        /**
         * Enables Least Recently Used cache. It is fixed size cache and it removes less used items to make space.
         *
         * @return this builder
         */
        public Maker cacheLRUEnable(){
            props.put(Keys.cache,Keys.cache_lru);
            return this;
        }

        /**
         * <p>
         * Disable locks. This will make MapDB thread unsafe. It will also disable any background thread workers.
         * </p><p>
         *
         * <b>WARNING: </b> this option is dangerous. With locks disabled multi-threaded access could cause data corruption and causes.
         * MapDB does not have fail-fast iterator or any other means of protection
         * </p>
         *
         * @return this builder
         */
        public Maker lockDisable() {
            props.put(Keys.lock, Keys.lock_threadUnsafe);
            return this;
        }

        /**
         * <p>
         * Disables double read-write locks and enables single read-write locks.
         * </p><p>
         *
         * This type of locking have smaller overhead and can be faster in mostly-write scenario.
         * </p>
         * @return this builder
         */
        public Maker lockSingleEnable() {
            props.put(Keys.lock, Keys.lock_single);
            return this;
        }


        /**
         * <p>
         * Sets concurrency scale. More locks means better scalability with multiple cores, but also higher memory overhead
         * </p><p>
         *
         * This value has to be power of two, so it is rounded up automatically.
         * </p>
         *
         * @return this builder
         */
        public Maker lockScale(int scale) {
            props.put(Keys.lockScale, "" + scale);
            return this;
        }


        /**
         *@deprecated renamed to {@link #fileMmapEnable()}
         */
        public Maker mmapFileEnable() {
            return fileMmapEnable();
        }


        /**
         * <p>
         * Enables Memory Mapped Files, much faster storage option. However on 32bit JVM this mode could corrupt
         * your DB thanks to 4GB memory addressing limit.
         * </p><p>
         *
         * You may experience {@code java.lang.OutOfMemoryError: Map failed} exception on 32bit JVM, if you enable this
         * mode.
         * </p>
         */
        public Maker fileMmapEnable() {
            assertNotInMemoryVolume();
            props.setProperty(Keys.volume,Keys.volume_mmapf);
            return this;
        }

        private void assertNotInMemoryVolume() {
            if(Keys.volume_byteBuffer.equals(props.getProperty(Keys.volume)) ||
                    Keys.volume_directByteBuffer.equals(props.getProperty(Keys.volume)))
                throw new IllegalArgumentException("Can not enable mmap file for in-memory store");
        }

        /**
         *
         * @return this
         * @deprecated mapdb 2.0 uses single file, no partial mapping possible
         */
        public Maker mmapFileEnablePartial() {
            return this;
        }

        /**
         * Enable Memory Mapped Files only if current JVM supports it (is 64bit).
         * @deprecated renamed to {@link #fileMmapEnableIfSupported()}
         */
        public Maker mmapFileEnableIfSupported() {
            return fileMmapEnableIfSupported();
        }

        /**
         * Enable Memory Mapped Files only if current JVM supports it (is 64bit).
         */
        public Maker fileMmapEnableIfSupported() {
            assertNotInMemoryVolume();
            props.setProperty(Keys.volume,Keys.volume_mmapfIfSupported);
            return this;
        }

        /**
         * Enable FileChannel access. By default MapDB uses {@link java.io.RandomAccessFile}.
         * whic is slower and more robust. but does not allow concurrent access (parallel read and writes). RAF is still thread-safe
         * but has global lock.
         * FileChannel does not have global lock, and is faster compared to RAF. However memory-mapped files are
         * probably best choice.
         */
        public Maker fileChannelEnable() {
            assertNotInMemoryVolume();
            props.setProperty(Keys.volume,Keys.volume_fileChannel);
            return this;
        }


        /**
         * MapDB supports snapshots. {@code TxEngine} requires additional locking which has small overhead when not used.
         * Snapshots are disabled by default. This option switches the snapshots on.
         *
         * @return this builder
         */
        public Maker snapshotEnable(){
            props.setProperty(Keys.snapshots,TRUE);
            return this;
        }


        /**
         * <p>
         * Enables mode where all modifications are queued and written into disk on Background Writer Thread.
         * So all modifications are performed in asynchronous mode and do not block.
         * </p><p>
         *
         * Enabling this mode might increase performance for single threaded apps.
         * </p>
         *
         * @return this builder
         */
        public Maker asyncWriteEnable(){
            props.setProperty(Keys.asyncWrite,TRUE);
            return this;
        }



        /**
         * <p>
         * Set flush interval for write cache, by default is 0
         * </p><p>
         * When BTreeMap is constructed from ordered set, tree node size is increasing linearly with each
         * item added. Each time new key is added to tree node, its size changes and
         * storage needs to find new place. So constructing BTreeMap from ordered set leads to large
         * store fragmentation.
         * </p><p>
         *
         * Setting flush interval is workaround as BTreeMap node is always updated in memory (write cache)
         * and only final version of node is stored on disk.
         * </p>
         *
         * @param delay flush write cache every N miliseconds
         * @return this builder
         */
        public Maker asyncWriteFlushDelay(int delay){
            props.setProperty(Keys.asyncWriteFlushDelay,""+delay);
            return this;
        }

        /**
         * <p>
         * Set size of async Write Queue. Default size is
         * </p><p>
         * Using too large queue size can lead to out of memory exception.
         * </p>
         *
         * @param queueSize of queue
         * @return this builder
         */
        public Maker asyncWriteQueueSize(int queueSize){
            props.setProperty(Keys.asyncWriteQueueSize,""+queueSize);
            return this;
        }


        /**
         * Try to delete files after DB is closed.
         * File deletion may silently fail, especially on Windows where buffer needs to be unmapped file delete.
         *
         * @return this builder
         */
        public Maker deleteFilesAfterClose(){
            props.setProperty(Keys.deleteFilesAfterClose,TRUE);
            return this;
        }

        /**
         * Adds JVM shutdown hook and closes DB just before JVM;
         *
         * @return this builder
         */
        public Maker closeOnJvmShutdown(){
            props.setProperty(Keys.closeOnJvmShutdown,TRUE);
            return this;
        }

        /**
         * <p>
         * Enables record compression.
         * </p><p>
         * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
         * </p>
         *
         * @return this builder
         */
        public Maker compressionEnable(){
            props.setProperty(Keys.compression,Keys.compression_lzf);
            return this;
        }


        /**
         * <p>
         * Encrypt storage using XTEA algorithm.
         * </p><p>
         * XTEA is sound encryption algorithm. However implementation in MapDB was not peer-reviewed.
         * MapDB only encrypts records data, so attacker may see number of records and their sizes.
         * </p><p>
         * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
         * </p>
         *
         * @param password for encryption
         * @return this builder
         */
        public Maker encryptionEnable(String password){
            return encryptionEnable(password.getBytes(Charset.forName("UTF8")));
        }



        /**
         * <p>
         * Encrypt storage using XTEA algorithm.
         * </p><p>
         * XTEA is sound encryption algorithm. However implementation in MapDB was not peer-reviewed.
         * MapDB only encrypts records data, so attacker may see number of records and their sizes.
         * </p><p>
         * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
         * </p>
         *
         * @param password for encryption
         * @return this builder
         */
        public Maker encryptionEnable(byte[] password){
            props.setProperty(Keys.encryption, Keys.encryption_xtea);
            props.setProperty(Keys.encryptionKey, DataIO.toHexa(password));
            return this;
        }


        /**
         * <p>
         * Adds CRC32 checksum at end of each record to check data integrity.
         * It throws 'IOException("Checksum does not match, data broken")' on de-serialization if data are corrupted
         * </p><p>
         * Make sure you enable this every time you reopen store, otherwise record de-serialization fails.
         * </p>
         *
         * @return this builder
         */
        public Maker checksumEnable(){
            props.setProperty(Keys.checksum,TRUE);
            return this;
        }


        /**
         * <p>
         * DB Get methods such as {@link DB#treeMap(String)} or {@link DB#atomicLong(String)} auto create
         * new record with default values, if record with given name does not exist. This could be problem if you would like to enforce
         * stricter database schema. So this parameter disables record auto creation.
         * </p><p>
         *
         * If this set, {@code DB.getXX()} will throw an exception if given name does not exist, instead of creating new record (or collection)
         * </p>
         *
         * @return this builder
         */
        public Maker strictDBGet(){
            props.setProperty(Keys.strictDBGet,TRUE);
            return this;
        }




        /**
         * Open store in read-only mode. Any modification attempt will throw
         * <code>UnsupportedOperationException("Read-only")</code>
         *
         * @return this builder
         */
        public Maker readOnly(){
            props.setProperty(Keys.readOnly,TRUE);
            return this;
        }

        /**
         * @deprecated right now not implemented, will be renamed to allocate*()
         * @param maxSize
         * @return this
         */
        public Maker sizeLimit(double maxSize){
            return this;
        }



        /**
         * Set free space reclaim Q.  It is value from 0 to 10, indicating how eagerly MapDB
         * searchs for free space inside store to reuse, before expanding store file.
         * 0 means that no free space will be reused and store file will just grow (effectively append only).
         * 10 means that MapDB tries really hard to reuse free space, even if it may hurt performance.
         * Default value is 5;
         *
         * @return this builder
         *
         * @deprecated ignored in MapDB 2 for now
         */
        public Maker freeSpaceReclaimQ(int q){
            if(q<0||q>10) throw new IllegalArgumentException("wrong Q");
            props.setProperty(Keys.freeSpaceReclaimQ,""+q);
            return this;
        }


        /**
         * Disables file sync on commit. This way transactions are preserved (rollback works),
         * but commits are not 'durable' and data may be lost if store is not properly closed.
         * File store will get properly synced when closed.
         * Disabling this will make commits faster.
         *
         * @return this builder
         * @deprecated ignored in MapDB 2 for now
         */
        public Maker commitFileSyncDisable(){
            props.setProperty(Keys.commitFileSyncDisable,TRUE);
            return this;
        }



        /** constructs DB using current settings */
        public DB make(){
            boolean strictGet = propsGetBool(Keys.strictDBGet);
            boolean deleteFilesAfterClose = propsGetBool(Keys.deleteFilesAfterClose);
            Engine engine = makeEngine();
            boolean dbCreated = false;
            boolean metricsLog = propsGetBool(Keys.metrics);
            long metricsLogInterval = propsGetLong(Keys.metricsLogInterval, metricsLog ? CC.DEFAULT_METRICS_LOG_PERIOD : 0);
            ScheduledExecutorService metricsExec2 = metricsLog? (metricsExecutor==null? executor:metricsExecutor) : null;

            try{
                DB db =  new  DB(
                        engine,
                        strictGet,
                        deleteFilesAfterClose,
                        executor,
                        false,
                        metricsExec2,
                        metricsLogInterval,
                        storeExecutor,
                        cacheExecutor);
                dbCreated = true;
                return db;
            }finally {
                //did db creation fail? in that case close engine to unlock files
                if(!dbCreated)
                    engine.close();
            }
        }


        public TxMaker makeTxMaker(){
            props.setProperty(Keys.fullTx,TRUE);
            snapshotEnable();
            Engine e = makeEngine();
            //init catalog if needed
            DB db = new DB(e);
            db.commit();
            return new TxMaker(e, propsGetBool(Keys.strictDBGet), propsGetBool(Keys.snapshots), executor);
        }

        /** constructs Engine using current settings */
        public Engine makeEngine(){

            if(storeExecutor==null) {
                storeExecutor = executor;
            }


            final boolean readOnly = propsGetBool(Keys.readOnly);
            final String file = props.containsKey(Keys.file)? props.getProperty(Keys.file):"";
            final String volume = props.getProperty(Keys.volume);
            final String store = props.getProperty(Keys.store);

            if(readOnly && file.isEmpty())
                throw new UnsupportedOperationException("Can not open in-memory DB in read-only mode.");

            if(readOnly && !new File(file).exists() && !Keys.store_append.equals(store)){
                throw new UnsupportedOperationException("Can not open non-existing file in read-only mode.");
            }


            Engine engine;
            int lockingStrategy = 0;
            String lockingStrategyStr = props.getProperty(Keys.lock,Keys.lock_readWrite);
            if(Keys.lock_single.equals(lockingStrategyStr)){
                lockingStrategy = 1;
            }else if(Keys.lock_threadUnsafe.equals(lockingStrategyStr)) {
                lockingStrategy = 2;
            }

            final int lockScale = DataIO.nextPowTwo(propsGetInt(Keys.lockScale,CC.DEFAULT_LOCK_SCALE));

            boolean cacheLockDisable = lockingStrategy!=0;
            byte[] encKey = propsGetXteaEncKey();
            final boolean snapshotEnabled =  propsGetBool(Keys.snapshots);
            if(Keys.store_heap.equals(store)){
                engine = new StoreHeap(propsGetBool(Keys.transactionDisable),lockScale,lockingStrategy,snapshotEnabled);
            }else  if(Keys.store_append.equals(store)){
                if(Keys.volume_byteBuffer.equals(volume)||Keys.volume_directByteBuffer.equals(volume))
                    throw new UnsupportedOperationException("Append Storage format is not supported with in-memory dbs");

                Volume.VolumeFactory volFac = extendStoreVolumeFactory(false);
                engine = new StoreAppend(
                        file,
                        volFac,
                        createCache(cacheLockDisable,lockScale),
                        lockScale,
                        lockingStrategy,
                        propsGetBool(Keys.checksum),
                        Keys.compression_lzf.equals(props.getProperty(Keys.compression)),
                        encKey,
                        propsGetBool(Keys.readOnly),
                        snapshotEnabled,
                        propsGetBool(Keys.transactionDisable),
                        storeExecutor
                );
            }else{
                Volume.VolumeFactory volFac = extendStoreVolumeFactory(false);
                boolean compressionEnabled = Keys.compression_lzf.equals(props.getProperty(Keys.compression));
                boolean asyncWrite = propsGetBool(Keys.asyncWrite) && !readOnly;
                boolean txDisable = propsGetBool(Keys.transactionDisable);

                if(!txDisable){
                    engine = new StoreWAL(
                            file,
                            volFac,
                            createCache(cacheLockDisable,lockScale),
                            lockScale,
                            lockingStrategy,
                            propsGetBool(Keys.checksum),
                            compressionEnabled,
                            encKey,
                            propsGetBool(Keys.readOnly),
                            snapshotEnabled,
                            propsGetInt(Keys.freeSpaceReclaimQ, CC.DEFAULT_FREE_SPACE_RECLAIM_Q),
                            propsGetBool(Keys.commitFileSyncDisable),
                            0,
                            storeExecutor,
                            CC.DEFAULT_STORE_EXECUTOR_SCHED_RATE,
                            propsGetInt(Keys.asyncWriteQueueSize,CC.DEFAULT_ASYNC_WRITE_QUEUE_SIZE)
                    );
                }else if(asyncWrite) {
                    engine = new StoreCached(
                            file,
                            volFac,
                            createCache(cacheLockDisable, lockScale),
                            lockScale,
                            lockingStrategy,
                            propsGetBool(Keys.checksum),
                            compressionEnabled,
                            encKey,
                            propsGetBool(Keys.readOnly),
                            snapshotEnabled,
                            propsGetInt(Keys.freeSpaceReclaimQ, CC.DEFAULT_FREE_SPACE_RECLAIM_Q),
                            propsGetBool(Keys.commitFileSyncDisable),
                            0,
                            storeExecutor,
                            CC.DEFAULT_STORE_EXECUTOR_SCHED_RATE,
                            propsGetInt(Keys.asyncWriteQueueSize,CC.DEFAULT_ASYNC_WRITE_QUEUE_SIZE)
                    );
                }else{
                    engine = new StoreDirect(
                            file,
                            volFac,
                            createCache(cacheLockDisable, lockScale),
                            lockScale,
                            lockingStrategy,
                            propsGetBool(Keys.checksum),
                            compressionEnabled,
                            encKey,
                            propsGetBool(Keys.readOnly),
                            snapshotEnabled,
                            propsGetInt(Keys.freeSpaceReclaimQ, CC.DEFAULT_FREE_SPACE_RECLAIM_Q),
                            propsGetBool(Keys.commitFileSyncDisable),
                            0,
                            storeExecutor);
                }
            }

            if(engine instanceof Store){
                ((Store)engine).init();
            }


            if(propsGetBool(Keys.fullTx))
                engine = extendSnapshotEngine(engine, lockScale);

            engine = extendWrapSnapshotEngine(engine);

            if(readOnly)
                engine = new Engine.ReadOnlyWrapper(engine);


            if(propsGetBool(Keys.closeOnJvmShutdown)){
                engine = new Engine.CloseOnJVMShutdown(engine);
            }


            //try to readrt one record from DB, to make sure encryption and compression are correctly set.
            Fun.Pair<Integer,byte[]> check = null;
            try{
                check = (Fun.Pair<Integer, byte[]>) engine.get(Engine.RECID_RECORD_CHECK, Serializer.BASIC);
                if(check!=null){
                    if(check.a != Arrays.hashCode(check.b))
                        throw new RuntimeException("invalid checksum");
                }
            }catch(Throwable e){
                throw new DBException.WrongConfig("Error while opening store. Make sure you have right password, compression or encryption is well configured.",e);
            }
            if(check == null && !engine.isReadOnly()){
                //new db, so insert testing record
                byte[] b = new byte[127];
                if(encKey!=null) {
                    new SecureRandom().nextBytes(b);
                } else {
                    new Random().nextBytes(b);
                }
                check = new Fun.Pair(Arrays.hashCode(b), b);
                engine.update(Engine.RECID_RECORD_CHECK, check, Serializer.BASIC);
                engine.commit();
            }


            return engine;
        }

        protected Store.Cache createCache(boolean disableLocks, int lockScale) {
            final String cache = props.getProperty(Keys.cache, CC.DEFAULT_CACHE);
            if(cacheExecutor==null) {
                cacheExecutor = executor;
            }

            long executorPeriod = propsGetLong(Keys.cacheExecutorPeriod, CC.DEFAULT_CACHE_EXECUTOR_PERIOD);

            if(Keys.cache_disable.equals(cache)){
                return null;
            }else if(Keys.cache_hashTable.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.HashTable(cacheSize,disableLocks);
            }else if (Keys.cache_hardRef.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.HardRef(cacheSize,disableLocks,cacheExecutor, executorPeriod);
            }else if (Keys.cache_weakRef.equals(cache)){
                return new Store.Cache.WeakSoftRef(true, disableLocks, cacheExecutor, executorPeriod);
            }else if (Keys.cache_softRef.equals(cache)){
                return new Store.Cache.WeakSoftRef(false, disableLocks, cacheExecutor,executorPeriod);
            }else if (Keys.cache_lru.equals(cache)){
                int cacheSize = propsGetInt(Keys.cacheSize, CC.DEFAULT_CACHE_SIZE) / lockScale;
                return new Store.Cache.LRU(cacheSize,disableLocks);
            }else{
                throw new IllegalArgumentException("unknown cache type: "+cache);
            }
        }


        protected int propsGetInt(String key, int defValue){
            String ret = props.getProperty(key);
            if(ret==null) return defValue;
            return Integer.valueOf(ret);
        }

        protected long propsGetLong(String key, long defValue){
            String ret = props.getProperty(key);
            if(ret==null) return defValue;
            return Long.valueOf(ret);
        }


        protected boolean propsGetBool(String key){
            String ret = props.getProperty(key);
            return ret!=null && ret.equals(TRUE);
        }

        protected byte[] propsGetXteaEncKey(){
            if(!Keys.encryption_xtea.equals(props.getProperty(Keys.encryption)))
                return null;
            return DataIO.fromHexa(props.getProperty(Keys.encryptionKey));
        }

        /**
         * Check if large files can be mapped into memory.
         * For example 32bit JVM can only address 2GB and large files can not be mapped,
         * so for 32bit JVM this function returns false.
         *
         */
        protected static boolean JVMSupportsLargeMappedFiles() {
            String prop = System.getProperty("os.arch");
            if(prop!=null && prop.contains("64")) return true;
            //TODO better check for 32bit JVM
            return false;
        }


        protected int propsGetRafMode(){
            String volume = props.getProperty(Keys.volume);
            if(volume==null||Keys.volume_raf.equals(volume)){
                return 2;
            }else if(Keys.volume_mmapfIfSupported.equals(volume)){
                return JVMSupportsLargeMappedFiles()?0:2;
                //TODO clear mmap values
//        }else if(Keys.volume_mmapfPartial.equals(volume)){
//            return 1;
            }else if(Keys.volume_fileChannel.equals(volume)){
                return 3;
            }else if(Keys.volume_mmapf.equals(volume)){
                return 0;
            }
            return 2; //default option is RAF
        }


        protected Engine extendSnapshotEngine(Engine engine, int lockScale) {
            return new TxEngine(engine,propsGetBool(Keys.fullTx), lockScale);
        }



        protected Engine extendWrapSnapshotEngine(Engine engine) {
            return engine;
        }


        protected Volume.VolumeFactory  extendStoreVolumeFactory(boolean index) {
            String volume = props.getProperty(Keys.volume);
            if(Keys.volume_byteBuffer.equals(volume))
                return Volume.ByteArrayVol.FACTORY;
            else if(Keys.volume_directByteBuffer.equals(volume))
                return Volume.MemoryVol.FACTORY;
            else if(Keys.volume_unsafe.equals(volume))
                return Volume.UNSAFE_VOL_FACTORY;

            int rafMode = propsGetRafMode();
            if(rafMode == 3)
                return Volume.FileChannelVol.FACTORY;
            boolean raf = rafMode!=0;
            if(raf && index && rafMode==1)
                raf = false;

            return raf?
                    Volume.RandomAccessFileVol.FACTORY:
                    Volume.MappedFileVol.FACTORY;
        }

    }


    public static DB.HTreeMapMaker hashMapSegmented(DBMaker.Maker maker){
        maker = maker
                .lockScale(1)
                        //TODO with some caches enabled, this will become thread unsafe
                .lockDisable()
                .transactionDisable();


        DB db = maker.make();
        Engine[] engines = new Engine[HTreeMap.SEG];
        engines[0] = db.engine;
        for(int i=1;i<HTreeMap.SEG;i++){
            engines[i] = maker.makeEngine();
        }
        return new DB.HTreeMapMaker(db,"hashMapSegmented", engines)
                .closeEngine();
    }

    public static DB.HTreeMapMaker hashMapSegmentedMemory(){
        return hashMapSegmented(
                DBMaker.memoryDB()
        );
    }

    public static DB.HTreeMapMaker hashMapSegmentedMemoryDirect(){
        return hashMapSegmented(
                DBMaker.memoryDirectDB()
        );
    }

}
