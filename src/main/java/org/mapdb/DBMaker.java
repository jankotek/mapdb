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

import org.mapdb.EngineWrapper.ReadOnlyEngine;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Set;

/**
 * A builder class for creating and opening a database.
 *
 * @author Jan Kotek
 */
public class DBMaker<DBMakerT extends DBMaker<DBMakerT>> {

    protected static final byte CACHE_DISABLE = 0;
    protected static final byte CACHE_FIXED_HASH_TABLE = 1;
    protected static final byte CACHE_HARD_REF = 2;
    protected static final byte CACHE_WEAK_REF = 3;
    protected static final byte CACHE_SOFT_REF = 4;
    protected static final byte CACHE_LRU = 5;

    protected byte _cache = CACHE_FIXED_HASH_TABLE;
    protected int _cacheSize = 1024*32;

    /** file to open, if null opens in memory store */
    protected File _file;

    protected boolean _transactionEnabled = true;

    protected boolean _asyncWriteEnabled = true;
    protected int _asyncFlushDelay = CC.ASYNC_WRITE_FLUSH_DELAY;

    protected boolean _deleteFilesAfterClose = false;
    protected boolean _readOnly = false;
    protected boolean _closeOnJvmShutdown = false;

    protected boolean _compressionEnabled = false;

    protected byte[] _xteaEncryptionKey = null;

    protected int _freeSpaceReclaimQ = 5;

    protected boolean _checksumEnabled = false;

    protected boolean _ifInMemoryUseDirectBuffer = false;

    protected boolean _syncOnCommitDisabled = false;

    protected boolean _snapshotDisabled = false;

    protected long _sizeLimit = 0;

    protected int _rafMode = 0;

    protected boolean _strictDBGet = false;

    protected boolean _appendStorage;

    protected boolean _fullTx = false;

    /** use static factory methods, or make subclass */
    protected DBMaker(){}

    protected DBMaker(File file) {
        this._file = file;
    }

    /** Creates new in-memory database. Changes are lost after JVM exits.
     * <p/>
     * This will use HEAP memory so Garbage Collector is affected.
     */
    public static DBMaker newMemoryDB(){
        return new DBMaker(null);
    }

    public DBMakerT _newMemoryDB(){
        this._file = null;
        return getThis();
    }


    /** Creates new in-memory database. Changes are lost after JVM exits.
     * <p/>
     * This will use DirectByteBuffer outside of HEAP, so Garbage Collector is not affected
     *
     */
    public static DBMaker newDirectMemoryDB(){
        return new DBMaker()._newDirectMemoryDB();
    }

    public  DBMakerT _newDirectMemoryDB() {
        _file = null;
        _ifInMemoryUseDirectBuffer = true;
        return getThis();
    }


    /**
     * Creates or open append-only database stored in file.
     * This database uses format otherthan usual file db
     *
     * @param file
     * @return maker
     */
    public static DBMaker newAppendFileDB(File file) {
        return new DBMaker()._newAppendFileDB(file);
    }

    public DBMakerT _newAppendFileDB(File file) {
        _file = file;
        _appendStorage = true;
        return getThis();
    }


    /**
     * Create new BTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K,V> BTreeMap<K,V> newTempTreeMap(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .getTreeMap("temp");
    }

    /**
     * Create new HTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K,V> HTreeMap<K,V> newTempHashMap(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .getHashMap("temp");
    }

    /**
     * Create new TreeSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K> NavigableSet<K> newTempTreeSet(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .getTreeSet("temp");
    }

    /**
     * Create new HashSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * <p>
     * Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K> Set<K> newTempHashSet(){
        return newTempFileDB()
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .getHashSet("temp");
    }

    /**
     * Creates new database in temporary folder.
     *
     * @return
     */
    public static DBMaker newTempFileDB() {
        try {
            return newFileDB(File.createTempFile("mapdb-temp","db"));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    /** Creates or open database stored in file. */
    public static DBMaker newFileDB(File file){
        return new DBMaker(file);
    }

    public DBMakerT _newFileDB(File file){
        this._file = file;
        return getThis();
    }



    protected DBMakerT getThis(){
        return (DBMakerT)this;
    }


    /**
     * Transaction journal is enabled by default
     * You must call <b>DB.commit()</b> to save your changes.
     * It is possible to disable transaction journal for better write performance
     * In this case all integrity checks are sacrificed for faster speed.
     * <p/>
     * If transaction journal is disabled, all changes are written DIRECTLY into store.
     * You must call DB.close() method before exit,
     * otherwise your store <b>WILL BE CORRUPTED</b>
     *
     *
     * @return this builder
     * @deprecated use {@link DBMaker#transactionDisable()} instead.
     */
    public DBMakerT writeAheadLogDisable(){
        this._transactionEnabled = false;
        return getThis();
    }

    /**
     * Transaction journal is enabled by default
     * You must call <b>DB.commit()</b> to save your changes.
     * It is possible to disable transaction journal for better write performance
     * In this case all integrity checks are sacrificed for faster speed.
     * <p/>
     * If transaction journal is disabled, all changes are written DIRECTLY into store.
     * You must call DB.close() method before exit,
     * otherwise your store <b>WILL BE CORRUPTED</b>
     *
     *
     * @return this builder
     */
    public DBMakerT transactionDisable(){
        this._transactionEnabled = false;
        return getThis();
    }


    /**
     * Instance cache is enabled by default.
     * This greatly decreases serialization overhead and improves performance.
     * Call this method to disable instance cache, so an object will always be deserialized.
     * <p/>
     * This may workaround some problems
     *
     * @return this builder
     */
    public DBMakerT cacheDisable(){
        this._cache = CACHE_DISABLE;
        return getThis();
    }

    /**
     * Enables unbounded hard reference cache.
     * This cache is good if you have lot of available memory.
     * <p/>
     * All fetched records are added to HashMap and stored with hard reference.
     * To prevent OutOfMemoryExceptions MapDB monitors free memory,
     * if it is bellow 25% cache is cleared.
     *
     * @return this builder
     */
    public DBMakerT cacheHardRefEnable(){
        this._cache = CACHE_HARD_REF;
        return getThis();
    }


    /**
     * Enables unbounded cache which uses <code>WeakReference</code>.
     * Items are removed from cache by Garbage Collector
     *
     * @return this builder
     */
    public DBMakerT cacheWeakRefEnable(){
        this._cache = CACHE_WEAK_REF;
        return getThis();
    }

    /**
     * Enables unbounded cache which uses <code>SoftReference</code>.
     * Items are removed from cache by Garbage Collector
     *
     * @return this builder
     */
    public DBMakerT cacheSoftRefEnable(){
        this._cache = CACHE_SOFT_REF;
        return getThis();
    }

    /**
     * Enables Least Recently Used cache. It is fixed size cache and it removes less used items to make space.
     *
     * @return this builder
     */
    public DBMakerT cacheLRUEnable(){
        this._cache = CACHE_LRU;
        return getThis();
    }
    /**
     * Enables compatibility storage mode for 32bit JVMs.
     * <p/>
     * By default MapDB uses memory mapped files. However 32bit JVM can only address 4GB of memory.
     * Also some older JVMs do not handle large memory mapped files well.
     * We can use {@code FileChannel} which does not use memory mapped files, but is slower.
     * Use this if you are experiencing <b>java.lang.OutOfMemoryError: Map failed</b> exceptions
     * <p/>
     * This options disables memory mapped files but causes storage to be slower.
     */
    public DBMakerT randomAccessFileEnable() {
        _rafMode = 2;
        return getThis();
    }


    /** Same as {@code randomAccessFileEnable()}, but part of storage is kept memory mapped.
     *  This mode is good performance compromise between memory mapped files and RAF.
     *  <p/>
     *  Index file is typically 5% of storage. It contains small frequently read values,
     *  which is where memory mapped file excel.
     *  <p/>
     *  With this mode you will experience <b>java.lang.OutOfMemoryError: Map failed</b> exceptions
     *  eventually. But storage size limit is pushed to somewhere around 40GB.
     *
     */
    public DBMakerT randomAccessFileEnableKeepIndexMapped() {
        this._rafMode = 1;
        return getThis();
    }

    /**
     * Check current JVM for known problems. If JVM does not handle large memory files well, this option
     * disables memory mapped files, and use safer and slower {@code RandomAccessFile} instead.
     */
    public DBMakerT randomAccessFileEnableIfNeeded() {
        this._rafMode = Utils.JVMSupportsLargeMappedFiles()? 0:2;
        return getThis();
    }

    /**
     * Set cache size. Interpretations depends on cache type.
     * For fixed size caches (such as FixedHashTable cache) it is maximal number of items in cache.
     * <p/>
     * For unbounded caches (such as HardRef cache) it is initial capacity of underlying table (HashMap).
     * <p/>
     * Default cache size is 32768.
     *
     * @param cacheSize new cache size
     * @return this builder
     */
    public DBMakerT cacheSize(int cacheSize){
        this._cacheSize = cacheSize;
        return getThis();
    }

    /**
     * MapDB supports snapshots. `TxEngine` requires additional locking which has small overhead when not used.
     * So it is possible to disable snapshots in order to maximize performance when snapshots are not used.
     *
     * @return this builder
     */
    public DBMakerT snapshotDisable(){
        this._snapshotDisabled = true;
        return getThis();
    }


    /**
     * By default all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     * <p/>
     * It is possible to disable Background Writer Thread, but this greatly hurts concurrency.
     * Without async writes, all threads blocks until all previous writes are not finished (single big lock).
     *
     * <p/>
     * This may workaround some problems
     *
     * @return this builder
     */
    public DBMakerT asyncWriteDisable(){
        this._asyncWriteEnabled = false;
        return getThis();
    }



    /**
     * Set flush iterval for write cache, by default is 0
     * <p/>
     * When BTreeMap is constructed from ordered set, tree node size is increasing linearly with each
     * item added. Each time new key is added to tree node, its size changes and
     * storage needs to find new place. So constructing BTreeMap from ordered set leads to large
     * store fragmentation.
     * <p/>
     *  Setting flush interval is workaround as BTreeMap node is always updated in memory (write cache)
     *  and only final version of node is stored on disk.
     *
     *
     * @param delay flush write cache every N miliseconds
     * @return this builder
     */
    public DBMakerT asyncFlushDelay(int delay){
        _asyncFlushDelay = delay;
        return getThis();
    }


    /**
     * Try to delete files after DB is closed.
     * File deletion may silently fail, especially on Windows where buffer needs to be unmapped file delete.
     *
     * @return this builder
     */
    public DBMakerT deleteFilesAfterClose(){
        this._deleteFilesAfterClose = true;
        return getThis();
    }

    /**
     * Adds JVM shutdown hook and closes DB just before JVM;
     *
     * @return this builder
     */
    public DBMakerT closeOnJvmShutdown(){
        this._closeOnJvmShutdown = true;
        return getThis();
    }

    /**
     * Enables record compression.
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
     *
     * @return this builder
     */
    public DBMakerT compressionEnable(){
        this._compressionEnabled = true;
        return getThis();
    }


    /**
     * Encrypt storage using XTEA algorithm.
     * <p/>
     * XTEA is sound encryption algorithm. However implementation in MapDB was not peer-reviewed.
     * MapDB only encrypts records data, so attacker may see number of records and their sizes.
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
     *
     * @param password for encryption
     * @return this builder
     */
    public DBMakerT encryptionEnable(String password){
        return encryptionEnable(password.getBytes(Utils.UTF8_CHARSET));
    }



    /**
     * Encrypt storage using XTEA algorithm.
     * <p/>
     * XTEA is sound encryption algorithm. However implementation in MapDB was not peer-reviewed.
     * MapDB only encrypts records data, so attacker may see number of records and their sizes.
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
     *
     * @param password for encryption
     * @return this builder
     */
    public DBMakerT encryptionEnable(byte[] password){
        _xteaEncryptionKey = password;
        return getThis();
    }


    /**
     * Adds Adler32 checksum at end of each record to check data integrity.
     * It throws 'IOException("Checksum does not match, data broken")' on de-serialization if data are corrupted
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails.
     *
     * @return this builder
     */
    public DBMakerT checksumEnable(){
        this._checksumEnabled = true;
        return getThis();
    }


    /**
     * DB Get methods such as {@link DB#getTreeMap(String)} or {@link DB#getAtomicLong(String)} auto create
     * new record with default values, if record with given name does not exist. This could be problem if you would like to enforce
     * stricter database schema. So this parameter disables record auto creation.
     *
     * If this set, `DB.getXX()` will throw an exception if given name does not exist, instead of creating new record (or collection)
     *
     * @return this builder
     */
    public DBMakerT strictDBGet(){
        this._strictDBGet = true;
        return getThis();
    }




    /**
     * Open store in read-only mode. Any modification attempt will throw
     * <code>UnsupportedOperationException("Read-only")</code>
     *
     * @return this builder
     */
    public DBMakerT readOnly(){
        this._readOnly = true;
        return getThis();
    }



    /**
     * Set free space reclaim Q.  It is value from 0 to 10, indicating how eagerly MapDB
     * searchs for free space inside store to reuse, before expanding store file.
     * 0 means that no free space will be reused and store file will just grow (effectively append only).
     * 10 means that MapDB tries really hard to reuse free space, even if it may hurt performance.
     * Default value is 5;
     *
     *
     * @return this builder
     */
    public DBMakerT freeSpaceReclaimQ(int q){
        if(q<0||q>10) throw new IllegalArgumentException("wrong Q");
        this._freeSpaceReclaimQ = q;
        return getThis();
    }

    /**
     * Enables power saving mode.
     * Typically MapDB runs daemon threads in infinitive cycle with delays and spin locks:
     * <pre>
     *     while(true){
     *         Thread.sleep(1000);
     *         doSomething();
     *     }
     *
     *    while(write_finished){
     *         write_chunk;
     *         sleep(10 nanoseconds)  //so OS gets chance to finish async writing
     *     }
     *
     * </pre>
     * This brings bit more stability (prevents deadlocks) and some extra speed.
     * However it causes higher CPU usage then necessary, also CPU wakes-up every
     * N seconds.
     * <p>
     * On power constrained devices (phones, laptops..) trading speed for energy
     * consumption is not desired. So this settings tells MapDB to prefer
     * energy efficiency over speed and stability. This is global settings, so
     * this settings may affects any MapDB part where this settings makes sense
     * <p>
     * Currently is used only in {@link AsyncWriteEngine} where power settings
     * may prevent Background Writer Thread from exiting, if main thread dies.
     *
     * @return this builder
     */
//    public DBMaker powerSavingModeEnable(){
//        this._powerSavingMode = true;
//        return this;
//    }

    /**
     * Disables file sync on commit. This way transactions are preserved (rollback works),
     * but commits are not 'durable' and data may be lost if store is not properly closed.
     * File store will get properly synced when closed.
     * Disabling this will make commits faster.
     *
     * @return this builder
     */
    public DBMakerT syncOnCommitDisable(){
        this._syncOnCommitDisabled = true;
        return getThis();
    }


    /**
     * Sets store size limit. Disk or memory space consumed be storage should not grow over this space.
     * Limit is not strict and does not apply to some parts such as index table. Actual store size might
     * be 10% or more bigger.
     *
     *
     * @param maxSize maximal store size in GB
     * @return this builder
     */
    public DBMakerT sizeLimit(double maxSize){
        this._sizeLimit = (long) (maxSize * 1024D*1024D*1024D);
        return getThis();
    }




    /** constructs DB using current settings */
    public DB make(){
        return new DB(makeEngine(), _strictDBGet);
    }

    
    public TxMaker makeTxMaker(){
        this._fullTx= true;
        asyncWriteDisable();
        Engine e = makeEngine();
        if(!(e instanceof TxEngine)) throw new IllegalArgumentException("Snapshot must be enabled for TxMaker");
        //init catalog if needed
        DB db = new DB(e);
        db.commit();
        return new TxMaker((TxEngine) e);
    }

    /** constructs Engine using current settings */
    public Engine makeEngine(){


        if(_readOnly && _file==null)
            throw new UnsupportedOperationException("Can not open in-memory DB in read-only mode.");

        if(_readOnly && !_file.exists() && !_appendStorage){
            throw new UnsupportedOperationException("Can not open non-existing file in read-only mode.");
        }

        if(_sizeLimit>0 && _appendStorage)
            throw new UnsupportedOperationException("Append-Only store does not support Size Limit");

        extendArgumentCheck();

        Engine engine;

        if(!_appendStorage){
            Volume.Factory folFac = extendStoreVolumeFactory();

            engine = _transactionEnabled ?
                    extendStoreWAL(folFac) :
                    extendStoreDirect(folFac);
        }else{
            if(_file==null) throw new UnsupportedOperationException("Append Storage format is not supported with in-memory dbs");
            engine = extendStoreAppend();
        }

        engine = extendWrapStore(engine);

        if(_asyncWriteEnabled && !_readOnly){
            engine = extendAsyncWriteEngine(engine);
        }


        if(_cache == CACHE_DISABLE){
            //do not wrap engine in cache
        }else if(_cache == CACHE_FIXED_HASH_TABLE){
            engine = extendCacheHashTable(engine);
        }else if (_cache == CACHE_HARD_REF){
            engine = extendCacheHardRef(engine);
        }else if (_cache == CACHE_WEAK_REF){
            engine = extendCacheWeakRef(engine);
        }else if (_cache == CACHE_SOFT_REF){
            engine = extendCacheSoftRef(engine);
        }else if (_cache == CACHE_LRU){
            engine = extendCacheLRU(engine);
        }

        engine = extendWrapCache(engine);


        if(!_snapshotDisabled)
            engine = extendSnapshotEngine(engine);

        engine = extendWrapSnapshotEngine(engine);

        if(_readOnly)
            engine = new ReadOnlyEngine(engine);

        if(_closeOnJvmShutdown){
            final Engine engine2 = engine;
            Runtime.getRuntime().addShutdownHook(new Thread("MapDB shutdown") {
                @Override
				public void run() {
                    if(!engine2.isClosed())
                        extendShutdownHookBefore(engine2);
                        engine2.close();
                        extendShutdownHookAfter(engine2);
                }
            });
        }


        //try to read one record from DB, to make sure encryption and compression are correctly set.
        Fun.Tuple2<Integer,String> check = null;
        try{
            check = (Fun.Tuple2<Integer, String>) engine.get(Engine.CHECK_RECORD, Serializer.BASIC);
            if(check!=null){
                if(check.a.intValue()!= check.b.hashCode())
                    throw new RuntimeException("invalid checksum");
            }
        }catch(Throwable e){
            throw new IllegalArgumentException("Error while opening store. Make sure you have right password, compression or encryption is well configured.",e);
        }
        if(check == null && !engine.isReadOnly()){
            //new db, so insert testing record
            String s = Utils.randomString(127); //random string so it is not that easy to decrypt
            check = Fun.t2(s.hashCode(), s);
            engine.update(Engine.CHECK_RECORD, check, Serializer.BASIC);
            engine.commit();
        }


        return engine;
    }

    protected void extendShutdownHookBefore(Engine engine) {
    }

    protected void extendShutdownHookAfter(Engine engine) {
    }

    protected TxEngine extendSnapshotEngine(Engine engine) {
        return new TxEngine(engine,_fullTx);
    }

    protected Caches.LRU extendCacheLRU(Engine engine) {
        return new Caches.LRU(engine, _cacheSize);
    }

    protected Caches.WeakSoftRef extendCacheWeakRef(Engine engine) {
        return new Caches.WeakSoftRef(engine,true);
    }

    protected Caches.WeakSoftRef extendCacheSoftRef(Engine engine) {
        return new Caches.WeakSoftRef(engine,false);
    }


    protected Caches.HardRef extendCacheHardRef(Engine engine) {
        return new Caches.HardRef(engine,_cacheSize);
    }

    protected Caches.HashTable extendCacheHashTable(Engine engine) {
        return new Caches.HashTable(engine,_cacheSize);
    }

    protected AsyncWriteEngine extendAsyncWriteEngine(Engine engine) {
        return new AsyncWriteEngine(engine, _asyncFlushDelay, null);
    }


    protected void extendArgumentCheck() {
    }

    protected Engine extendWrapStore(Engine engine) {
        return engine;
    }


    protected Engine extendWrapCache(Engine engine) {
        return engine;
    }

    protected Engine extendWrapSnapshotEngine(Engine engine) {
        return engine;
    }



    protected StoreAppend extendStoreAppend() {
        return new StoreAppend(_file, _rafMode>0, _readOnly, !_transactionEnabled, _deleteFilesAfterClose, _syncOnCommitDisabled,
                _checksumEnabled,_compressionEnabled,_xteaEncryptionKey);
    }

    protected Store extendStoreDirect(Volume.Factory folFac) {
        return new StoreDirect(folFac,  _readOnly,_deleteFilesAfterClose, _freeSpaceReclaimQ,_syncOnCommitDisabled,_sizeLimit,
                _checksumEnabled,_compressionEnabled,_xteaEncryptionKey);
    }

    protected Store extendStoreWAL(Volume.Factory folFac) {
        return new StoreWAL(folFac,  _readOnly,_deleteFilesAfterClose, _freeSpaceReclaimQ,_syncOnCommitDisabled,_sizeLimit,
                _checksumEnabled,_compressionEnabled,_xteaEncryptionKey);
    }

    protected Volume.Factory extendStoreVolumeFactory() {
        return _file == null?
                    Volume.memoryFactory(_ifInMemoryUseDirectBuffer,_sizeLimit):
                    Volume.fileFactory(_readOnly, _rafMode, _file,_sizeLimit);
    }


}
