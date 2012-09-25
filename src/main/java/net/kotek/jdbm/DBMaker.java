package net.kotek.jdbm;

import java.io.File;

/**
 * A builder class for creating and opening a database.
 */
public class DBMaker {


    private static final byte CACHE_DISABLE = 0;
    private static final byte CACHE_FIXED_HASH_TABLE = 1;
    private static final byte CACHE_HARD_REF = 2;


    protected byte cache = CACHE_FIXED_HASH_TABLE;
    protected int cacheSize = 1024*32;

    /** file to open, if null opens in memory store */
    protected File file;

    protected boolean transactionsEnabled = true;

    protected boolean asyncWriteEnabled = true;
    protected boolean asyncSerializationEnabled = true;





    /** use static factory methods, or make subclass */
    protected DBMaker(){}

    /** Creates new in-memory database. Changes are lost after JVM exits*/
    public static DBMaker newMemoryDB(){
        DBMaker m = new DBMaker();
        m.file = null;
        return  m;
    }

    /** Creates or open database stored in file. */
    public static DBMaker newFileDB(File file){
        DBMaker m = new DBMaker();
        m.file = file;
        return  m;
    }


    /**
     * Transactions are enabled by default (but not implemented yet).
     * You must call <db>DB.commit()</db> to save your changes.
     * It is possible to disable transactions for better write performance
     * In this case all integrity checks are sacrificed for faster speed.
     * If transactions are disabled, you must call DB.close() method before exit,
     * otherwise your store <b>WILL BE CORRUPTED</b>
     *
     * @return this builder
     */
    public DBMaker transactionDisable(){
        this.transactionsEnabled = false;
        return this;
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
    public DBMaker cacheDisable(){
        this.cache = CACHE_DISABLE;
        return this;
    }

    /**
     * Enables unbounded hard reference cache.
     * This cache is good if you have lot of available memory.
     * <p/>
     * All fetched records are added to HashMap and stored with hard reference.
     * To prevent OutOfMemoryExceptions JDBM monitors free memory,
     * if it is bellow 25% cache is cleared.
     *
     * @return this builder
     */
    public DBMaker cacheHardRefEnable(){
        this.cache = CACHE_HARD_REF;
        return this;
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
    public DBMaker cacheSize(int cacheSize){
        this.cacheSize = cacheSize;
        return this;
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
    public DBMaker asyncWriteDisable(){
        this.asyncWriteEnabled = false;
        return this;
    }

    /**
     * By default all objects are serialized in Background Writer Thread.
     * <p/>
     * This may improve performance. For example with single thread access, Async Serialization offloads
     * lot of work to second core. Or when multiple values are added into single tree node,
     * node has to be serialized only once. Without Async Serialization node is serialized each time
     * node is updated.
     * <p/>
     * On other side Async Serialization moves all serialization into single thread. This
     * hurts performance with many concurrent-independent updates.
     * <p/>
     * Async Serialization may also produce some unexpected results when your data classes are not
     * immutable. Consider example bellow. If Async Serialization is disabled, it always prints 'Peter'.
     * If it is enabled (by default) it creates race condition and randomly prints 'Peter' or 'Jack',
     * <pre>
     *     Person person = new Person();
     *     person.setName("Peter");
     *     map.put(id, person)
     *     person.setName("Jack");
     *     //long pause
     *     println(map.get(id).getName());
     * </pre>
     *
     * <p/>
     * This may also workaround some problems
     *
     * @return this builder
     */
    public DBMaker asyncSerializationDisable(){
        this.asyncSerializationEnabled = false;
        return this;
    }


    /** constructs DB using current settings */
    public DB make(){

        RecordManager recman = new RecordStore(file, transactionsEnabled, !asyncSerializationEnabled);

        if(asyncWriteEnabled)
            recman = new RecordAsyncWrite(recman, asyncSerializationEnabled);

        if(cache == CACHE_DISABLE){
            //do not wrap recman in cache
        }if(cache == CACHE_FIXED_HASH_TABLE){
            recman = new RecordCacheHashTable(recman,cacheSize);
        }else if (cache == CACHE_HARD_REF){
            recman = new RecordCacheHardRef(recman,cacheSize);
        }

        return new DB(recman);
    }


}
