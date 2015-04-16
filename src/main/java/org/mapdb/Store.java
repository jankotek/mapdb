package org.mapdb;

import java.io.DataInput;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;

/**
 *
 */
public abstract class Store implements Engine {

    protected static final Logger LOG = Logger.getLogger(Store.class.getName());

    //TODO if locks are disabled, use NoLock for structuralLock and commitLock

    /** protects structural layout of records. Memory allocator is single threaded under this lock */
    protected final ReentrantLock structuralLock = new ReentrantLock(CC.FAIR_LOCKS);

    /** protects lifecycle methods such as commit, rollback and close() */
    protected final ReentrantLock commitLock = new ReentrantLock(CC.FAIR_LOCKS);

    /** protects data from being overwritten while read */
    protected final ReadWriteLock[] locks;
    protected final int lockScale;
    protected final int lockMask;


    protected volatile boolean closed = false;
    protected final boolean readonly;

    protected final String fileName;
    protected Fun.Function1<Volume, String> volumeFactory;
    protected boolean checksum;
    protected boolean compress;
    protected boolean encrypt;
    protected final EncryptionXTEA encryptionXTEA;
    protected final ThreadLocal<CompressLZF> LZF;

    protected final AtomicLong metricsDataWrite;
    protected final AtomicLong metricsRecordWrite;
    protected final AtomicLong metricsDataRead;
    protected final AtomicLong metricsRecordRead;


    protected final Cache[] caches;

    public static final int LOCKING_STRATEGY_READWRITELOCK=0;
    public static final int LOCKING_STRATEGY_WRITELOCK=1;
    public static final int LOCKING_STRATEGY_NOLOCK=2;

    protected Store(
            String fileName,
            Fun.Function1<Volume, String> volumeFactory,
            Cache cache,
            int lockScale,
            int lockingStrategy,
            boolean checksum,
            boolean compress,
            byte[] password,
            boolean readonly) {
        this.fileName = fileName;
        this.volumeFactory = volumeFactory;
        this.lockScale = lockScale;
        this.lockMask = lockScale-1;
        if(Integer.bitCount(lockScale)!=1)
            throw new IllegalArgumentException();
        //TODO replace with incrementer on java 8
        metricsDataWrite = new AtomicLong();
        metricsRecordWrite = new AtomicLong();
        metricsDataRead = new AtomicLong();
        metricsRecordRead = new AtomicLong();

        locks = new ReadWriteLock[lockScale];
        for(int i=0;i< locks.length;i++){
            if(lockingStrategy==LOCKING_STRATEGY_READWRITELOCK)
                locks[i] = new ReentrantReadWriteLock(CC.FAIR_LOCKS);
            else if(lockingStrategy==LOCKING_STRATEGY_WRITELOCK){
                locks[i] = new ReadWriteSingleLock(new ReentrantLock(CC.FAIR_LOCKS));
            }else if(lockingStrategy==LOCKING_STRATEGY_NOLOCK){
                locks[i] = new ReadWriteSingleLock(NOLOCK);
            }else{
                throw new IllegalArgumentException("Illegal locking strategy: "+lockingStrategy);
            }
        }

        if(cache==null) {
            caches = null;
        }else {
            caches = new Cache[lockScale];
            caches[0] = cache;
            for (int i = 1; i < caches.length; i++) {
                //each segment needs different cache, since StoreCache is not thread safe
                caches[i] = cache.newCacheForOtherSegment();
            }
        }


        this.checksum = checksum;
        this.compress = compress;
        this.encrypt =  password!=null;
        this.readonly = readonly;
        this.encryptionXTEA = !encrypt?null:new EncryptionXTEA(password);

        this.LZF = !compress?null:new ThreadLocal<CompressLZF>() {
            @Override
            protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };
    }

    public void init(){}

    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");

        int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].readLock();
        final Cache cache = caches==null ? null : caches[lockPos];
        lock.lock();
        try{
            A o = cache==null ? null : (A) cache.get(recid);
            if(o!=null) {
                return o== Cache.NULL?null:o;
            }
            o =  get2(recid,serializer);
            if(cache!=null) {
                cache.put(recid, o);
            }
            return o;
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> A get2(long recid, Serializer<A> serializer);

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");


        //serialize outside lock
        DataIO.DataOutputByteArray out = serialize(value, serializer);
        int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].writeLock();
        final Cache cache = caches==null ? null : caches[lockPos];
        lock.lock();
        try{
            if(cache!=null) {
                cache.put(recid, value);
            }
            update2(recid,out);
        }finally {
            lock.unlock();
        }
    }

    //TODO DataOutputByteArray is not thread safe, make one recycled per segment lock
    protected final AtomicReference<DataIO.DataOutputByteArray> recycledDataOut =
            new AtomicReference<DataIO.DataOutputByteArray>();

    protected <A> DataIO.DataOutputByteArray serialize(A value, Serializer<A> serializer){
        if(value==null)
            return null;
        try {
            DataIO.DataOutputByteArray out = newDataOut2();

            serializer.serialize(out,value);

            if(out.pos>0){

                if(compress){
                    DataIO.DataOutputByteArray tmp = newDataOut2();
                    tmp.ensureAvail(out.pos+40);
                    final CompressLZF lzf = LZF.get();
                    int newLen;
                    try{
                        newLen = lzf.compress(out.buf,out.pos,tmp.buf,0);
                    }catch(IndexOutOfBoundsException e){
                        newLen=0; //larger after compression
                    }
                    if(newLen>=out.pos) newLen= 0; //larger after compression

                    if(newLen==0){
                        recycledDataOut.lazySet(tmp);
                        //compression had no effect, so just write zero at beginning and move array by 1
                        out.ensureAvail(out.pos+1);
                        System.arraycopy(out.buf,0,out.buf,1,out.pos);
                        out.pos+=1;
                        out.buf[0] = 0;
                    }else{
                        //compression had effect, so write decompressed size and compressed array
                        final int decompSize = out.pos;
                        out.pos=0;
                        DataIO.packInt(out,decompSize);
                        out.write(tmp.buf,0,newLen);
                        recycledDataOut.lazySet(tmp);
                    }

                }


                if(encrypt){
                    int size = out.pos;
                    //round size to 16
                    if(size%EncryptionXTEA.ALIGN!=0)
                        size += EncryptionXTEA.ALIGN - size%EncryptionXTEA.ALIGN;
                    final int sizeDif=size-out.pos;
                    //encrypt
                    out.ensureAvail(sizeDif+1);
                    encryptionXTEA.encrypt(out.buf,0,size);
                    //and write diff from 16
                    out.pos = size;
                    out.writeByte(sizeDif);
                }

                if(checksum){
                    CRC32 crc = new CRC32();
                    crc.update(out.buf,0,out.pos);
                    out.writeInt((int)crc.getValue());
                }

                if(CC.PARANOID)try{
                    //check that array is the same after deserialization
                    DataInput inp = new DataIO.DataInputByteArray(Arrays.copyOf(out.buf, out.pos));
                    byte[] decompress = deserialize(Serializer.BYTE_ARRAY_NOSIZE,out.pos,inp);

                    DataIO.DataOutputByteArray expected = newDataOut2();
                    serializer.serialize(expected,value);

                    byte[] expected2 = Arrays.copyOf(expected.buf, expected.pos);
                    //check arrays equals
                    if(CC.PARANOID && ! (Arrays.equals(expected2,decompress)))
                        throw new AssertionError();


                }catch(Exception e){
                    throw new RuntimeException(e);
                }
            }

            metricsDataWrite.getAndAdd(out.pos);
            metricsRecordWrite.incrementAndGet();

            return out;
        } catch (IOException e) {
            throw new IOError(e);
        }

    }

    protected DataIO.DataOutputByteArray newDataOut2() {
        DataIO.DataOutputByteArray tmp = recycledDataOut.getAndSet(null);
        if(tmp==null) tmp = new DataIO.DataOutputByteArray();
        else tmp.pos=0;
        return tmp;
    }


    protected <A> A deserialize(Serializer<A> serializer, int size, DataInput input){
        try {
            //TODO if serializer is not trusted, use boundary check
            //TODO return future and finish deserialization outside lock, does even bring any performance bonus?

            DataIO.DataInputInternal di = (DataIO.DataInputInternal) input;
            if (size > 0 && (checksum || encrypt || compress))  {
                return deserializeExtra(serializer,size,di);
            }

            int start = di.getPos();

            A ret = serializer.deserialize(di, size);
            if (size + start > di.getPos())
                throw new AssertionError("data were not fully read, check your serializer ");
            if (size + start < di.getPos())
                throw new AssertionError("data were read beyond record size, check your serializer");

            metricsDataRead.getAndAdd(size);
            metricsRecordRead.getAndIncrement();

            return ret;
        }catch(IOException e){
            throw new IOError(e);
        }
    }

    /** helper method, it is called if compression or other stuff is used. It can not be JITed that well. */
    private <A> A deserializeExtra(Serializer<A> serializer, int size, DataIO.DataInputInternal di) throws IOException {
        if (checksum) {
            //last two digits is checksum
            size -= 4;

            //read data into tmp buffer
            DataIO.DataOutputByteArray tmp = newDataOut2();
            tmp.ensureAvail(size);
            int oldPos = di.getPos();
            di.readFully(tmp.buf, 0, size);
            final int checkExpected = di.readInt();
            di.setPos(oldPos);
            //calculate checksums
            CRC32 crc = new CRC32();
            crc.update(tmp.buf, 0, size);
            recycledDataOut.lazySet(tmp);
            int check = (int) crc.getValue();
            if (check != checkExpected)
                throw new IOException("Checksum does not match, data broken");
        }

        if (encrypt) {
            DataIO.DataOutputByteArray tmp = newDataOut2();
            size -= 1;
            tmp.ensureAvail(size);
            di.readFully(tmp.buf, 0, size);
            encryptionXTEA.decrypt(tmp.buf, 0, size);
            int cut = di.readUnsignedByte(); //length dif from 16bytes
            di = new DataIO.DataInputByteArray(tmp.buf);
            size -= cut;
        }

        if (compress) {
            //final int origPos = di.pos;
            int decompSize = DataIO.unpackInt(di);
            if (decompSize == 0) {
                size -= 1;
                //rest of `di` is uncompressed data
            } else {
                DataIO.DataOutputByteArray out = newDataOut2();
                out.ensureAvail(decompSize);
                CompressLZF lzf = LZF.get();
                //TODO copy to heap if Volume is not mapped
                //argument is not needed; unpackedSize= size-(di.pos-origPos),
                byte[] b = di.internalByteArray();
                if (b != null) {
                    lzf.expand(b, di.getPos(), out.buf, 0, decompSize);
                } else {
                    ByteBuffer bb = di.internalByteBuffer();
                    if (bb != null) {
                        lzf.expand(bb, di.getPos(), out.buf, 0, decompSize);
                    } else {
                        lzf.expand(di, out.buf, 0, decompSize);
                    }
                }
                di = new DataIO.DataInputByteArray(out.buf);
                size = decompSize;
            }
        }


        int start = di.getPos();

        A ret = serializer.deserialize(di, size);
        if (size + start > di.getPos())
            throw new AssertionError("data were not fully read, check your serializer ");
        if (size + start < di.getPos())
            throw new AssertionError("data were read beyond record size, check your serializer");
        return ret;
    }

    protected abstract  void update2(long recid, DataIO.DataOutputByteArray out);

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");


        //TODO binary CAS & serialize outside lock
        final int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].writeLock();
        final Cache cache = caches==null ? null : caches[lockPos];
        lock.lock();
        try{
            A oldVal =  cache==null ? null : (A)cache.get(recid);
            if(oldVal == null) {
                oldVal = get2(recid, serializer);
            }else if(oldVal == Cache.NULL){
                oldVal = null;
            }
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                update2(recid,serialize(newValue,serializer));
                if(cache!=null) {
                    cache.put(recid, newValue);
                }
                return true;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }


    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");


        final int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].writeLock();
        final Cache cache = caches==null ? null : caches[lockPos];
        lock.lock();
        try{
            if(cache!=null) {
                cache.put(recid, null);
            }
            delete2(recid, serializer);
        }finally {
            lock.unlock();
        }
    }

    protected abstract <A> void delete2(long recid, Serializer<A> serializer);

    protected final int lockPos(final long recid) {
        int h = (int)(recid ^ (recid >>> 32));
        //spread bits, so each bit becomes part of segment (lockPos)
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        h ^= (h<<4);
        return h & lockMask;
    }

    protected void assertReadLocked(long recid) {
//        if(locks[lockPos(recid)].writeLock().getHoldCount()!=0){
//            throw new AssertionError();
//        }
    }

    protected void assertWriteLocked(int segment) {
        ReadWriteLock l = locks[segment];
        if(l instanceof ReentrantReadWriteLock && !((ReentrantReadWriteLock) l).isWriteLockedByCurrentThread()){
            throw new AssertionError();
        }
    }


    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    /** traverses Engine wrappers and returns underlying {@link Store}*/
    public static Store forDB(DB db){
        return forEngine(db.engine);
    }

    /** traverses Engine wrappers and returns underlying {@link Store}*/
    public static Store forEngine(Engine e){
        Engine engine2 = e.getWrappedEngine();
        if(engine2!=null)
            return forEngine(engine2);

        return (Store) e;
    }

    public abstract long getCurrSize();

    public abstract long getFreeSize();

    @Override
    public void clearCache() {
        if(closed)
            throw new IllegalAccessError("closed");

        if(caches==null)
            return;

        for(int i=0;i<locks.length;i++){
            Lock lock = locks[i].readLock();
            lock.lock();
            try{
                caches[i].clear();
            }finally {
                lock.unlock();
            }
        }
    }

    /** puts metrics into given map */
    public void metricsCollect(Map<String,Long> map) {
        map.put(DB.METRICS_DATA_WRITE,metricsDataWrite.getAndSet(0));
        map.put(DB.METRICS_RECORD_WRITE,metricsRecordWrite.getAndSet(0));
        map.put(DB.METRICS_DATA_READ,metricsDataRead.getAndSet(0));
        map.put(DB.METRICS_RECORD_READ,metricsRecordRead.getAndSet(0));

        long cacheHit = 0;
        long cacheMiss = 0;
        if(caches!=null) {
            for (Cache c : caches) {
                cacheHit += c.metricsCacheHit();
                cacheMiss += c.metricsCacheMiss();
            }
        }

        map.put(DB.METRICS_CACHE_HIT,cacheHit);
        map.put(DB.METRICS_CACHE_MISS, cacheMiss);
    }

    /**
     * Cache implementation, part of {@link Store} class.
     */
    public static abstract class Cache {

        protected final Lock lock;
        protected long cacheHitCounter = 0;
        protected long cacheMissCounter = 0;

        protected static final Object NULL = new Object();

        public Cache(boolean disableLocks) {
            this.lock = disableLocks?null:  new ReentrantLock(CC.FAIR_LOCKS);
        }


        public abstract Object get(long recid);
        public abstract void put(long recid, Object item);

        public abstract void clear();
        public abstract void close();

        public abstract Cache newCacheForOtherSegment();

        /** how many times was cache hit, also reset counter */
        public long metricsCacheHit() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try {
                long ret = cacheHitCounter;
                cacheHitCounter=0;
                return ret;
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }


        /** how many times was cache miss, also reset counter */
        public long metricsCacheMiss() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try {
                long ret = cacheMissCounter;
                cacheMissCounter=0;
                return ret;
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        /**
         * <p>
         * Fixed size cache which uses hash table.
         * Is thread-safe and requires only minimal locking.
         * Items are randomly removed and replaced by hash collisions.
         * </p><p>
         * This is simple, concurrent, small-overhead, random cache.
         * </p>
         *
         * @author Jan Kotek
         */
        public static final class HashTable extends Cache {


            protected final long[] recids; //TODO 6 byte longs
            protected final Object[] items;

            protected final int cacheMaxSizeMask;


            public HashTable(int cacheMaxSize, boolean disableLocks) {
                super(disableLocks);
                cacheMaxSize = DataIO.nextPowTwo(cacheMaxSize); //next pow of two

                this.cacheMaxSizeMask = cacheMaxSize-1;

                this.recids = new long[cacheMaxSize];
                this.items = new Object[cacheMaxSize];
            }

            @Override
            public Object get(long recid) {
                int pos = pos(recid);
                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try {
                    boolean hit = recids[pos] == recid;
                    if(hit){
                        if(CC.METRICS_CACHE)
                            cacheHitCounter++;
                        return items[pos];
                    }else{
                        if(CC.METRICS_CACHE)
                            cacheMissCounter++;
                        return null;
                    }
                }finally {
                    if(lock!=null)
                        lock.unlock();
                }
            }

            @Override
            public void put(long recid, Object item) {
                if(item == null)
                    item = NULL;
                int pos = pos(recid);
                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try {
                    recids[pos] = recid;
                    items[pos] = item;
                }finally {
                    if(lock!=null)
                        lock.unlock();
                }
            }

            protected int pos(long recid) {
                return DataIO.longHash(recid)&cacheMaxSizeMask;
            }

            @Override
            public void clear() {
                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try {
                    Arrays.fill(recids, 0L);
                    Arrays.fill(items, null);
                }finally {
                    if(lock!=null)
                        lock.unlock();
                }
            }

            @Override
            public void close() {
                clear();
            }

            @Override
            public Cache newCacheForOtherSegment() {
                return new HashTable(recids.length,lock==null);
            }

        }


    /**
     * Instance cache which uses <code>SoftReference</code> or <code>WeakReference</code>
     * Items can be removed from cache by Garbage Collector if
     *
     * @author Jan Kotek
     */
    public static class WeakSoftRef extends Store.Cache {


        protected interface CacheItem{
            long getRecid();
            Object get();
            void clear();
        }

        protected static final class CacheWeakItem<A> extends WeakReference<A> implements CacheItem {

            final long recid;

            public CacheWeakItem(A referent, ReferenceQueue<A> q, long recid) {
                super(referent, q);
                this.recid = recid;
            }

            @Override
            public long getRecid() {
                return recid;
            }
        }

        protected static final class CacheSoftItem<A> extends SoftReference<A> implements CacheItem {

            final long recid;

            public CacheSoftItem(A referent, ReferenceQueue<A> q, long recid) {
                super(referent, q);
                this.recid = recid;
            }

            @Override
            public long getRecid() {
                return recid;
            }
        }

        protected ReferenceQueue<Object> queue = new ReferenceQueue<Object>();

        protected LongObjectMap<CacheItem> items = new LongObjectMap<CacheItem>();

        protected final static int CHECK_EVERY_N = 0xFFFF;
        protected int counter = 0;
        protected final ScheduledExecutorService executor;

        protected final boolean useWeakRef;
        protected final long executorScheduledRate;

        public WeakSoftRef(boolean useWeakRef, boolean disableLocks,
                           ScheduledExecutorService executor,
                           long executorScheduledRate) {
            super(disableLocks);
            if(CC.PARANOID && disableLocks && executor!=null) {
                throw new IllegalArgumentException("Lock can not be disabled with executor enabled");
            }
            this.useWeakRef = useWeakRef;
            this.executor = executor;
            this.executorScheduledRate = executorScheduledRate;
            if(executor!=null){
                executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        WeakSoftRef.this.flushGCedLocked();
                    }
                    },
                    (long) (executorScheduledRate*Math.random()),
                    executorScheduledRate,
                    TimeUnit.MILLISECONDS);
            }
        }


        @Override
        public Object get(long recid) {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                CacheItem item = items.get(recid);
                Object ret;
                if(item==null){
                    if(CC.METRICS_CACHE)
                        cacheMissCounter++;
                    ret = null;
                }else{
                    if(CC.METRICS_CACHE)
                        cacheHitCounter++;
                    ret = item.get();
                }

                if (executor==null && (((counter++) & CHECK_EVERY_N) == 0)) {
                    flushGCed();
                }
                return ret;
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        @Override
        public void put(long recid, Object item) {
            if(item ==null)
                item = Cache.NULL;
            CacheItem cacheItem = useWeakRef?
                    new CacheWeakItem(item,queue,recid):
                    new CacheSoftItem(item,queue,recid);
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                CacheItem older = items.put(recid,cacheItem);
                if(older!=null)
                    older.clear();
                if (executor==null && (((counter++) & CHECK_EVERY_N) == 0)) {
                    flushGCed();
                }
            }finally {
                if(lock!=null)
                    lock.unlock();
            }

        }

        @Override
        public void clear() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                items.clear(); //TODO more efficient method, which would bypass queue
            }finally {
                if(lock!=null)
                    lock.unlock();
            }

        }

        @Override
        public void close() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                //TODO howto correctly shutdown queue? possible memory leak here?
                items.clear();
                items = null;
                flushGCed();
                queue = null;
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        @Override
        public Cache newCacheForOtherSegment() {
            return new Cache.WeakSoftRef(
                    useWeakRef,
                    lock==null,
                    executor,
                    executorScheduledRate);
        }

        protected void flushGCed() {
            if(CC.PARANOID && lock!=null &&
                    (lock instanceof ReentrantLock) &&
                    !((ReentrantLock)lock).isHeldByCurrentThread()) {
                throw new AssertionError("Not locked by current thread");
            }
            counter = 1;
            CacheItem item = (CacheItem) queue.poll();
            while(item!=null){
                long recid = item.getRecid();

                CacheItem otherEntry = items.get(recid);
                if(otherEntry !=null && otherEntry.get()==null)
                    items.remove(recid);

                item = (CacheItem) queue.poll();
            }
        }


        protected void flushGCedLocked() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                flushGCed();
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

    }

    /**
     * Cache created objects using hard reference.
     * It checks free memory every N operations (1024*10). If free memory is bellow 75% it clears the cache
     *
     * @author Jan Kotek
     */
    public static final class HardRef extends  Store.Cache{

        protected final static int CHECK_EVERY_N = 0xFFFF;

        protected int counter;

        protected final Store.LongObjectMap cache;

        protected final int initialCapacity;

        protected final ScheduledExecutorService executor;
        protected final long executorPeriod;


        public HardRef(int initialCapacity, boolean disableLocks, ScheduledExecutorService executor, long executorPeriod) {
            super(disableLocks);
            if(disableLocks && executor!=null)
                throw new IllegalArgumentException("Executor can not be enabled with lock disabled");
            
            this.initialCapacity = initialCapacity;
            cache = new Store.LongObjectMap(initialCapacity);
            this.executor = executor;
            this.executorPeriod = executorPeriod;
            if(executor!=null){
                executor.scheduleAtFixedRate(new Runnable() {
                    @Override
                    public void run() {
                        Lock lock = HardRef.this.lock;
                        lock.lock();
                        try {
                            checkFreeMem();
                        }finally {
                            lock.unlock();
                        }
                    }
                },executorPeriod,executorPeriod,TimeUnit.MILLISECONDS);
            }
        }


        private void checkFreeMem() {
            counter=1;
            Runtime r = Runtime.getRuntime();
            long max = r.maxMemory();
            if(max == Long.MAX_VALUE)
                return;

            double free = r.freeMemory();
            double total = r.totalMemory();
            //We believe that free refers to total not max.
            //Increasing heap size to max would increase to max
            free = free + (max-total);

            if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                LOG.fine("HardRefCache: freemem = " +free + " = "+(free/max)+"%");
            //$DELAY$
            if(free<1e7 || free*4 <max){
                cache.clear();
                if(CC.LOG_EWRAP && LOG.isLoggable(Level.FINE))
                    LOG.fine("Clear HardRef cache");
            }
        }

        @Override
        public Object get(long recid) {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try {
                if (executor==null && ((counter++) & CHECK_EVERY_N) == 0) {
                    checkFreeMem();
                }
                Object item = cache.get(recid);

                if(CC.METRICS_CACHE){
                    if(item!=null){
                        cacheHitCounter++;
                    }else{
                        cacheMissCounter++;
                    }
                }

                return item;
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        @Override
        public void put(long recid, Object item) {
            if(item == null)
                item = Cache.NULL;
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try {
                if (executor==null && ((counter++) & CHECK_EVERY_N) == 0) {
                    checkFreeMem();
                }
                cache.put(recid,item);
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        @Override
        public void clear() {
            Lock lock = this.lock;
            if(lock!=null)
                lock.lock();
            try{
                cache.clear();
            }finally {
                if(lock!=null)
                    lock.unlock();
            }
        }

        @Override
        public void close() {
            clear();
        }

        @Override
        public Cache newCacheForOtherSegment() {
            return new HardRef(initialCapacity,lock==null,executor,executorPeriod);
        }
    }

        public static final class LRU extends Cache {

            protected final int cacheSize;

            //TODO specialized version of LinkedHashMap to use primitive longs
            protected final LinkedHashMap<Long, Object> items = new LinkedHashMap<Long,Object>();

            public LRU(int cacheSize, boolean disableLocks) {
                super(disableLocks);
                this.cacheSize = cacheSize;
            }

            @Override
            public Object get(long recid) {
                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try{
                    Object ret =  items.get(recid);
                    if(CC.METRICS_CACHE){
                        if(ret!=null){
                            cacheHitCounter++;
                        }else{
                            cacheMissCounter++;
                        }
                    }
                    return ret;

                }finally {
                    if(lock!=null)
                        lock.unlock();
                }
            }

            @Override
            public void put(long recid, Object item) {
                if(item == null)
                    item = Cache.NULL;

                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try{
                    items.put(recid,item);

                    //remove oldest items from queue if necessary
                    int itemsSize = items.size();
                    if(itemsSize>cacheSize) {
                        Iterator iter = items.entrySet().iterator();
                        while(itemsSize-- > cacheSize && iter.hasNext()){
                            iter.next();
                            iter.remove();
                        }
                    }

                }finally {
                    if(lock!=null)
                        lock.unlock();
                }

            }

            @Override
            public void clear() {
                Lock lock = this.lock;
                if(lock!=null)
                    lock.lock();
                try{
                    items.clear();
                }finally {
                    if(lock!=null)
                        lock.unlock();
                }
            }

            @Override
            public void close() {
                clear();
            }

            @Override
            public Cache newCacheForOtherSegment() {
                return new LRU(cacheSize,lock==null);
            }
        }
    }



    /**
     * <p>
     * Open Hash Map which uses primitive long as values and keys.
     * </p><p>
     *
     * This is very stripped down version from Koloboke Collection Library.
     * I removed modCount, free value (defaults to zero) and
     * most of the methods. Only put/get operations are supported.
     * </p><p>
     *
     * To iterate over collection one has to traverse {@code table} which contains
     * key-value pairs and skip zero pairs.
     * </p>
     *
     * @author originaly part of Koloboke library, Roman Leventov, Higher Frequency Trading
     * @author heavily modified for MapDB
     */
    public static final class LongLongMap {

        int size;

        int maxSize;

        long[] table;

        public LongLongMap(){
            this(32);
        }

        public LongLongMap(int initCapacity) {
            initCapacity = DataIO.nextPowTwo(initCapacity)*2;
            table = new long[initCapacity];
        }


        public long get(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = index(key);
            if (index >= 0) {
                // key is presentt
                return table[index + 1];
            } else {
                // key is absent
                return 0;
            }
        }

        public long put(long key, long value) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            if(CC.PARANOID && value==0)
                throw new IllegalArgumentException("zero val");

            int index = insert(key, value);
            if (index < 0) {
                // key was absent
                return 0;
            } else {
                // key is present
                long[] tab = table;
                long prevValue = tab[index + 1];
                tab[index + 1] = value;
                return prevValue;
            }
        }

        int insert(long key, long value) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            long[] tab = table;
            int capacityMask, index;
            long cur;
            keyAbsent:
            if ((cur = tab[index = DataIO.longHash(key) & (capacityMask = tab.length - 2)]) != 0) {
                if (cur == key) {
                    // key is present
                    return index;
                } else {
                    while (true) {
                        if ((cur = tab[(index = (index - 2) & capacityMask)]) == 0) {
                            break keyAbsent;
                        } else if (cur == key) {
                            // key is present
                            return index;
                        }
                    }
                }
            }
            // key is absent
            tab[index] = key;
            tab[index + 1] = value;

            //post insert hook
            if (++size > maxSize) {
                int capacity = table.length >> 1;
                if (!isMaxCapacity(capacity)) {
                    rehash(capacity << 1);
                }
            }


            return -1;
        }

        int index(long key) {
            if (key != 0) {
                long[] tab = table;
                int capacityMask, index;
                long cur;
                if ((cur = tab[index = DataIO.longHash(key) & (capacityMask = tab.length - 2)]) == key) {
                    // key is present
                    return index;
                } else {
                    if (cur == 0) {
                        // key is absent
                        return -1;
                    } else {
                        while (true) {
                            if ((cur = tab[(index = (index - 2) & capacityMask)]) == key) {
                                // key is present
                                return index;
                            } else if (cur == 0) {
                                // key is absent
                                return -1;
                            }
                        }
                    }
                }
            } else {
                // key is absent
                return -1;
            }
        }

        public int size(){
            return size;
        }

        public void clear() {
            size = 0;
            Arrays.fill(table,0);
        }


        void rehash(int newCapacity) {
            long[] tab = table;
            if(CC.PARANOID && !((newCapacity & (newCapacity - 1)) == 0)) //is power of two?
                throw new AssertionError();
            maxSize = maxSize(newCapacity);
            table = new long[newCapacity * 2];

            long[] newTab = table;
            int capacityMask = newTab.length - 2;
            for (int i = tab.length - 2; i >= 0; i -= 2) {
                long key;
                if ((key = tab[i]) != 0) {
                    int index;
                    if (newTab[index = DataIO.longHash(key) & capacityMask] != 0) {
                        while (true) {
                            if (newTab[(index = (index - 2) & capacityMask)] == 0) {
                                break;
                            }
                        }
                    }
                    newTab[index] = key;
                    newTab[index + 1] = tab[i + 1];
                }
            }
        }

        static int maxSize(int capacity) {
            // No sense in trying to rehash after each insertion
            // if the capacity is already reached the limit.
            return !isMaxCapacity(capacity) ?
                    capacity/2 //TODO not sure I fully understand how growth factors works here
                    : capacity - 1;
        }

        private static final int MAX_INT_CAPACITY = 1 << 30;

        private static boolean isMaxCapacity(int capacity) {
            int maxCapacity = MAX_INT_CAPACITY;
            maxCapacity >>= 1;
            return capacity == maxCapacity;
        }


    }


    /**
     * <p>
     * Open Hash Map which uses primitive long as keys.
     * </p><p>
     *
     * This is very stripped down version from Koloboke Collection Library.
     * I removed modCount, free value (defaults to zero) and
     * most of the methods. Only put/get/remove operations are supported.
     * </p><p>
     *
     * To iterate over collection one has to traverse {@code set} which contains
     * keys, values are in separate field.
     * </p>
     *
     * @author originaly part of Koloboke library, Roman Leventov, Higher Frequency Trading
     * @author heavily modified for MapDB
     */
    public static final class LongObjectMap<V> {

        int size;

        int maxSize;

        long[] set;
        Object[] values;

        public LongObjectMap(){
            this(32);
        }

        public LongObjectMap(int initCapacity) {
            initCapacity = DataIO.nextPowTwo(initCapacity);
            set = new long[initCapacity];
            values = (V[]) new Object[initCapacity];
        }

        public V get(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = index(key);
            if (index >= 0) {
                // key is present
                return (V) values[index];
            } else {
                // key is absent
                return null;
            }
        }

        int index(long key) {
            if (key != 0) {
                long[] keys = set;
                int capacityMask, index;
                long cur;
                if ((cur = keys[index = DataIO.longHash(key) & (capacityMask = keys.length - 1)]) == key) {
                    // key is present
                    return index;
                } else {
                    if (cur == 0) {
                        // key is absent
                        return -1;
                    } else {
                        while (true) {
                            if ((cur = keys[(index = (index - 1) & capacityMask)]) == key) {
                                // key is present
                                return index;
                            } else if (cur == 0) {
                                // key is absent
                                return -1;
                            }
                        }
                    }
                }
            } else {
                // key is absent
                return -1;
            }
        }

        public V put(long key, V value) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = insert(key, value);
            if (index < 0) {
                // key was absent
                return null;
            } else {
                // key is present
                Object[] vals = values;
                V prevValue = (V) vals[index];
                vals[index] = value;
                return prevValue;
            }
        }

        int insert(long key, V value) {
            long[] keys = set;
            int capacityMask, index;
            long cur;
            keyAbsent:
            if ((cur = keys[index = DataIO.longHash(key) & (capacityMask = keys.length - 1)]) != 0) {
                if (cur == key) {
                    // key is present
                    return index;
                } else {
                    while (true) {
                        if ((cur = keys[(index = (index - 1) & capacityMask)]) == 0) {
                            break keyAbsent;
                        } else if (cur == key) {
                            // key is present
                            return index;
                        }
                    }
                }
            }
            // key is absent

            keys[index] = key;
            values[index] = value;
            postInsertHook();
            return -1;
        }

        void postInsertHook() {
            if (++size > maxSize) {
            /* if LHash hash */
                int capacity = set.length;
                if (!LongLongMap.isMaxCapacity(capacity)) {
                    rehash(capacity << 1);
                }
            }
        }


        void rehash(int newCapacity) {
            long[] keys = set;
            Object[] vals = values;

            maxSize = LongLongMap.maxSize(newCapacity);
            set = new long[newCapacity];
            values = new Object[newCapacity];

            long[] newKeys = set;
            int capacityMask = newKeys.length - 1;
            Object[] newVals = values;
            for (int i = keys.length - 1; i >= 0; i--) {
                long key;
                if ((key = keys[i]) != 0) {
                    int index;
                    if (newKeys[index = DataIO.longHash(key) & capacityMask] != 0) {
                        while (true) {
                            if (newKeys[(index = (index - 1) & capacityMask)] == 0) {
                                break;
                            }
                        }
                    }
                    newKeys[index] = key;
                    newVals[index] = vals[i];
                }
            }
        }


        public void clear() {
            size = 0;
            Arrays.fill(set,0);
            Arrays.fill(values,null);
        }

        public V remove(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");
                long[] keys = set;
                int capacityMask = keys.length - 1;
                int index;
                long cur;
                keyPresent:
                if ((cur = keys[index = DataIO.longHash(key) & capacityMask]) != key) {
                    if (cur == 0) {
                        // key is absent
                        return null;
                    } else {
                        while (true) {
                            if ((cur = keys[(index = (index - 1) & capacityMask)]) == key) {
                                break keyPresent;
                            } else if (cur == 0) {
                                // key is absent
                                return null;
                            }
                        }
                    }
                }
                // key is present
                Object[] vals = values;
                V val = (V) vals[index];

                int indexToRemove = index;
                int indexToShift = indexToRemove;
                int shiftDistance = 1;
                while (true) {
                    indexToShift = (indexToShift - 1) & capacityMask;
                    long keyToShift;
                    if ((keyToShift = keys[indexToShift]) == 0) {
                        break;
                    }
                    if (((DataIO.longHash(keyToShift) - indexToShift) & capacityMask) >= shiftDistance) {
                        keys[indexToRemove] = keyToShift;
                        vals[indexToRemove] = vals[indexToShift];
                        indexToRemove = indexToShift;
                        shiftDistance = 1;
                    } else {
                        shiftDistance++;
                        if (indexToShift == 1 + index) {
                            throw new java.util.ConcurrentModificationException();
                        }
                    }
                }
                keys[indexToRemove] = 0;
                vals[indexToRemove] = null;

                //post remove hook
                size--;

                return val;
        }
    }


    /** fake lock */

    public static final Lock NOLOCK = new Lock(){

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    };

    /** fake read/write lock which in fact locks on single write lock */
    public static final class ReadWriteSingleLock implements ReadWriteLock{

        protected final Lock lock;

        public ReadWriteSingleLock(Lock lock) {
            this.lock = lock;
        }


        @Override
        public Lock readLock() {
            return lock;
        }

        @Override
        public Lock writeLock() {
            return lock;
        }
    }


    /**
     * <p>
     * Open Hash Map which uses primitive long as keys.
     * It also has two values, instead of single one
     * </p><p>
     *
     * This is very stripped down version from Koloboke Collection Library.
     * I removed modCount, free value (defaults to zero) and
     * most of the methods. Only put/get/remove operations are supported.
     * </p><p>
     *
     * To iterate over collection one has to traverse {@code set} which contains
     * keys, values are in separate field.
     * </p>
     *
     * @author originaly part of Koloboke library, Roman Leventov, Higher Frequency Trading
     * @author heavily modified for MapDB
     */
    public static final class LongObjectObjectMap<V1,V2> {

        int size;

        int maxSize;

        long[] set;
        Object[] values;

        public LongObjectObjectMap(){
            this(32);
        }

        public LongObjectObjectMap(int initCapacity) {
            initCapacity = DataIO.nextPowTwo(initCapacity);
            set = new long[initCapacity];
            values =  new Object[initCapacity*2];
        }

        public int get(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = index(key);
            if (index >= 0) {
                // key is present
                return index;
            } else {
                // key is absent
                return -1;
            }
        }


        public V1 get1(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = index(key);
            if (index >= 0) {
                // key is present
                return (V1) values[index*2];
            } else {
                // key is absent
                return null;
            }
        }

        public V2 get2(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = index(key);
            if (index >= 0) {
                // key is present
                return (V2) values[index*2+1];
            } else {
                // key is absent
                return null;
            }
        }


        int index(long key) {
            if (key != 0) {
                long[] keys = set;
                int capacityMask, index;
                long cur;
                if ((cur = keys[index = DataIO.longHash(key) & (capacityMask = keys.length - 1)]) == key) {
                    // key is present
                    return index;
                } else {
                    if (cur == 0) {
                        // key is absent
                        return -1;
                    } else {
                        while (true) {
                            if ((cur = keys[(index = (index - 1) & capacityMask)]) == key) {
                                // key is present
                                return index;
                            } else if (cur == 0) {
                                // key is absent
                                return -1;
                            }
                        }
                    }
                }
            } else {
                // key is absent
                return -1;
            }
        }

        public int put(long key, V1 val1, V2 val2) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");

            int index = insert(key, val1,val2);
            if (index < 0) {
                // key was absent
                return -1;
            } else {
                // key is present
                Object[] vals = values;
                vals[index*2] = val1;
                vals[index*2+1] = val2;
                return index;
            }
        }

        int insert(long key, V1 val1, V2 val2) {
            long[] keys = set;
            int capacityMask, index;
            long cur;
            keyAbsent:
            if ((cur = keys[index = DataIO.longHash(key) & (capacityMask = keys.length - 1)]) != 0) {
                if (cur == key) {
                    // key is present
                    return index;
                } else {
                    while (true) {
                        if ((cur = keys[(index = (index - 1) & capacityMask)]) == 0) {
                            break keyAbsent;
                        } else if (cur == key) {
                            // key is present
                            return index;
                        }
                    }
                }
            }
            // key is absent

            keys[index] = key;
            index*=2;
            values[index] = val1;
            values[index+1] = val2;
            postInsertHook();
            return -1;
        }

        void postInsertHook() {
            if (++size > maxSize) {
            /* if LHash hash */
                int capacity = set.length;
                if (!LongLongMap.isMaxCapacity(capacity)) {
                    rehash(capacity << 1);
                }
            }
        }


        void rehash(int newCapacity) {
            long[] keys = set;
            Object[] vals = values;

            maxSize = LongLongMap.maxSize(newCapacity);
            set = new long[newCapacity];
            values = new Object[newCapacity*2];

            long[] newKeys = set;
            int capacityMask = newKeys.length - 1;
            Object[] newVals = values;
            for (int i = keys.length - 1; i >= 0; i--) {
                long key;
                if ((key = keys[i]) != 0) {
                    int index;
                    if (newKeys[index = DataIO.longHash(key) & capacityMask] != 0) {
                        while (true) {
                            if (newKeys[(index = (index - 1) & capacityMask)] == 0) {
                                break;
                            }
                        }
                    }
                    newKeys[index] = key;
                    newVals[index*2] = vals[i*2];
                    newVals[index*2+1] = vals[i*2+1];
                }
            }
        }


        public void clear() {
            size = 0;
            Arrays.fill(set,0);
            Arrays.fill(values,null);
        }

        public int  remove(long key) {
            if(CC.PARANOID && key==0)
                throw new IllegalArgumentException("zero key");
            long[] keys = set;
            int capacityMask = keys.length - 1;
            int index;
            long cur;
            keyPresent:
            if ((cur = keys[index = DataIO.longHash(key) & capacityMask]) != key) {
                if (cur == 0) {
                    // key is absent
                    return -1;
                } else {
                    while (true) {
                        if ((cur = keys[(index = (index - 1) & capacityMask)]) == key) {
                            break keyPresent;
                        } else if (cur == 0) {
                            // key is absent
                            return -1;
                        }
                    }
                }
            }
            // key is present
            Object[] vals = values;
            int val = index;

            int indexToRemove = index;
            int indexToShift = indexToRemove;
            int shiftDistance = 1;
            while (true) {
                indexToShift = (indexToShift - 1) & capacityMask;
                long keyToShift;
                if ((keyToShift = keys[indexToShift]) == 0) {
                    break;
                }
                if (((DataIO.longHash(keyToShift) - indexToShift) & capacityMask) >= shiftDistance) {
                    keys[indexToRemove] = keyToShift;
                    vals[indexToRemove] = vals[indexToShift];
                    indexToRemove = indexToShift;
                    shiftDistance = 1;
                } else {
                    shiftDistance++;
                    if (indexToShift == 1 + index) {
                        throw new java.util.ConcurrentModificationException();
                    }
                }
            }
            keys[indexToRemove] = 0;
            indexToRemove*=2;
            vals[indexToRemove] = null;
            vals[indexToRemove+1] = null;

            //post remove hook
            size--;

            return val;
        }
    }

    @Override
    public Engine getWrappedEngine() {
        return null;
    }
}
