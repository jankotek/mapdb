
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
            props.put(Keys.cache, Keys.cache_softRef);
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


        /**
         * Tells allocator to set initial store size, when new store is created.
         * Value is rounder up to nearest multiple of 1MB or allocation increment.
         *
         * @return this builder
         */
        public Maker allocateStartSize(long size){
            props.setProperty(Keys.allocateStartSize,""+size);
            return this;
        }

        /**
         * Tells allocator to grow store with this size increment. Minimal value is 1MB.
         * Incremental size is rounded up to nearest power of two.
         *
         * @return this builder
         */
        public Maker allocateIncrement(long sizeIncrement){
            props.setProperty(Keys.allocateIncrement,""+sizeIncrement);
            return this;
        }

        /**
         * Sets class loader used to POJO serializer to load classes during deserialization.
         *
         * @return this builder
         */
        public Maker serializerClassLoader(ClassLoader classLoader ){
            this.serializerClassLoader = classLoader;
            return this;
        }

        /**
         * Register class with given Class Loader. This loader will be used by POJO deserializer to load and instantiate new classes.
         * This might be needed in OSGI containers etc.
         *
         * @return this builder
         */
        public Maker serializerRegisterClass(String className, ClassLoader classLoader ){
            if(this.serializerClassLoaderRegistry==null)
                this.serializerClassLoaderRegistry = new HashMap<String, ClassLoader>();
            this.serializerClassLoaderRegistry.put(className, classLoader);
            return this;
        }


        /**
         * Register classes with their Class Loaders. This loader will be used by POJO deserializer to load and instantiate new classes.
         * This might be needed in OSGI containers etc.
         *
         * @return this builder
         */
        public Maker serializerRegisterClass(Class... classes){
            if(this.serializerClassLoaderRegistry==null)
                this.serializerClassLoaderRegistry = new HashMap<String, ClassLoader>();
            for(Class clazz:classes) {
                this.serializerClassLoaderRegistry.put(clazz.getName(), clazz.getClassLoader());
            }
            return this;
        }



        /**
         * Allocator reuses recids immediately, that can cause problems to some data types.
         * This option disables recid reusing, until they are released by compaction.
         * This option will cause higher store fragmentation with HTreeMap, queues etc..
         *
         * @deprecated this setting might be removed before 2.0 stable release, it is very likely it will become enabled by default
         * @return this builder
         */
        public Maker allocateRecidReuseDisable(){
            props.setProperty(Keys.allocateRecidReuseDisable,TRUE);
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
                        cacheExecutor,
                        makeClassLoader());
                dbCreated = true;
                return db;
            }finally {
                //did db creation fail? in that case close engine to unlock files
                if(!dbCreated)
                    engine.close();
            }
        }

        protected Fun.Function1<Class, String> makeClassLoader() {
            if(serializerClassLoader==null &&
                    (serializerClassLoaderRegistry==null || serializerClassLoaderRegistry.isEmpty())){
                return null;
            }

            //makje defensive copies
            final ClassLoader serializerClassLoader2 = this.serializerClassLoader;
            final Map<String, ClassLoader> serializerClassLoaderRegistry2 =
                    new HashMap<String, ClassLoader>();
            if(this.serializerClassLoaderRegistry!=null){
                serializerClassLoaderRegistry2.putAll(this.serializerClassLoaderRegistry);
            }

            return new Fun.Function1<Class, String>() {
                @Override
                public Class run(String className) {
                    ClassLoader loader = serializerClassLoaderRegistry2.get(className);
                    if(loader == null)
                        loader = serializerClassLoader2;
                    if(loader == null)
                        loader = Thread.currentThread().getContextClassLoader();
                    return SerializerPojo.classForName(className, loader);
                }
            };
        }


        public TxMaker makeTxMaker(){
            props.setProperty(Keys.fullTx,TRUE);
            if(props.containsKey(Keys.cache)){
                props.remove(Keys.cache);
                LOG.warning("Cache setting was disabled. Instance Cache can not be used together with TxMaker");
            }

            snapshotEnable();
            Engine e = makeEngine();
            //init catalog if needed
            DB db = new DB(e);
            db.commit();
            return new TxMaker(e, propsGetBool(Keys.strictDBGet), executor, makeClassLoader());
        }

        /** constructs Engine using current settings */
        public Engine makeEngine(){

            if(storeExecutor==null) {
                storeExecutor = executor;
            }


            final boolean readOnly = propsGetBool(Keys.readOnly);
            final boolean fileLockDisable = propsGetBool(Keys.fileLockDisable) || propsGetBool(Keys.fileLockHeartbeatEnable);
            final String file = props.containsKey(Keys.file)? props.getProperty(Keys.file):"";
            final String volume = props.getProperty(Keys.volume);
            final String store = props.getProperty(Keys.store);

            if(readOnly && file.isEmpty())
                throw new UnsupportedOperationException("Can not open in-memory DB in read-only mode.");

            if(readOnly && !new File(file).exists() && !Keys.store_append.equals(store)){
                throw new UnsupportedOperationException("Can not open non-existing file in read-only mode.");
            }

            DataIO.HeartbeatFileLock heartbeatFileLock = null;
            if(propsGetBool(Keys.fileLockHeartbeatEnable) && file!=null && file.length()>0
                    && !readOnly){ //TODO should we lock readonly files?

                File lockFile = new File(file+".lock");
                heartbeatFileLock = new DataIO.HeartbeatFileLock(lockFile, CC.FILE_LOCK_HEARTBEAT);
                heartbeatFileLock.lock();
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

            final long allocateStartSize = propsGetLong(Keys.allocateStartSize,0L);
            final long allocateIncrement = propsGetLong(Keys.allocateIncrement,0L);
            final boolean allocateRecidReuseDisable = propsGetBool(Keys.allocateRecidReuseDisable);

            boolean cacheLockDisable = lockingStrategy!=0;
            byte[] encKey = propsGetXteaEncKey();
            final boolean snapshotEnabled =  propsGetBool(Keys.snapshots);
            if(Keys.store_heap.equals(store)) {
                engine = new StoreHeap(propsGetBool(Keys.transactionDisable), lockScale, lockingStrategy, snapshotEnabled);
            }else if(Keys.store_archive.equals(store)){
                Volume.VolumeFactory volFac = extendStoreVolumeFactory(false);
                engine = new StoreArchive(
                        file,
                        volFac,
                        true
                );
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
                        fileLockDisable,
                        heartbeatFileLock,
                        propsGetBool(Keys.transactionDisable),
                        storeExecutor,
                        allocateStartSize,
                        allocateIncrement
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
                            fileLockDisable,
                            heartbeatFileLock,
                            storeExecutor,
                            allocateStartSize,
                            allocateIncrement,
                            allocateRecidReuseDisable,
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
                            fileLockDisable,
                            heartbeatFileLock,
                            storeExecutor,
                            allocateStartSize,
                            allocateIncrement,
                            allocateRecidReuseDisable,
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
                            fileLockDisable,
                            heartbeatFileLock,
                            storeExecutor,
                            allocateStartSize,
                            allocateIncrement,
                            allocateRecidReuseDisable);
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

            if (!readOnly && propsGetBool(Keys.deleteFilesAfterClose)) {
            	engine = new Engine.DeleteFileEngine(engine, file);
            }

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
            if(prop!=null && prop.contains("64")) {
                String os = System.getProperty("os.name");
                if(os==null)
                    return false;
                os = os.toLowerCase();
                return !os.startsWith("windows");
            }
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
            boolean cleanerHackEnabled = propsGetBool(Keys.fileMmapCleanerHack);
            boolean mmapPreclearDisabled = propsGetBool(Keys.fileMmapPreclearDisable);
            if(Keys.volume_byteBuffer.equals(volume))
                return Volume.ByteArrayVol.FACTORY;
            else if(Keys.volume_directByteBuffer.equals(volume))
                return cleanerHackEnabled?
                        Volume.MemoryVol.FACTORY_WITH_CLEANER_HACK:
                        Volume.MemoryVol.FACTORY;
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
                    new Volume.MappedFileVol.MappedFileFactory(cleanerHackEnabled, mmapPreclearDisabled);
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

    /**
     * Returns Compiler Config, static settings MapDB was compiled with
     * @return Compiler Config
     */
    public static Map<String,Object> CC() throws IllegalAccessException {
        Map<String, Object> ret = new TreeMap<String, Object>();

        for (Field f : CC.class.getDeclaredFields()) {
            f.setAccessible(true);
            Object value = f.get(null);
            ret.put(f.getName(), value);
        }
        return ret;
    }
}
