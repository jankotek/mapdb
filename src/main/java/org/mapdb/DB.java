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

import java.io.Closeable;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
//PERF DB uses global lock, replace it with ReadWrite lock or fine grained locking.
@SuppressWarnings("unchecked")
public class DB implements Closeable {


    protected final boolean strictDBGet;
    protected final boolean deleteFilesAfterClose;

    /** Engine which provides persistence for this DB*/
    protected Engine engine;
    /** already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking*/
    protected Map<String, WeakReference<?>> namesInstanciated = new HashMap<String, WeakReference<?>>();

    protected Map<IdentityWrapper, String> namesLookup =
            new ConcurrentHashMap<IdentityWrapper, String>();

    /** view over named records */
    protected SortedMap<String, Object> catalog;

    protected ScheduledExecutorService executor = null;

    protected SerializerPojo serializerPojo;

    protected ScheduledExecutorService metricsExecutor;
    protected ScheduledExecutorService storeExecutor;
    protected ScheduledExecutorService cacheExecutor;

    protected final Set<String> unknownClasses = new ConcurrentSkipListSet<String>();

    //TODO collection get/create should be under consistencyLock.readLock()
    protected final ReadWriteLock consistencyLock;

    /** changes object hash and equals method to use identity */
    protected static class IdentityWrapper{

        final Object o;

        public IdentityWrapper(Object o) {
            this.o = o;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(o);
        }

        @Override
        public boolean equals(Object v) {
            return ((IdentityWrapper)v).o==o;
        }
    }

    /**
     * Construct new DB. It is just thin layer over {@link Engine} which does the real work.
     * @param engine
     */
    public DB(final Engine engine){
        this(engine,false,false, null, false, null, 0, null, null, null);
    }

    public DB(
            final Engine engine,
            boolean strictDBGet,
            boolean deleteFilesAfterClose,
            ScheduledExecutorService executor,
            boolean lockDisable,
            ScheduledExecutorService metricsExecutor,
            long metricsLogInterval,
            ScheduledExecutorService storeExecutor,
            ScheduledExecutorService cacheExecutor,
            Fun.Function1<Class, String> classLoader
            ) {
        //TODO investigate dereference and how non-final field affect performance. Perhaps abandon dereference completely
//        if(!(engine instanceof EngineWrapper)){
//            //access to Store should be prevented after `close()` was called.
//            //So for this we have to wrap raw Store into EngineWrapper
//            engine = new EngineWrapper(engine);
//        }
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.deleteFilesAfterClose = deleteFilesAfterClose;
        this.executor = executor;
        this.consistencyLock = lockDisable ?
                new Store.ReadWriteSingleLock(Store.NOLOCK) :
                new ReentrantReadWriteLock();

        this.metricsExecutor = metricsExecutor==null ? executor : metricsExecutor;
        this.storeExecutor = storeExecutor;
        this.cacheExecutor = cacheExecutor;

        serializerPojo = new SerializerPojo(
                //get name for given object
                new Fun.Function1<String, Object>() {
                    @Override
                    public String run(Object o) {
                        if(o==DB.this)
                            return "$$DB_OBJECT_Q!#!@#!#@9009a09sd";
                        return getNameForObject(o);
                    }
                },
                //get object with given name
                new Fun.Function1<Object, String>() {
                    @Override
                    public Object run(String name) {
                        Object ret = get(name);
                        if(ret == null && "$$DB_OBJECT_Q!#!@#!#@9009a09sd".equals(name))
                            return DB.this;
                        return ret;
                    }
                },
                //load class catalog
                new Fun.Function1Int<SerializerPojo.ClassInfo>() {
                    @Override
                    public SerializerPojo.ClassInfo run(int index) {
                        long[] classInfoRecids = DB.this.engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        if(classInfoRecids==null || index<0 || index>=classInfoRecids.length)
                            return null;
                        return getEngine().get(classInfoRecids[index], serializerPojo.classInfoSerializer);
                    }
                },
                new Fun.Function0<SerializerPojo.ClassInfo[]>() {
                    @Override
                    public SerializerPojo.ClassInfo[] run() {
                        long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                        SerializerPojo.ClassInfo[] ret = new SerializerPojo.ClassInfo[classInfoRecids==null?0:classInfoRecids.length];
                        for(int i=0;i<ret.length;i++){
                            ret[i] = engine.get(classInfoRecids[i],serializerPojo.classInfoSerializer);
                        }
                        return ret;
                    }
                },
                //notify DB than given class is missing in catalog and should be added on next commit.
        new Fun.Function1<Void, String>() {
                    @Override public Void run(String className) {
                        unknownClasses.add(className);
                        return null;
                    }
                },
                classLoader,
                engine);
        reinit();

        if(metricsExecutor!=null && metricsLogInterval!=0){

            if(!CC.METRICS_CACHE){
                LOG.warning("MapDB was compiled without cache metrics. No cache hit/miss will be reported");
            }

            metricsExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    metricsLog();
                }
            }, metricsLogInterval, metricsLogInterval, TimeUnit.MILLISECONDS);
        }
    }

    public void metricsLog() {
        Map metrics = DB.this.metricsGet();
        String s = metrics.toString();
        LOG.info("Metrics: "+s);
    }

    public Map<String,Long> metricsGet() {
        Map ret = new TreeMap();
        Store s = Store.forEngine(engine);
        s.metricsCollect(ret);
        return Collections.unmodifiableMap(ret);
    }

    /** delete record/collection with given name*/
    synchronized public void delete(String name){
        //$DELAY$
        Object r = get(name);
        if(r instanceof Atomic.Boolean){
            engine.delete(((Atomic.Boolean)r).recid, Serializer.BOOLEAN);
        }else if(r instanceof Atomic.Integer){
            engine.delete(((Atomic.Integer)r).recid, Serializer.INTEGER);
        }else if(r instanceof Atomic.Long){
            engine.delete(((Atomic.Long)r).recid, Serializer.LONG);
        }else if(r instanceof Atomic.String){
            engine.delete(((Atomic.String)r).recid, Serializer.STRING_NOSIZE);
        }else if(r instanceof Atomic.Var){
            engine.delete(((Atomic.Var)r).recid, ((Atomic.Var)r).serializer);
        }else if(r instanceof Queue){
            //drain queue
            Queue q = (Queue) r;
            while(q.poll()!=null){
                //do nothing
            }
        }else if(r instanceof HTreeMap || r instanceof HTreeMap.KeySet){
            HTreeMap m = (r instanceof HTreeMap)? (HTreeMap) r : ((HTreeMap.KeySet)r).parent();
            m.clear();
            //$DELAY$
            //delete segments
            for(long segmentRecid:m.segmentRecids){
                engine.delete(segmentRecid, HTreeMap.DIR_SERIALIZER);
            }
        }else if(r instanceof BTreeMap || r instanceof BTreeMap.KeySet){
            BTreeMap m = (r instanceof BTreeMap)? (BTreeMap) r : (BTreeMap) ((BTreeMap.KeySet) r).m;
            //$DELAY$
            //TODO on BTreeMap recursively delete all nodes
            m.clear();

            if(m.counter!=null)
                engine.delete(m.counter.recid,Serializer.LONG);
        }

        for(String n:catalog.keySet()){
            if(!n.startsWith(name))
                continue;
            String suffix = n.substring(name.length());
            if(suffix.charAt(0)=='.' && suffix.length()>1 && !suffix.substring(1).contains("."))
                catalog.remove(n);
        }
        namesInstanciated.remove(name);
        namesLookup.remove(new IdentityWrapper(r));
    }


    /**
     * @return true if DB is closed and can no longer be used
     */
    public synchronized  boolean isClosed(){
        return engine == null || engine.isClosed();
    }

    /**
     * Commit changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#commit()
     */
    synchronized public void commit() {
        checkNotClosed();

        consistencyLock.writeLock().lock();
        try {
            //update Class Catalog with missing classes as part of this transaction
            String[] toBeAdded = unknownClasses.isEmpty() ? null : unknownClasses.toArray(new String[0]);

            //TODO if toBeAdded is modified as part of serialization, and `executor` is not null (background threads are enabled),
            // schedule this operation with 1ms delay, so it has higher chances of becoming part of the same transaction
            if (toBeAdded != null) {
                long[] classInfoRecids = engine.get(Engine.RECID_CLASS_CATALOG, Serializer.RECID_ARRAY);
                long[] classInfoRecidsOrig = classInfoRecids;
                if(classInfoRecids==null)
                    classInfoRecids = new long[0];

                int pos = classInfoRecids.length;
                classInfoRecids = Arrays.copyOf(classInfoRecids,classInfoRecids.length+toBeAdded.length);

                for (String className : toBeAdded) {
                    SerializerPojo.ClassInfo classInfo = serializerPojo.makeClassInfo(className);
                    //persist and add new recids
                    classInfoRecids[pos++] = engine.put(classInfo,serializerPojo.classInfoSerializer);
                }
                if(!engine.compareAndSwap(Engine.RECID_CLASS_CATALOG, classInfoRecidsOrig, classInfoRecids, Serializer.RECID_ARRAY)){
                    LOG.log(Level.WARNING, "Could not update class catalog with new classes, CAS failed");
                }
            }


            engine.commit();

            if (toBeAdded != null) {
                for (String className : toBeAdded) {
                    unknownClasses.remove(className);
                }
            }
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    /**
     * Rollback changes made on collections loaded by this DB
     *
     * @see org.mapdb.Engine#rollback()
     */
    synchronized public void rollback() {
        checkNotClosed();
        consistencyLock.writeLock().lock();
        try {
            engine.rollback();
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    /**
     * Perform storage maintenance.
     * Typically compact underlying storage and reclaim unused space.
     * <p>
     * NOTE: MapDB does not have smart defragmentation algorithms. So compaction usually recreates entire
     * store from scratch. This may require additional disk space.
     */
    synchronized public void compact(){
        engine.compact();
    }


    /**
     * Make readonly snapshot view of DB and all of its collection
     * Collections loaded by this instance are not affected (are still mutable).
     * You have to load new collections from DB returned by this method
     *
     * @return readonly snapshot view
     */
    synchronized public DB snapshot(){
        consistencyLock.writeLock().lock();
        try {
            Engine snapshot = TxEngine.createSnapshotFor(engine);
            return new DB(snapshot);
        }finally {
            consistencyLock.writeLock().unlock();
        }
    }

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    public  Serializer getDefaultSerializer() {
        return serializerPojo;
    }

    /**
     * @return underlying engine which takes care of persistence for this DB.
     */
    public Engine getEngine() {
        return engine;
    }

    public void checkType(String type, String expected) {
        //$DELAY$
        if(!expected.equals(type)) throw new IllegalArgumentException("Wrong type: "+type);
    }

    /**
     * Returns consistency lock which groups operation together and ensures consistency.
     * Operations which depends on each other are performed under read lock.
     * Snapshots, close etc are performed under write-lock.
     *
     * @return
     */
    public ReadWriteLock consistencyLock(){
        return consistencyLock;
    }


}
