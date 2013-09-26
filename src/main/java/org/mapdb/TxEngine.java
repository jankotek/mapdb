package org.mapdb;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Naive implementation of Snapshots on top of StorageEngine.
 * On update it takes old value and stores it aside.
 * <p/>
 * TODO merge snapshots down with Storage for best performance
 *
 * @author Jan Kotek
 */
public class TxEngine extends EngineWrapper{

    protected static final Object TOMBSTONE = new Object();

    /** write lock:
     *  - `TX.modified` and `TX.old` are updated
     *  - commits and rollback
     *  read lock on everything else
     */
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    protected final Set<Reference<TX>> txs = Collections.synchronizedSet(new HashSet<Reference<TX>>());

    /** true if there are data which can be rolled back */
    protected boolean uncommitedData = false;

    protected final boolean fullTx;

    protected TxEngine(Engine engine, boolean fullTx) {
        super(engine);
        this.fullTx = fullTx;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        lock.writeLock().lock();
        try{
            uncommitedData = true;
            long recid = super.put(value, serializer);
            for(Reference<TX> r:txs){
                TX tx = r.get();
                if(tx==null) continue; //TODO remove expired refs
                tx.old.putIfAbsent(recid,TOMBSTONE);
            }
            return recid;
        }finally{
            lock.writeLock().unlock();
        }
    }


    @Override
    public <A> A get(long recid, Serializer<A> serializer) {
//        lock.readLock().lock();
//        try{
            return super.get(recid, serializer);
//        }finally{
//            lock.readLock().unlock();
//        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        lock.writeLock().lock();
        try{
            updateNoLock(recid, value, serializer);
        }finally{
            lock.writeLock().unlock();
        }
    }

    private <A> void updateNoLock(long recid, A value, Serializer<A> serializer) {
        uncommitedData = true;
        Object old = get(recid,serializer);
        if(old == null) old=TOMBSTONE;
        for(Reference<TX> r:txs){
            TX tx = r.get();
            if(tx==null) continue; //TODO remove expired refs
            tx.old.putIfAbsent(recid,old);
        }
        super.update(recid, value, serializer);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        lock.writeLock().lock();
        try{
            boolean ret =  super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
            if(ret){
                uncommitedData = true;
                for(Reference<TX> r:txs){
                    TX tx = r.get();
                    if(tx==null) continue; //TODO remove expired refs
                    tx.old.putIfAbsent(recid,expectedOldValue);
                }
            }
            return ret;
        }finally{
            lock.writeLock().unlock();
        }
    }

    @Override
    public <A> void delete(long recid, Serializer<A> serializer) {
        lock.writeLock().lock();
        try{
            deleteNoLock(recid, serializer);
        }finally{
            lock.writeLock().unlock();
        }
    }

    private <A> void deleteNoLock(long recid, Serializer<A> serializer) {
        uncommitedData = true;
        Object old = get(recid,serializer);
        if(old == null) old=TOMBSTONE;
        for(Reference<TX> r:txs){
            TX tx = r.get();
            if(tx==null) continue; //TODO remove expired refs
            tx.old.putIfAbsent(recid,old);
        }
        super.delete(recid, serializer);
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try{
            super.close();
        }finally{
            lock.writeLock().unlock();
        }
    }

    @Override
    public void commit() {
        lock.writeLock().lock();
        try{
            super.commit();
            uncommitedData = false;
        }finally{
            lock.writeLock().unlock();
        }
    }

    @Override
    public void rollback() {
        lock.writeLock().lock();
        try{
            super.rollback();
            uncommitedData = false;
        }finally{
            lock.writeLock().unlock();
        }
    }

    public static Engine createSnapshotFor(Engine engine) {
        while(true){
            if(engine instanceof TxEngine){
                return ((TxEngine) engine).snapshot();
            }else if(engine instanceof EngineWrapper){
                engine = ((EngineWrapper)engine).getWrappedEngine();
            }else{
                throw new IllegalArgumentException("Could not create Snapshot for Engine: "+engine);
            }
        }
    }


    public Engine snapshot() {
        lock.writeLock().lock();
        try{
            if(uncommitedData && canRollback()){
                //TODO we can not create snapshot if user can rollback data, it would ruin consistency
                throw new IllegalAccessError("Can not create snapshot with uncommited data");
            }
            return new TX();
        }finally{
            lock.writeLock().unlock();
        }
    }

    public class TX implements Engine{

        public TX(){
            TxEngine.this.txs.add(ref);
        }

        protected final Reference<TX> ref = new WeakReference<TX>(TX.this);

        protected final LongConcurrentHashMap<Object> old = new LongConcurrentHashMap<Object>();

        protected final LongConcurrentHashMap<Fun.Tuple2<Object,Serializer>> modified =
                fullTx ? new LongConcurrentHashMap<Fun.Tuple2<Object,Serializer>>() : null;


        @Override
        public long preallocate() {
            //TODO does not respect TX
            return TxEngine.this.preallocate();
        }

        @Override
        public void preallocate(long[] recids) {
            //TODO does not respect TX
            TxEngine.this.preallocate(recids);
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            checkFullTx();
            lock.writeLock().lock();
            try{
                //put null into underlying engine
                long recid = TxEngine.this.preallocate();
                modified.put(recid,Fun.t2((Object)value,(Serializer)serializer));
                return recid;
                //TODO remove empty recid on rollback
            }finally{
                lock.writeLock().unlock();
            }
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            lock.readLock().lock();
            try{
                return getNoLock(recid, serializer);
            }finally{
                lock.readLock().unlock();
            }
        }

        private <A> A getNoLock(long recid, Serializer<A> serializer) {
            Fun.Tuple2 o = fullTx ? modified.get(recid) : null;
            if(o!=null) return o.a==TOMBSTONE ? null : (A) o.a;
            Object o2 = old.get(recid);
            if(o2 == TOMBSTONE) return null;
            if(o2!=null) return (A) o2;
            return TxEngine.this.get(recid, serializer);
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            checkFullTx();
            lock.writeLock().lock();
            try{
                if(old.containsKey(recid)) throw new TxRollbackException();
                modified.put(recid,Fun.t2((Object)value,(Serializer)serializer));
            }finally{
                lock.writeLock().unlock();
            }
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            checkFullTx();
            lock.writeLock().lock();
            try{
                Object oldObj = getNoLock(recid,serializer);
                if(oldObj==expectedOldValue || (oldObj!=null && oldObj.equals(expectedOldValue))){
                    if(old.containsKey(recid)) throw new TxRollbackException();
                    modified.put(recid,Fun.t2((Object)newValue,(Serializer)serializer));
                    return true;
                }
                return false;
            }finally{
                lock.writeLock().unlock();
            }
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            checkFullTx();
            lock.writeLock().lock();
            try{
                if(old.containsKey(recid)) throw new TxRollbackException();
                modified.put(recid,Fun.t2(TOMBSTONE,(Serializer)serializer));
            }finally{
                lock.writeLock().unlock();
            }
        }

        protected void checkFullTx() {
            if(!fullTx) throw new UnsupportedOperationException("read-only snapshot");
        }

        @Override
        public void close() {
            ref.clear();
            TxEngine.this.txs.remove(ref);
            if(fullTx){
                modified.clear();
            }
            old.clear();
        }

        @Override
        public boolean isClosed() {
            return ref.get()!=null;
        }

        @Override
        public void commit() {
            checkFullTx();
            lock.writeLock().lock();
            try{
                LongMap.LongMapIterator<Fun.Tuple2<Object,Serializer>> iter = modified.longMapIterator();
                while(iter.moveToNext()){
                    long recid = iter.key();
                    if(old.containsKey(recid)){
                        TxEngine.this.rollback();
                        throw new TxRollbackException();
                    }
                    Fun.Tuple2<Object,Serializer> val = iter.value();
                    if(val.a==TOMBSTONE){
                        TxEngine.this.deleteNoLock(recid,val.b);
                    }else {
                        TxEngine.this.updateNoLock(recid, val.a, val.b);
                    }
                }
                TxEngine.this.commit();
                close();
            }finally{
                lock.writeLock().unlock();
            }
        }

        @Override
        public void rollback() throws UnsupportedOperationException {
            checkFullTx();
            lock.writeLock().lock();
            try{
                modified.clear();
                close();
            }finally{
                lock.writeLock().unlock();
            }
        }

        @Override
        public boolean isReadOnly() {
            return !fullTx;
        }

        @Override
        public boolean canRollback() {
            return !fullTx;
        }

        @Override
        public void clearCache() {
            //nothing to do
        }

        @Override
        public void compact() {
            //nothing to do
        }

        @Override
        public SerializerPojo getSerializerPojo() {
            //TODO make readonly if needed
            return TxEngine.this.getSerializerPojo();
        }
    }
}
