package org.mapdb;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Store which keeps all instances on heap. It does not use serialization.
 */

public class StoreHeap extends Store{

    protected final LongObjectMap[] data;
    protected final LongObjectMap[] rollback;

    protected static final Object TOMBSTONE = new Object();
    protected static final Object NULL = new Object();

    protected long[] freeRecid;
    protected int freeRecidTail;
    protected long maxRecid = RECID_FIRST;
    protected final Lock newRecidLock;
    protected List<Snapshot> snapshots;


    public StoreHeap(boolean txDisabled, int lockScale, int lockingStrategy, boolean snapshotEnable){
        super(null,null,null,lockScale, 0, false,false,null,false, snapshotEnable);
        data = new LongObjectMap[this.lockScale];
        for(int i=0;i<data.length;i++){
            data[i] = new LongObjectMap();
        }

        if(txDisabled){
            rollback = null;
        }else {
            rollback = new LongObjectMap[this.lockScale];
            for (int i = 0; i < rollback.length; i++) {
                rollback[i] = new LongObjectMap();
            }
        }

        newRecidLock = lockingStrategy==LOCKING_STRATEGY_NOLOCK?
               NOLOCK : new ReentrantLock(CC.FAIR_LOCKS);
        freeRecid = new long[16];
        freeRecidTail=0;

        snapshots = snapshotEnable?
                new CopyOnWriteArrayList<Snapshot>():
                null;

        for(long recid=1;recid<=RECID_LAST_RESERVED;recid++){
            data[lockPos(recid)].put(recid,NULL);
        }
    }


    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if(CC.ASSERT)
            assertReadLocked(recid);

        int pos = lockPos(recid);
        A ret =  (A) data[pos].get(recid);
        if(ret == null)
            throw new DBException.EngineGetVoid();
        if(ret == TOMBSTONE||ret==NULL)
            ret = null;
        return ret;
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");

        Object val2 = value==null?NULL:value;

        int pos = lockPos(recid);
        LongObjectMap data2 = data[pos];
        Lock lock = locks[pos].writeLock();
        lock.lock();
        try{
            Object old = data2.put(recid,val2);
            updateOld(pos, recid, old);
        }finally {
            lock.unlock();
        }
    }

    @Override
    protected void update2(long recid, DataIO.DataOutputByteArray out) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected <A> void delete2(long recid, Serializer<A> serializer) {
        int pos = lockPos(recid);

        if(CC.ASSERT)
            assertWriteLocked(pos);

        Object old = data[pos].put(recid,TOMBSTONE);
        updateOld(pos,recid,old);
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");

        final int pos = lockPos(recid);
        final Lock lock = locks[pos].writeLock();
        lock.lock();
        try{
            A oldVal = get2(recid, serializer);
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                Object newValue2 = newValue==null?NULL:newValue;
                Object old = data[pos].put(recid, newValue2);

                updateOld(pos, recid, old);


                return true;
            }
            return false;
        }finally {
            lock.unlock();
        }
    }

    protected void updateOld(int pos, long recid, Object old) {
        if(rollback!=null){
            LongObjectMap rol = rollback[pos];
            if(rol.get(recid)==null)
                rol.put(recid,old);
        }
        if(snapshots!=null){
            for(Snapshot snap:snapshots){
                snap.oldData[pos].putIfAbsent(recid, old);
            }
        }
    }

    @Override
    public long getCurrSize() {
        return -1;
    }

    @Override
    public long getFreeSize() {
        return -1;
    }

    @Override
    public long preallocate() {
        if(closed)
            throw new IllegalAccessError("closed");

        long recid = allocateRecid();
        int lockPos = lockPos(recid);
        Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try{
            data[lockPos].put(recid,NULL);

            if(rollback!=null){
                LongObjectMap rol = rollback[lockPos];
                if(rol.get(recid)==null)
                    rol.put(recid,TOMBSTONE);
            }

        }finally {
            lock.unlock();
        }
        return recid;
    }

    protected long allocateRecid() {
        long recid;
        newRecidLock.lock();
        try {
            if(freeRecidTail>0) {
                //take from stack of free recids
                freeRecidTail--;
                recid = freeRecid[freeRecidTail];
                freeRecid[freeRecidTail]=0;
            }else{
                //allocate new recid
                recid = maxRecid++;
            }

        }finally {
            newRecidLock.unlock();
        }
        return recid;
    }

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        if(closed)
            throw new IllegalAccessError("closed");

        long recid = allocateRecid();
        update(recid, value, serializer);
        return recid;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public void commit() {
        if(closed)
            throw new IllegalAccessError("closed");

        if(rollback!=null) {
            commitLock.lock();
            try {
                for (int i = 0; i < data.length; i++) {
                    Lock lock = locks[i].writeLock();
                    lock.lock();
                    try {
                        rollback[i].clear();
                    }finally {
                        lock.unlock();
                    }
                }
            } finally {
                commitLock.unlock();
            }
        }
    }

    @Override
    public void rollback() throws UnsupportedOperationException {
        if(closed)
            throw new IllegalAccessError("closed");

        if(rollback==null)
            throw new UnsupportedOperationException();

        commitLock.lock();
        try{
            for (int i = 0; i < data.length; i++) {
                Lock lock = locks[i].writeLock();
                lock.lock();
                try {
                    //move content of rollback map into primary map
                    LongObjectMap r = rollback[i];
                    LongObjectMap d = data[i];

                    long[] rs = r.set;
                    Object[] rv = r.values;
                    for(int j=0;j<rs.length;j++){
                        long recid = rs[j];
                        if(recid==0)
                            continue;

                        Object val = rv[j];
                        if(val==TOMBSTONE)
                            d.remove(recid);
                        else
                            d.put(recid,val);
                    }

                    r.clear();
                }finally {
                    lock.unlock();
                }
            }
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public boolean canRollback() {
        return rollback!=null;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        if(snapshots==null)
            throw new UnsupportedOperationException();
        return new Snapshot(StoreHeap.this);
    }

    @Override
    public void compact() {
        commitLock.lock();
        try{

            newRecidLock.lock();
            try{

                for(int i=0;i<locks.length;i++){
                    locks[i].writeLock().lock();
                    try{
                        LongObjectMap m = data[i];
                        for(int j=0;j<m.set.length;j++){
                            long recid = m.set[j];
                            if(recid==0 || m.values[j]!=TOMBSTONE) {
                                continue;
                            }

                            //put into list of free recids
                            m.remove(m.set[j]);

                            if(freeRecid.length==freeRecidTail){
                                freeRecid = Arrays.copyOf(freeRecid, freeRecid.length*2);
                            }
                            freeRecid[freeRecidTail++] = recid;

                        }
                    }finally {
                        locks[i].writeLock().unlock();
                    }
                }

            }finally {
                newRecidLock.unlock();
            }
        }finally {
            commitLock.unlock();
        }

    }

    public static class Snapshot extends ReadOnly {

        protected StoreHeap engine;

        protected LongObjectMap[] oldData;

        public Snapshot(StoreHeap engine) {
            this.engine = engine;
            oldData = new LongObjectMap[engine.lockScale];
            for(int i=0;i<oldData.length;i++){
                oldData[i] = new LongObjectMap();
            }
            engine.snapshots.add(Snapshot.this);
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            StoreHeap engine = this.engine;
            int pos = engine.lockPos(recid);
            Lock lock = engine.locks[pos].readLock();
            lock.lock();
            try{
                Object ret = oldData[pos].get(recid);
                if(ret==null)
                    ret = engine.get(recid,serializer);
                if(ret==TOMBSTONE)
                    return null;
                return (A) ret;
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            engine.snapshots.remove(Snapshot.this);
            engine = null;
            oldData = null;
        }

        @Override
        public boolean isClosed() {
            return engine!=null;
        }

        @Override
        public boolean canRollback() {
            return false;
        }

        @Override
        public boolean canSnapshot() {
            return true;
        }

        @Override
        public Engine snapshot() throws UnsupportedOperationException {
            return this;
        }

        @Override
        public Engine getWrappedEngine() {
            return engine;
        }

        @Override
        public void clearCache() {

        }
    }
}
