package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Naive implementation of Snapshots on top of StorageEngine.
 * On update it takes old value and stores it aside.
 * <p/>
 * TODO merge snapshots down with Storage for best performance
 *
 * @author Jan Kotek
 */
public class SnapshotEngine extends EngineWrapper{

    protected final static byte[] NOT_EXIST = new byte[0];

    private final int cacheSize;

    public SnapshotEngine(Engine engine, int cacheSize) {
        super(engine);
        this.cacheSize = cacheSize;
    }

    /** contains currently opened snapshots, Snapshot is removed from here after it was closed */
    protected Collection<Snapshot> snapshots = new HashSet<Snapshot>();

    /** protects <code>snapshot</code> when modified */
    protected final ReentrantReadWriteLock snapshotsLock = new ReentrantReadWriteLock();

    @Override
    public <A> long recordPut(A value, Serializer<A> serializer) {
        long ret = super.recordPut(value, serializer);
        snapshotsLock.readLock().lock();
        try{

            for(Snapshot s:snapshots){
                s.oldRecords.putIfAbsent(ret, NOT_EXIST);
            }
            return ret;
        }finally{
            snapshotsLock.readLock().unlock();
        }
    }

    @Override
    public <A> void recordUpdate(long recid, A value, Serializer<A> serializer) {
        updateOldRec(recid);
        super.recordUpdate(recid, value, serializer);
    }

    private void updateOldRec(long recid) {
        snapshotsLock.readLock().lock();
        try{
            byte[] prevValue = NOT_EXIST;
            for(Snapshot s:snapshots){
                if(prevValue == NOT_EXIST){
                    if(!s.oldRecords.containsKey(recid)){
                        prevValue = super.recordGet(recid, Serializer.BYTE_ARRAY_SERIALIZER);
                        s.oldRecords.putIfAbsent(recid, prevValue);
                    }
                }else{
                    s.oldRecords.putIfAbsent(recid, prevValue);
                }
            }
        }finally{
            snapshotsLock.readLock().unlock();
        }
    }

    @Override
    public <A> boolean recordCompareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        updateOldRec(recid);
        return super.recordCompareAndSwap(recid, expectedOldValue, newValue, serializer);
    }

    @Override
    public void recordDelete(long recid) {
        updateOldRec(recid);
        super.recordDelete(recid);
    }


    public Engine snapshot(){
        snapshotsLock.writeLock().lock();
        try{
            Engine ret = new Snapshot(this);
            snapshots.add((Snapshot) ret);
            if(cacheSize>0){
                ret = new CacheHashTable(ret, cacheSize);
                ret = new ReadOnlyEngine(ret);
            }
            //find async write engine if enabled, and flush write queue
            EngineWrapper e = this;
            while(e.getWrappedEngine() instanceof EngineWrapper){
                e = (EngineWrapper) e.getWrappedEngine();
            }

            return ret;
        }finally {
            snapshotsLock.writeLock().unlock();
        }
    }

    protected static class Snapshot extends ReadOnlyEngine{

        protected LongConcurrentHashMap<byte[]> oldRecords = new LongConcurrentHashMap<byte[]>();

        protected Snapshot(SnapshotEngine engine) {
            super(engine);
        }

        @Override
        public <A> A recordGet(long recid, Serializer<A> serializer) {
            byte[] b = oldRecords.get(recid);
            if(b==NOT_EXIST) return null;
            else if(b==null) return super.recordGet(recid, serializer);
            else{
                DataInput2 in = new DataInput2(b);
                try {
                    return serializer.deserialize(in, b.length);
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
        }

        @Override
        public void close() {
            SnapshotEngine se = (SnapshotEngine) engine;
            se.snapshotsLock.writeLock().lock();
            try{
                se.snapshots.remove(this);
            }finally {
                se.snapshotsLock.writeLock().unlock();
            }
            engine = null;
            oldRecords = null;
        }
    }

    @Override
    public void close() {
        boolean locked = snapshotsLock.isWriteLocked();
        if(!locked)snapshotsLock.readLock().lock();
        try{
            for(Snapshot s:snapshots){
                s.close();
            }
        }finally {
            snapshots = null;
            if(!locked)snapshotsLock.readLock().unlock();
        }
        super.close();
    }

    public static Engine createSnapshotFor(Engine engine){
        Engine engineOrig = engine;
        while(!(engine instanceof SnapshotEngine)){
            if(engine instanceof EngineWrapper){
                engine = ((EngineWrapper)engine).getWrappedEngine();
            }else{
                throw new InternalError("Could not create snapshot for Engine: "+engineOrig);
            }
        }
        return ((SnapshotEngine)engine).snapshot();
    }


}
