package org.mapdb;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    protected final Locks.RecidLocks locks = new Locks.LongHashMapRecidLocks();

    protected final static Object NOT_EXIST = new Object();
    protected final static Object NOT_INIT_YET = new Object();


    protected final Map<Snapshot, String> snapshots = new ConcurrentHashMap<Snapshot, String>();


    protected SnapshotEngine(Engine engine) {
        super(engine);
    }

    public Engine snapshot() {
        return new Snapshot();
    }

    /** protects <code>snapshot</code> when modified */
    protected final ReentrantReadWriteLock snapshotsLock = new ReentrantReadWriteLock();

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        long recid = super.put(value, serializer);
        locks.lock(recid);
        try{
            for(Snapshot s:snapshots.keySet()){
                s.oldValues.putIfAbsent(recid, NOT_EXIST);
            }
            return recid;
        }finally{
            locks.unlock(recid);
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        locks.lock(recid);
        try{
            boolean ret =  super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
            if(ret==true){
                for(Snapshot s:snapshots.keySet()){
                    s.oldValues.putIfAbsent(recid, expectedOldValue);
                }
            }
            return ret;
        }finally{
            locks.unlock(recid);
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        locks.lock(recid);
        try{
            Object val = NOT_INIT_YET;
            for(Snapshot s:snapshots.keySet()){
                if(s.oldValues.get(recid)==null){
                    if(val == NOT_INIT_YET)
                        val = get(recid, serializer);
                    s.oldValues.put(recid,val);
                }
            }

            super.update(recid, value, serializer);
        }finally{
            locks.unlock(recid);
        }
    }

    @Override
    public  <A> void delete(long recid, Serializer<A> serializer) {
        locks.lock(recid);
        try{
            Object val = NOT_INIT_YET;
            for(Snapshot s:snapshots.keySet()){
                if(s.oldValues.get(recid)==null){
                    if(val == NOT_INIT_YET)
                        val = get(recid, serializer);
                    s.oldValues.put(recid,val);
                }
            }

            super.delete(recid,serializer);
        }finally{
            locks.unlock(recid);
        }
    }

    public static Engine createSnapshotFor(Engine engine) {
        SnapshotEngine se = null;
        while(true){
            if(engine instanceof SnapshotEngine){
                se = (SnapshotEngine) engine;
                break;
            }else if(engine instanceof EngineWrapper){
                engine = ((EngineWrapper)engine).getWrappedEngine();
            }else{
                throw new IllegalArgumentException("Could not create Snapshot for Engine: "+engine);
            }
        }

        return se.snapshot();
    }

    protected class Snapshot extends ReadOnlyEngine{

        protected LongConcurrentHashMap oldValues = new LongConcurrentHashMap();

        public Snapshot() {
            super(SnapshotEngine.this);
            snapshots.put(Snapshot.this, "");
        }


        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            locks.lock(recid);
            try{
                Object ret = oldValues.get(recid);
                if(ret!=null){
                    if(ret==NOT_EXIST) return null;
                    return (A) ret;
                }
                return SnapshotEngine.this.getWrappedEngine().get(recid, serializer);
            }finally{
                locks.unlock(recid);
            }
        }

        @Override
        public boolean isClosed() {
           return oldValues!=null;
        }

        @Override
        public void close() {
            snapshots.remove(Snapshot.this);
            oldValues.clear();
        }
    }
}
