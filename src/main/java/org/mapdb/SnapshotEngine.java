package org.mapdb;

import java.io.IOError;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Naive implementation of Snapshots on top of StorageEngine.
 * On update it takes old value and stores it aside.
 * <p/>
 * TODO merge snapshots down with Storage for best performance
 * TODO better concurrent scalability (uses single lock right now)
 *
 * @author Jan Kotek
 */
public class SnapshotEngine extends EngineWrapper{



    protected final Object lock = new Object();

    protected final int cacheSize;

    protected final static Object NOT_EXIST = new Object();
    protected final static Object NOT_INIT_YET = new Object();


    protected final Set<Snapshot> snapshots = new HashSet<Snapshot>();


    protected SnapshotEngine(Engine engine, int cacheSize) {
        super(engine);
        this.cacheSize = cacheSize;
    }

    public Engine snapshot() {
        return new Snapshot();
    }

    /** protects <code>snapshot</code> when modified */
    protected final ReentrantReadWriteLock snapshotsLock = new ReentrantReadWriteLock();

    @Override
    public <A> long put(A value, Serializer<A> serializer) {
        synchronized (this){
            long recid = super.put(value, serializer);
            for(Snapshot s:snapshots){
                if(s.oldValues.get(recid)==null){
                    s.oldValues.put(recid, NOT_EXIST);
                }
            }
            return recid;
        }
    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        synchronized (lock){
            boolean ret =  super.compareAndSwap(recid, expectedOldValue, newValue, serializer);
            if(ret==true){
                for(Snapshot s:snapshots){
                    if(s.oldValues.get(recid)==null){
                        s.oldValues.put(recid, expectedOldValue);
                    }
                }
            }
            return ret;
        }
    }

    @Override
    public <A> void update(long recid, A value, Serializer<A> serializer) {
        synchronized (lock){
            Object val = NOT_INIT_YET;
            for(Snapshot s:snapshots){
                if(s.oldValues.get(recid)==null){
                    if(val == NOT_INIT_YET)
                        val = get(recid, serializer);
                    s.oldValues.put(recid,val);
                }
            }

            super.update(recid, value, serializer);
        }
    }

    @Override
    public  <A> void delete(long recid, Serializer<A> serializer) {
        synchronized (lock){
            Object val = NOT_INIT_YET;
            for(Snapshot s:snapshots){
                if(s.oldValues.get(recid)==null){
                    if(val == NOT_INIT_YET)
                        val = get(recid, serializer);
                    s.oldValues.put(recid,val);
                }
            }

            super.delete(recid,serializer);
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

        protected LongMap oldValues = new LongHashMap();

        public Snapshot() {
            super(SnapshotEngine.this);
            synchronized (lock){
                snapshots.add(Snapshot.this);
            }
        }


        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            synchronized (lock){
                Object ret = oldValues.get(recid);
                if(ret!=null){
                    if(ret==NOT_EXIST) return null;
                    return (A) ret;
                }
                return SnapshotEngine.this.getWrappedEngine().get(recid, serializer);
            }
        }

        @Override
        public boolean isClosed() {
           return oldValues!=null;
        }

        @Override
        public void close() {
            synchronized (lock){
                oldValues = null;
                snapshots.remove(Snapshot.this);
            }
        }
    }
}
