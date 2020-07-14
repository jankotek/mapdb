package org.mapdb.store;

import org.javatuples.Pair;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DBException;
import org.mapdb.ser.Serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StoreTxWrap {

    protected static final Object PREALLOC = new  Object();
    protected static final Pair<Object,Serializer> PREALLOC_PAIR = new Pair(PREALLOC, null);

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    protected final Map<Long,Wrap> readRecids = new ConcurrentHashMap<>();
    protected final Map<Long,Wrap> modifiedRecids =
            new ConcurrentHashMap<>();

    private final Store store;

    public StoreTxWrap(Store store) {
        this.store = store;
    }

    public Store newTx() {
        return new Wrap();
    }

    public void txStart() {

    }

    public void commit() {

    }

    public class Wrap implements StoreTx {

        protected final ConcurrentHashMap<Long,Pair<Object, Serializer>> mods  = new ConcurrentHashMap();
        protected final ConcurrentHashMap<Long, Wrap> deleteOnRollback = new ConcurrentHashMap<>();

        @Override
        public long preallocate() {
            lock.writeLock().lock();
            try{
                long recid = store.preallocate();
                modifiedRecids.put(recid, Wrap.this);
                mods.put(recid, PREALLOC_PAIR);
                return recid;
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> void preallocatePut(long recid, @NotNull Serializer<R> serializer, @NotNull R record) {
            boolean replaced = mods.replace(recid, PREALLOC_PAIR, new Pair(record, serializer));
            if(!replaced)
                throw new DBException.RecordNotPreallocated();
        }

        @Override
        public <R> @NotNull long put(@NotNull R record, @NotNull Serializer<R> serializer) {
            lock.writeLock().lock();
            try {
                long recid = store.preallocate();
                deleteOnRollback.put(recid, Wrap.this);
                modifiedRecids.put(recid, Wrap.this);
                mods.put(recid, new  Pair(record, serializer));
                return recid;
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> void update(long recid, @NotNull Serializer<R> serializer, @NotNull R updatedRecord) {
            lock.writeLock().lock();
            try{
                modifiedRecids.put(recid, Wrap.this);
                mods.put(recid, new Pair(updatedRecord, serializer));
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void verify() {

        }

        @Override
        public void commit() {

        }

        @Override
        public void compact() {

        }

        @Override
        public boolean isThreadSafe() {
            return true;
        }

        @Override
        public <R> void updateAtomic(long recid, @NotNull Serializer<R> serializer, @NotNull Transform<R> r) {
            lock.writeLock().lock();
            try{
                modifiedRecids.put(recid, Wrap.this);
                //FIXME
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> boolean compareAndUpdate(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord, @NotNull R updatedRecord) {
            lock.writeLock().lock();
            try{
                modifiedRecids.put(recid, Wrap.this);
                mods.put(recid, new Pair(updatedRecord, serializer));

                //FIXME
                return false;
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> boolean compareAndDelete(long recid, @NotNull Serializer<R> serializer, @NotNull R expectedOldRecord) {
            lock.writeLock().lock();
            try{
                modifiedRecids.put(recid, Wrap.this);
                //FIXME
                return false;
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> void delete(long recid, @NotNull Serializer<R> serializer) {
            lock.writeLock().lock();
            try{
                //FIXME
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public <R> @NotNull R getAndDelete(long recid, @NotNull Serializer<R> serializer) {
            return null;
        }

        @Override
        public void rollback() {

        }

        @Override
        public <K> @NotNull K get(long recid, @NotNull Serializer<K> ser) {
            lock.writeLock().lock();
            try{
                Pair<Object,Serializer> ret = mods.get(recid);
                if(ret!=null) //TODO prealloc and delete
                    return (K) ret.getValue0();
                //TODO modified recids
                return store.get(recid, ser);
            }finally {
                lock.writeLock().unlock();
            }
        }

        @Override
        public void close() {

        }

        @Override
        public void getAll(@NotNull GetAllCallback callback) {

        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }
}
