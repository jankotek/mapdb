package org.mapdb;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
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


    public StoreHeap(boolean txDisabled, int lockScale, int lockingStrategy){
        super(null,null,null,lockScale, 0, false,false,null,false);
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


        for(long recid=1;recid<=RECID_LAST_RESERVED;recid++){
            data[lockPos(recid)].put(recid,NULL);
        }
    }


    @Override
    protected <A> A get2(long recid, Serializer<A> serializer) {
        if(CC.PARANOID)
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
            if(rollback!=null){
                LongObjectMap rol = rollback[pos];
                if(rol.get(recid)==null)
                    rol.put(recid,old);
            }
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

        if(CC.PARANOID)
            assertWriteLocked(pos);

        Object old = data[pos].put(recid,TOMBSTONE);

        if(rollback!=null){
            LongObjectMap rol = rollback[pos];
            if(rol.get(recid)==null)
                rol.put(recid,old);
        }

    }

    @Override
    public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
        if(serializer==null)
            throw new NullPointerException();
        if(closed)
            throw new IllegalAccessError("closed");

        final int lockPos = lockPos(recid);
        final Lock lock = locks[lockPos].writeLock();
        lock.lock();
        try{
            A oldVal = get2(recid, serializer);
            if(oldVal==expectedOldValue || (oldVal!=null && serializer.equals(oldVal,expectedOldValue))){
                Object newValue2 = newValue==null?NULL:newValue;
                Object old = data[lockPos].put(recid,newValue2);

                if(rollback!=null){
                    LongObjectMap rol = rollback[lockPos];
                    if(rol.get(recid)==null)
                        rol.put(recid,old);
                }

                return true;
            }
            return false;
        }finally {
            lock.unlock();
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
    public boolean canSnapshot() {
        return false;
    }

    @Override
    public Engine snapshot() throws UnsupportedOperationException {
        return null;
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
                            m.remove(m.set[i]);

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
}
