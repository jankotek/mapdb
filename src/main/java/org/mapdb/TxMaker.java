package org.mapdb;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMaker {

    /** marker for deleted records*/
    protected static final Object DELETED = new Object();

    /** parent engine under which modifications are stored */
    protected SnapshotEngine engine;

    /**
     * Locked when one of transactions is making commit.
     * Commits are serialized, underlying store has only single journal (and tx).
     * This lock prevents multiple transactions to mix into single store tx.
     *
     */
    protected final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

    /**
     * Lists all recids modified by opened TXs. Value is engine which made modification.
     * This way way we can check if record was modified by other TX.
     *
     * Updates must be protected by `commitLock.readLock()`
     */
    protected LongConcurrentHashMap<Engine> globalMods = new LongConcurrentHashMap<Engine>();
    protected volatile boolean commitPending;

    public TxMaker(SnapshotEngine engine) {
        if(engine==null) throw new IllegalArgumentException();
        if(engine.isReadOnly()) throw new IllegalArgumentException("read only");
        if(!engine.canRollback()) throw new IllegalArgumentException("no rollback");
        this.engine = engine;
    }

    
    public DB makeTx(){
        Engine snapshot = null;
        try{
            commitLock.readLock().lock();
            snapshot = engine.snapshot();
        }finally{
            commitLock.readLock().unlock();
        }
        return new DB(new TxEngine(snapshot));
    }

    public void close() {
        if(engine == null) return;
        commitLock.writeLock().lock();
        try{
            engine.close();
            engine = null;
            globalMods = null;
        }finally {
            commitLock.writeLock().unlock();
        }
    }

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * @param txBlock
     */
    public void execute(TxBlock txBlock) {
        for(;;){
            DB tx = makeTx();
            try{
                txBlock.tx(tx);
                if(!tx.isClosed())
                    tx.commit();
                return;
            }catch(TxRollbackException e){
                //failed, so try again
            }
        }
    }

    protected class TxEngine extends EngineWrapper{

        /** list of modifications made by this transaction */
        protected LongConcurrentHashMap<Fun.Tuple2<?, Serializer>> mods =
                new LongConcurrentHashMap<Fun.Tuple2<?, Serializer>>();

        protected LongConcurrentHashMap<Serializer> newRecids =
                new LongConcurrentHashMap<Serializer>();


        protected final ReentrantReadWriteLock[] locks = Utils.newReadWriteLocks();



        protected TxEngine(Engine snapshot) {
            super(snapshot);
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            final long recid;

            //need to get new recid from underlying engine
            commitLock.readLock().lock();
            try{
                commitPending = true;
                recid = engine.put(Utils.EMPTY_STRING, Serializer.STRING_NOSIZE);
                lockGlobalMods(recid);
            }finally {
                commitLock.readLock().unlock();
            }

            Lock lock = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
            lock.lock();
            try{
                //update local modifications
                newRecids.put(recid, serializer);
                mods.put(recid,Fun.t2(value,(Serializer)serializer));
            }finally {
                lock.unlock();
            }
            return recid;
        }

        protected void lockGlobalMods(long recid) {
            Engine other = globalMods.putIfAbsent(recid,this);
            if(other!=this && other!=null){
                rollback();
                throw new TxRollbackException();
            }
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            Lock lock = locks[Utils.longHash(recid)&Utils.LOCK_MASK].readLock();
            lock.lock();
            try{
                return getNoLock(recid, serializer);
            }finally {
                lock.unlock();
            }

        }

        private <A> A getNoLock(long recid, Serializer<A> serializer) {
            //try local mods
            Fun.Tuple2 t = mods.get(recid);
            if(t!=null){
                if(t.a == DELETED) return null;
                else return (A) t.a;
            }
            return super.get(recid,serializer);
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            lockGlobalMods(recid);
            Lock lock = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
            lock.lock();
            try{
                //update local modifications
                mods.put(recid,Fun.t2(value,(Serializer)serializer));
            }finally {
                lock.unlock();
            }
        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            final Object other = globalMods.get(recid);
            if(other!=null && other !=this){
                rollback();
                throw new TxRollbackException();
            }
            Lock lock = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
            lock.lock();
            try{
                Object oldVal = getNoLock(recid,serializer);
                if(expectedOldValue==oldVal || (expectedOldValue!=null && expectedOldValue.equals(oldVal))){
                    lockGlobalMods(recid);

                    mods.put(recid,Fun.t2(newValue,(Serializer)serializer));
                    return true;
                }
                return false;
            }finally {
                lock.unlock();
            }

        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer) {
            lockGlobalMods(recid);

            Lock lock = locks[Utils.longHash(recid)&Utils.LOCK_MASK].writeLock();
            lock.lock();
            try{
                //add marked which indicates deleted
                mods.put(recid,Fun.t2(DELETED,(Serializer)serializer));
            }finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            mods = null;
            newRecids = null;
            super.close();
        }


        @Override
        public void commit() {
            //replay all items in transactions
            for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();

            try{
                super.close();
                commitLock.writeLock().lock();
                try{
                    if(commitPending){
                        engine.commit();
                        commitPending = false;
                    }


                    while(!mods.isEmpty()){
                        LongMap.LongMapIterator<Fun.Tuple2<?, Serializer>> iter = mods.longMapIterator();
                        while(iter.moveToNext()){

                            final long recid = iter.key();
                            if(!globalMods.remove(recid,this)){
                                engine.rollback();
                                throw new InternalError("record was not modified by this transaction");
                            }

                            final Object value = iter.value().a;
                            final Serializer serializer = iter.value().b;

                            if(value==DELETED){
                                engine.delete(recid,serializer);
                            }else{
                                engine.update(recid,value,serializer);
                            }
                            iter.remove();
                        }
                    }
                    engine.commit();
                }finally{
                    commitLock.writeLock().unlock();
                }

            }finally{
                mods = null;
                newRecids = null;

                for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();
            }
        }

        @Override
        public void rollback() {

            for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();

            try{
                super.close();
                commitLock.writeLock().lock();
                try{
                    if(commitPending){
                        engine.commit();
                        commitPending = false;
                    }
                    LongMap.LongMapIterator<Fun.Tuple2<?, Serializer>> iter2 = mods.longMapIterator();
                    while(iter2.moveToNext()){
                        final long recid = iter2.key();
                        if(!globalMods.remove(recid,this)){
                            engine.rollback();
                            throw new InternalError("record was not modified by this transaction");
                        }
                    }


                    //remove allocated recids from store
                    LongMap.LongMapIterator<Serializer> iter = newRecids.longMapIterator();
                    while(iter.moveToNext()){
                        long recid = iter.key();

                        engine.delete(recid,iter.value());
                    }

                    engine.commit();
                }finally {
                    commitLock.writeLock().unlock();
                }

            }finally{
                mods = null;
                newRecids = null;
                for(ReentrantReadWriteLock lock:locks) lock.writeLock().lock();
            }
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

    }
}
