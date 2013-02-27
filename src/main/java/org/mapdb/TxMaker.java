package org.mapdb;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMaker {

    protected static final Fun.Tuple2<Object, Serializer> DELETED = new Fun.Tuple2(null, Serializer.STRING_SERIALIZER);

    protected Engine engine;

    protected final Object lock = new Object();

    protected final LongMap<TxEngine> globalMod = new LongHashMap<TxEngine>();


    public TxMaker(Engine engine) {
        if(engine==null) throw new IllegalArgumentException();
        this.engine = engine;
    }

    
    public DB makeTx(){
        return new DB(new TxEngine(engine));
    }

    public void close() {
        if(engine==null)
            engine.close();
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

        protected LongMap<Fun.Tuple2<?, Serializer>> modItems =
                new LongHashMap<Fun.Tuple2<?, Serializer>>();

        protected Set<Long> newItems = new LinkedHashSet<Long>();


        protected TxEngine(Engine engine) {
            super(engine);
        }

        @Override
        public <A> long put(A value, Serializer<A> serializer) {
            if(isClosed()) throw new IllegalAccessError("already closed");
            synchronized (lock){
                long recid = engine.put(Utils.EMPTY_STRING, Serializer.EMPTY_SERIALIZER);
                newItems.add(recid);
                modItems.put(recid, Fun.t2(value, (Serializer)serializer));
                globalMod.put(recid, TxEngine.this);
                return recid;
            }
        }

        @Override
        public <A> A get(long recid, Serializer<A> serializer) {
            if(isClosed()) throw new IllegalAccessError("already closed");
            synchronized (lock){
                Fun.Tuple2 t = modItems.get(recid);
                if(t!=null){
                    return (A) t.a;
                    //TODO compare serializers?
                }else{
                    return super.get(recid, serializer);
                }
            }
        }

        @Override
        public <A> void update(long recid, A value, Serializer<A> serializer) {
            if(isClosed()) throw new IllegalAccessError("already closed");
            synchronized (lock){
                TxEngine other = globalMod.get(recid);
                if(other!=null && other!=TxEngine.this) {
                    rollback();
                    throw new TxRollbackException();
                }
                modItems.put(recid, new Fun.Tuple2(value, serializer));
                globalMod.put(recid, TxEngine.this);
            }

        }

        @Override
        public <A> boolean compareAndSwap(long recid, A expectedOldValue, A newValue, Serializer<A> serializer) {
            if(isClosed()) throw new IllegalAccessError("already closed");
            throw new IllegalAccessError("Compare and Swap not supported in Tx mode");
        }

        @Override
        public <A> void delete(long recid, Serializer<A> serializer){
            if(isClosed()) throw new IllegalAccessError("already closed");
            synchronized (lock){
                TxEngine other = globalMod.get(recid);
                if(other!=null && other!=TxEngine.this) {
                    rollback();
                    throw new TxRollbackException();
                }
                modItems.put(recid, DELETED);
                globalMod.put(recid, TxEngine.this);
            }
        }

        @Override
        public void commit() {
            synchronized (lock){
                //remove locally modified items from global list
                LongMap.LongMapIterator<Fun.Tuple2<?, Serializer>> iter = modItems.longMapIterator();
                while(iter.moveToNext()){
                    TxEngine other = globalMod.remove(iter.key());
                    if(other!=TxEngine.this) throw new InternalError();
                    Fun.Tuple2<?, Serializer> t = iter.value();
                    engine.update(iter.key(), t.a, t.b);
                }
                modItems = null;
                newItems = null;

                engine.commit();
            }

        }

        @Override
        public void rollback() {
            synchronized (lock){
                //remove locally modified items from global list
                LongMap.LongMapIterator iter = modItems.longMapIterator();
                while(iter.moveToNext()){
                    TxEngine other = globalMod.remove(iter.key());
                    if(other!=TxEngine.this) throw new InternalError();
                }
                //delete preallocated items
                for(long recid:newItems){
                    engine.delete(recid, Serializer.EMPTY_SERIALIZER);
                }
                modItems = null;
                newItems = null;
            }

        }

        @Override
        public void close() {
            rollback();
        }
    }


}
