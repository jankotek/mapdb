package org.mapdb;


/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMaker {

    /** marker for deleted records*/
    protected static final Object DELETED = new Object();

    /** parent engine under which modifications are stored */
    protected org.mapdb.TxEngine engine;


    public TxMaker(org.mapdb.TxEngine engine) {
        if(engine==null) throw new IllegalArgumentException();
        if(engine.isReadOnly()) throw new IllegalArgumentException("read only");
        if(!engine.canRollback()) throw new IllegalArgumentException("no rollback");
        this.engine = engine;
    }

    
    public DB makeTx(){
        return new DB(engine.snapshot());
    }

    public void close() {
        engine.close();
        engine = null;
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
                if(!tx.isClosed()) tx.close();
            }
        }
    }

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * This method returns result returned by txBlock.
     *
     * @param txBlock
     */
    public <A> A execute(Fun.Function1<A, DB> txBlock) {
        for(;;){
            DB tx = makeTx();
            try{
                A a = txBlock.run(tx);
                if(!tx.isClosed())
                    tx.commit();
                return a;
            }catch(TxRollbackException e){
                //failed, so try again
                if(!tx.isClosed()) tx.close();
            }
        }
    }
}
