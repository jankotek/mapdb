package org.mapdb;


/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMaker {

    /** marker for deleted records*/
    protected static final Object DELETED = new Object();
    private final boolean txSnapshotsEnabled;
    private final boolean strictDBGet;

    /** parent engine under which modifications are stored */
    protected Engine engine;

    public TxMaker(Engine engine) {
        this(engine,false,false);
    }

    public TxMaker(Engine engine, boolean strictDBGet, boolean txSnapshotsEnabled) {
        if(engine==null) throw new IllegalArgumentException();
        if(!engine.canSnapshot())
            throw new IllegalArgumentException("Snapshot must be enabled for TxMaker");
        if(engine.isReadOnly())
            throw new IllegalArgumentException("TxMaker can not be used with read-only Engine");
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.txSnapshotsEnabled = txSnapshotsEnabled;
    }

    
    public DB makeTx(){
        Engine snapshot = engine.snapshot();
        if(txSnapshotsEnabled)
            snapshot = new TxEngine(snapshot,false);
        return new DB(snapshot,strictDBGet);
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
