package org.mapdb;


public class StoreHeapTxTest extends EngineTest<StoreHeap>{


    @Override
    protected StoreHeap openEngine() {
        return new StoreHeap(false,CC.DEFAULT_LOCK_SCALE,0,false);
    }

    @Override boolean canReopen(){return false;}

    @Override boolean canRollback(){return true;}


}
