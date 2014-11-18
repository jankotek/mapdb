package org.mapdb;


public class StoreHeapTest extends EngineTest<StoreHeap>{


    @Override
    protected StoreHeap openEngine() {
        return new StoreHeap(true);
    }

    @Override boolean canReopen(){return false;}

    @Override boolean canRollback(){return false;}


}
