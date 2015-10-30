package org.mapdb;


public class StoreHeapTest extends EngineTest<StoreHeap>{


    static public class WithSnapshot extends StoreHeapTest{
        @Override
        protected StoreHeap openEngine() {
            return new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,true);
        }
    }

    @Override
    protected StoreHeap openEngine() {
        return new StoreHeap(true,CC.DEFAULT_LOCK_SCALE,0,false);
    }

    @Override boolean canReopen(){return false;}

    @Override boolean canRollback(){return false;}


}
