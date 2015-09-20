package org.mapdb;


public class StoreWAL2_Test extends StoreDirect2_BaseTest{
    @Override
    protected StoreDirect2 openStore() {
        return new StoreWAL2(null);
    }
}
