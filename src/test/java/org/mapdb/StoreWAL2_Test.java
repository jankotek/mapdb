package org.mapdb;


public class StoreWAL2_Test extends StoreDirect2_BaseTest{
    @Override
    protected StoreDirect2 openStore(String file) {
        return new StoreWAL2(file);
    }
}
