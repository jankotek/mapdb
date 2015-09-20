package org.mapdb;


public class StoreCached2_Test extends StoreDirect2_BaseTest{
    @Override
    protected StoreDirect2 openStore(String file) {
        return new StoreCached2(file);
    }
}
