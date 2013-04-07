package org.mapdb;

public class StoreWALTest extends StoreDirectTest<StoreWAL>{

    Volume.Factory fac = Volume.fileFactory(false,false,Utils.tempDbFile());

    @Override
    protected StoreWAL openEngine() {
        return new StoreWAL(fac,false,false);
    }
}
