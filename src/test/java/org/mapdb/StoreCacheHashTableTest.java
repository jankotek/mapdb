package org.mapdb;

import java.io.File;

public class StoreCacheHashTableTest<E extends StoreDirect> extends EngineTest<E>{

    File f = TT.tempDbFile();

    @Override protected E openEngine() {
        StoreDirect e =new StoreDirect(
                f.getPath(),
                Volume.FileChannelVol.FACTORY,
                new Store.Cache.HashTable(1024,false),
                CC.DEFAULT_LOCK_SCALE,
                0,
                false,
                false,
                null,
                false,
                false,
                false,
                null,
                null,
                0L,
                0L,
                false
                );
        e.init();
        return (E)e;
    }

    @Override
    boolean canRollback() {
        return false;
    }
}