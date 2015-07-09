package org.mapdb20;

import java.io.File;

import static org.junit.Assert.*;

public class StoreCacheHashTableTest<E extends StoreDirect> extends EngineTest<E>{

    File f = UtilsTest.tempDbFile();

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
                0,
                false,
                0,
                null
                );
        e.init();
        return (E)e;
    }

    @Override
    boolean canRollback() {
        return false;
    }
}