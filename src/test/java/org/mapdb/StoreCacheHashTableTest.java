package org.mapdb;

import java.io.File;

import static org.junit.Assert.*;

public class StoreCacheHashTableTest<E extends StoreDirect> extends EngineTest<E>{

    File f = UtilsTest.tempDbFile();

    @Override protected E openEngine() {
        StoreDirect e =new StoreDirect(
                f.getPath(),
                Volume.fileFactory(),
                new Store.Cache.HashTable(1024,false),
                0,
                false,
                false,
                null,
                false,
                0,
                false,
                0
                );
        e.init();
        return (E)e;
    }

    @Override
    boolean canRollback() {
        return false;
    }
}