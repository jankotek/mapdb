package org.mapdb;


import java.io.File;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreWALTest<E extends StoreWAL> extends StoreCachedTest<E>{

    @Override boolean canRollback(){return true;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        return (E) new StoreWAL(f.getPath());
    }

}
