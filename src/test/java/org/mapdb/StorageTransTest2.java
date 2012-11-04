package org.mapdb;


public class StorageTransTest2 extends StorageDirectTest {
    


    protected StorageTrans openEngine() {
        return new StorageTrans(index);
    }



}
