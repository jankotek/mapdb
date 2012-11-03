package org.mapdb;


public class StorageTransTest2 extends StorageDirectTest {
    


    protected StorageTrans openRecordManager() {
        return new StorageTrans(index);
    }



}
