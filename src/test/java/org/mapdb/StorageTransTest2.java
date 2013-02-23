package org.mapdb;


public class StorageTransTest2 extends StorageDirectTest {



    @Override
	protected StorageWriteAhead openEngine() {
        return new StorageWriteAhead(fac);
    }



}
