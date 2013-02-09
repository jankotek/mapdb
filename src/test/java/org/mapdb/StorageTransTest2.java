package org.mapdb;


public class StorageTransTest2 extends StorageDirectTest {



    @Override
	protected StorageJournaled openEngine() {
        return new StorageJournaled(fac);
    }



}
