package org.mapdb;


public class StorageAppendTest extends StorageTestCase {

    StorageAppend engine = (StorageAppend) super.engine;

    @Override
    protected Engine openEngine() {
        return new StorageAppend(index, false, false, false);
    }
}
