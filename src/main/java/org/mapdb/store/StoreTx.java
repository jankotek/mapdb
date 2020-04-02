package org.mapdb.store;

public interface StoreTx extends Store {
    void commit();
    void rollback();

}
