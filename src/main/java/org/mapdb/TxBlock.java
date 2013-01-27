package org.mapdb;

/**
 * Wraps single transaction in a block
 */
public interface TxBlock {

    void tx(DB db) throws TxRollbackException;
}
