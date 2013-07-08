package org.mapdb;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Low level record store.
 */
public abstract class Store implements Engine{

    public abstract long getMaxRecid();
    public abstract ByteBuffer getRaw(long recid);
    public abstract Iterator<Long> getFreeRecids();
    public abstract void updateRaw(long recid, ByteBuffer data);

    /** returns maximal store size or `0` if there is no limit */
    public abstract long getSizeLimit();

    /** returns current size occupied by physical store (does not include index). It means file allocated by physical file */
    public abstract long getCurrSize();

    /** returns free size in  physical store (does not include index). */
    public abstract long getFreeSize();

    /** get some statistics about store. This may require traversing entire store, so it can take some time.*/
    public abstract String calculateStatistics();

    public void printStatistics(){
        System.out.println(calculateStatistics());
    }


}
