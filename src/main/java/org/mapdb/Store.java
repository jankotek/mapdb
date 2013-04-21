package org.mapdb;

import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * Low level record store.
 */
public interface Store extends Engine{



    long getMaxRecid();
    ByteBuffer getRaw(long recid);
    Iterator<Long> getFreeRecids();
    void updateRaw(long recid, ByteBuffer data);
}
