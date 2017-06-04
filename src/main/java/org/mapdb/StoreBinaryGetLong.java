package org.mapdb;

import org.mapdb.DataInput2;
import org.mapdb.StoreBinary;

import java.io.IOException;

/**
 * Binary operations performed on {@link StoreBinary} which retuns long
 */
public interface StoreBinaryGetLong {

    long get(DataInput2 input, int size) throws IOException;

}
