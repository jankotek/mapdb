package org.mapdb.io;

import java.io.DataOutput;
import java.io.IOException;


public interface DataOutput2 extends DataOutput {

    /**
     * Give a hint to DataOutput about total size of serialized data.
     * This may prevent `byte[]` resize. And huge records might be placed into temporary file
     */
    void sizeHint(int size);

    /** write int in packed form */
    void writePackedInt(int value) throws IOException;

    /** write long in packed form */
    void writePackedLong(long value) throws IOException;

    /** write recid, recids have extra parity bit to detect data corruption */
    void writeRecid(long recid) throws IOException;

    /** write recid in packed form, recids have extra parity bit to detect data corruption */
    void writePackedRecid(long recid) throws IOException;

    /** copy content of this DataOutput2 into `ByteArray` */
    byte[] copyBytes() throws IOException;


    @Deprecated         //TODO temp method for compatibility
    default void packInt(int i) throws IOException {
        writePackedInt(i);
    }
}
