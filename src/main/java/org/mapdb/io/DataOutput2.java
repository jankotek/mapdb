package org.mapdb.io;

import java.io.DataOutput;
import java.io.IOException;


public interface DataOutput2{

    /**
     * Give a hint to DataOutput about total size of serialized data.
     * This may prevent `byte[]` resize. And huge records might be placed into temporary file
     */
    void sizeHint(int size);

    /** write int in packed form */
    void writePackedInt(int value);

    /** write long in packed form */
    void writePackedLong(long value);

    /** write recid, recids have extra parity bit to detect data corruption */
    void writeRecid(long recid);

    /** write recid in packed form, recids have extra parity bit to detect data corruption */
    void writePackedRecid(long recid);

    /** copy content of this DataOutput2 into `ByteArray` */
    byte[] copyBytes();


    void writeBoolean(boolean v);

    void writeByte(int v);

    void writeShort(int v);

    void writeChar(int v);

    void writeInt(int v);

    void writeLong(long v);

    void writeFloat(float v);

    void writeDouble(double v);

    void writeBytes(String s);

    void writeChars(String s);

    void writeUTF(String s);

    @Deprecated         //TODO temp method for compatibility
    default void packInt(int i){
        writePackedInt(i);
    }

    void write(int bytee); //TODO remove this method? compatibility with java.io.DataOutput

    void write(byte[] buf);
    void write(byte b[], int off, int len);
}
