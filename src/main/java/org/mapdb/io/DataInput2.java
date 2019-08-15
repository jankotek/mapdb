package org.mapdb.io;

import java.io.DataInput;
import java.io.IOException;

public interface DataInput2 extends DataInput {

    /**
     * How many bytes are available for read (`total size - current position`).
     * Returns `Integer.MIN_VALUE` if this information is not available 
     */
    int available() throws IOException ;


    /** returns true if more bytes are available for read
     *
     * TODO throws exception if output has no EOF info?
     */
    boolean availableMore()throws IOException;


    int readPackedInt() throws IOException ;

    long readPackedLong() throws IOException ;

    default long readPackedRecid() throws IOException{
        //TODO bit parity for recids
        return readPackedLong();
    }

    default long readRecid() throws IOException{
        //TODO bit parity for recids
        return readLong();
    }

    @Override  default float readFloat() throws IOException {
        return java.lang.Float.intBitsToFloat(readInt());
    }

    @Override default double readDouble() throws IOException {
        return java.lang.Double.longBitsToDouble(readLong());
    }

    @Override default String readLine() throws IOException {
        return readUTF();
    }

    @Override default String readUTF() throws IOException {
        //TODO is it better then DataOutputStream.readUTF?
        int len = readPackedInt();
        char[] b = new char[len];
        for (int i=0;i<len;i++)
            b[i] = (char) readPackedInt();
        return new String(b);
    }

    @Override default int readUnsignedShort() throws IOException {
        return readChar();
    }


    @Override default void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }


    @Deprecated //TODO temp method for compatibility
    default int unpackInt() throws IOException {
        return readPackedInt();
    }


    void unpackLongSkip(int count) throws IOException ;

}
