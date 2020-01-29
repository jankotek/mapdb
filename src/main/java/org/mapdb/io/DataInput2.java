package org.mapdb.io;

import java.io.DataInput;
import java.io.IOException;

public interface DataInput2{

    /**
     * How many bytes are available for read (`total size - current position`).
     * Returns `Integer.MIN_VALUE` if this information is not available 
     */
    int available() ;


    /** returns true if more bytes are available for read
     *
     * TODO throws exception if output has no EOF info?
     */
    boolean availableMore()throws IOException;


    int readPackedInt() ;

    long readPackedLong() ;

    default long readPackedRecid(){
        //TODO bit parity for recids
        return readPackedLong();
    }

    default long readRecid(){
        //TODO bit parity for recids
        return readLong();
    }

    long readLong();

    default float readFloat() {
        return java.lang.Float.intBitsToFloat(readInt());
    }

    int readInt();

    default double readDouble() {
        return java.lang.Double.longBitsToDouble(readLong());
    }

    default String readLine() {
        return readUTF();
    }

    default String readUTF() {
        //TODO is it better then DataOutputStream.readUTF?
        int len = readPackedInt();
        char[] b = new char[len];
        for (int i=0;i<len;i++)
            b[i] = (char) readPackedInt();
        return new String(b);
    }

    default int readUnsignedShort() {
        return readChar();
    }

    int skipBytes(int n);

    boolean readBoolean();

    byte readByte();

    int readUnsignedByte();

    short readShort();

    char readChar();


    default void readFully(byte[] b) {
        readFully(b, 0, b.length);
    }

    void readFully(byte[] b, int offset, int length);


    @Deprecated //TODO temp method for compatibility
    default int unpackInt() {
        return readPackedInt();
    }


    void unpackLongSkip(int count) ;

}
