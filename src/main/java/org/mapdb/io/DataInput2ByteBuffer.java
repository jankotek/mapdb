package org.mapdb.io;

import java.nio.ByteBuffer;

/** DataInput on top of {@link ByteBuffer} */
public final class DataInput2ByteBuffer implements DataInput2 {

    protected final ByteBuffer buf;

    public DataInput2ByteBuffer(ByteBuffer buf) {
        this.buf = buf;
    }

    public void readFully(byte[] b, int off, int len) {
        buf.get(b, off,len);
    }

    @Override
    public int skipBytes(final int n) {
        buf.position(buf.position()+n);
        return n;
    }

    @Override
    public boolean readBoolean() {
        return buf.get() == 1;
    }

    @Override
    public byte readByte() {
        return buf.get();
    }

    @Override
    public int readUnsignedByte() {
        return buf.get() & 0xff;
    }

    @Override
    public short readShort() {
        return buf.getShort();
    }

    @Override
    public char readChar() {
        return buf.getChar();
    }

    @Override
    public int readInt() {
        return buf.getInt();
    }

    @Override
    public long readLong() {
        return buf.getLong();
    }


    @Override
    public int available() {
        return buf.remaining();
    }

    @Override
    public boolean availableMore() {
        return available()>0;
    }

    @Override
    public int readPackedInt() {
        return readInt();
    }

    @Override
    public long readPackedLong() {
        return readLong();
    }


    @Override
    public void unpackLongSkip(int count) {
        while(count>0){
            //TODO go back to packed longs, remove code bellow
            readLong();
            count--;
        }
    }

}

