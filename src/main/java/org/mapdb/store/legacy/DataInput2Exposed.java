package org.mapdb.store.legacy;

import org.mapdb.io.DataInput2;

import java.nio.ByteBuffer;

/** DataInput on top of {@code byte[]} */
public final class DataInput2Exposed implements DataInput2 {
    
    protected final ByteBuffer buf;
    protected int pos;

    public DataInput2Exposed(ByteBuffer b) {
        this(b, 0);
    }

    public DataInput2Exposed(ByteBuffer bb, int pos) {
        buf = bb;
        this.pos = pos;
    }

    public int getPos(){
        return pos;
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        ByteBuffer clone = buf.duplicate();
        clone.position(pos);
        pos+=len;
        clone.get(b,off,len);
    }

    @Override
    public int skipBytes(final int n) {
        pos += n;
        return n;
    }

    @Override
    public boolean readBoolean() {
        return buf.get(pos++) ==1;
    }

    @Override
    public byte readByte() {
        return buf.get(pos++);
    }

    @Override
    public int readUnsignedByte() {
        return buf.get(pos++)& 0xff;
    }

    @Override
    public short readShort() {
        final short ret = buf.getShort(pos);
        pos+=2;
        return ret;
    }

    @Override
    public char readChar() {
        final char ret = buf.getChar(pos);
        pos+=2;
        return ret;
    }

    @Override
    public int readInt() {
        final int ret = buf.getInt(pos);
        pos+=4;
        return ret;
    }

    @Override
    public long readLong() {
        final long ret = buf.getLong(pos);
        pos+=8;
        return ret;
    }


    @Override
    public int available() {
        return buf.limit()-pos;
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
        int pos2 = this.pos;
        while(count>0){
//            count -= (b[pos2++]&0x80)>>7;
            //TODO go back to packed longs, remove code bellow
            readLong();
            count--;
            pos2+=8;
        }
        this.pos = pos2;
    }

}

