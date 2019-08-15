package org.mapdb.io;

import java.io.IOException;

/** DataInput on top of {@code byte[]} */
public final class DataInput2ByteArray implements DataInput2 {
    
    protected final byte[] buf;
    protected int pos;

    public DataInput2ByteArray(byte[] b) {
        this(b, 0);
    }

    public DataInput2ByteArray(byte[] bb, int pos) {
        buf = bb;
        this.pos = pos;
    }

    public int getPos(){
        return pos;
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        System.arraycopy(buf, pos, b, off, len);
        pos += len;
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        pos += n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return buf[pos++] == 1;
    }

    @Override
    public byte readByte() throws IOException {
        return buf[pos++];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return buf[pos++] & 0xff;
    }

    @Override
    public short readShort() throws IOException {
        return (short)((buf[pos++] << 8) | (buf[pos++] & 0xff));
    }

    @Override
    public char readChar() throws IOException {
        return (char) (
                ((buf[pos++] & 0xff) << 8) |
                        (buf[pos++] & 0xff));
    }

    @Override
    public int readInt() throws IOException {
        int p = pos;
        final byte[] b = buf;
        final int ret =
                ((((int)b[p++]) << 24) |
                        (((int)b[p++] & 0xFF) << 16) |
                        (((int)b[p++] & 0xFF) <<  8) |
                        (((int)b[p++] & 0xFF)));
        pos = p;
        return ret;
    }

    @Override
    public long readLong() throws IOException {
        int p = pos;
        final byte[] b = buf;
        final long ret =
                ((((long)b[p++]) << 56) |
                        (((long)b[p++] & 0xFF) << 48) |
                        (((long)b[p++] & 0xFF) << 40) |
                        (((long)b[p++] & 0xFF) << 32) |
                        (((long)b[p++] & 0xFF) << 24) |
                        (((long)b[p++] & 0xFF) << 16) |
                        (((long)b[p++] & 0xFF) <<  8) |
                        (((long)b[p++] & 0xFF)));
        pos = p;
        return ret;
    }


    @Override
    public int available() {
        return buf.length-pos;
    }

    @Override
    public boolean availableMore() throws IOException {
        return available()>0;
    }

    @Override
    public int readPackedInt() throws IOException {
        return readInt();
    }

    @Override
    public long readPackedLong() throws IOException {
        return readLong();
    }


    @Override
    public void unpackLongSkip(int count) throws IOException {
        byte[] b = buf;
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

