package net.kotek.jdbm;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Wraps ByteBuffer and provides DataInput
 */
public class DataInput2 implements DataInput {

    ByteBuffer buf;
    int pos;

    public DataInput2(final ByteBuffer buf, final int pos) {
        this.buf = buf;
        this.pos = pos;
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        //naive, but only thread safe way
        //TODO investigate
        for(int i=off;i<off+len;i++){
            b[i] = readByte();
        }
    }

    @Override
    public int skipBytes(final int n) throws IOException {
        pos +=n;
        return n;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return buf.get(pos++) ==1;
    }

    @Override
    public byte readByte() throws IOException {
        return buf.get(pos++);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return buf.get(pos++)& 0xff;
    }

    @Override
    public short readShort() throws IOException {
        final short ret = buf.getShort(pos);
        pos+=2;
        return ret;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return (((int) (buf.get(pos++) & 0xff) << 8) |
                ((int) (buf.get(pos++) & 0xff)));
    }

    @Override
    public char readChar() throws IOException {
        return (char) readInt();
    }

    @Override
    public int readInt() throws IOException {
        final int ret = buf.getInt(pos);
        pos+=4;
        return ret;
    }

    @Override
    public long readLong() throws IOException {
        final long ret = buf.getLong(pos);
        pos+=8;
        return ret;
    }

    @Override
    public float readFloat() throws IOException {
        final float ret = buf.getFloat(pos);
        pos+=4;
        return ret;
    }

    @Override
    public double readDouble() throws IOException {
        final double ret = buf.getDouble(pos);
        pos+=8;
        return ret;
    }

    @Override
    public String readLine() throws IOException {
        return readUTF();
    }

    @Override
    public String readUTF() throws IOException {
        return SerializerBase.deserializeString(this);
    }
}
