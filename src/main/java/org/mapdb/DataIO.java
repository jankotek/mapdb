package org.mapdb;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Various IO classes and utilities..
 */
public final class DataIO {

    private DataIO(){}


    /**
     * Give state to internal byte[] or ByteBuffer in DataInput2..
     * Should not be used unless you are writing MapDB extension and needs some performance bonus
     */
    interface DataInputInternal extends DataInput,Closeable {

        int getPos();
        void setPos(int pos);

        /** return underlying `byte[]` or null if it does not exist*/
        byte[] internalByteArray();

        /** return underlying `ByteBuffer` or null if it does not exist*/
        ByteBuffer internalByteBuffer();


        void close();
    }

    /** DataInput on top of `byte[]` */
    static public final class DataInputByteArray implements DataInput, DataInputInternal {
        protected final byte[] buf;
        protected int pos;


        public DataInputByteArray(byte[] b) {
            this(b, 0);
        }

        public DataInputByteArray(byte[] bb, int pos) {
            buf = bb;
            this.pos = pos;
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
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
        public int readUnsignedShort() throws IOException {
            return (((buf[pos++] & 0xff) << 8) |
                    ((buf[pos++] & 0xff)));
        }

        @Override
        public char readChar() throws IOException {
            // I know: 4 bytes, but char only consumes 2,
            // has to stay here for backward compatibility
            return (char) readInt();
        }

        @Override
        public int readInt() throws IOException {
            final int end = pos + 4;
            int ret = 0;
            for (; pos < end; pos++) {
                ret = (ret << 8) | (buf[pos] & 0xFF);
            }
            return ret;
        }

        @Override
        public long readLong() throws IOException {
            final int end = pos + 8;
            long ret = 0;
            for (; pos < end; pos++) {
                ret = (ret << 8) | (buf[pos] & 0xFF);
            }
            return ret;
        }

        @Override
        public float readFloat() throws IOException {
            return Float.intBitsToFloat(readInt());
        }

        @Override
        public double readDouble() throws IOException {
            return Double.longBitsToDouble(readLong());
        }

        @Override
        public String readLine() throws IOException {
            return readUTF();
        }

        @Override
        public String readUTF() throws IOException {
            final int size = DataInput2.unpackInt(this);
            return SerializerBase.deserializeString(this, size);
        }

        @Override
        public int getPos() {
            return pos;
        }

        @Override
        public void setPos(int pos) {
            this.pos = pos;
        }

        @Override
        public byte[] internalByteArray() {
            return buf;
        }

        @Override
        public ByteBuffer internalByteBuffer() {
            return null;
        }

        @Override
        public void close() {
        }

    }


    /**
     * Wraps `DataInput` into `InputStream`
     */
    public static class DataInputToStream extends InputStream {

        protected final DataInput in;

        public DataInputToStream(DataInput in) {
            this.in = in;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            in.readFully(b,off,len);
            return len;
        }

        @Override
        public long skip(long n) throws IOException {
            n = Math.min(n, Integer.MAX_VALUE);
            return in.skipBytes((int) n);
        }

        @Override
        public void close() throws IOException {
            if(in instanceof Closeable)
                ((Closeable) in).close();
        }

        @Override
        public int read() throws IOException {
            return in.readUnsignedByte();
        }
    }

}
