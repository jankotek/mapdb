package org.mapdb;

import java.io.*;

/**
 * Used for serialization
 */
public abstract class DataInput2 implements DataInput {


    /** DataInput on top of {@code byte[]} */
    public static final class ByteArray extends DataInput2 {
        protected final byte[] buf;
        public int pos;

        public ByteArray(byte[] b) {
            this(b, 0);
        }

        public ByteArray(byte[] bb, int pos) {
            //$DELAY$
            buf = bb;
            this.pos = pos;
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            System.arraycopy(buf, pos, b, off, len);
            //$DELAY$
            pos += len;
        }

        @Override
        public int skipBytes(final int n) throws IOException {
            pos += n;
            //$DELAY$
            return n;
        }

        @Override
        public boolean readBoolean() throws IOException {
            //$DELAY$
            return buf[pos++] == 1;
        }

        @Override
        public byte readByte() throws IOException {
            //$DELAY$
            return buf[pos++];
        }

        @Override
        public int readUnsignedByte() throws IOException {
            //$DELAY$
            return buf[pos++] & 0xff;
        }

        @Override
        public short readShort() throws IOException {
            //$DELAY$
            return (short)((buf[pos++] << 8) | (buf[pos++] & 0xff));
        }

        @Override
        public char readChar() throws IOException {
            //$DELAY$
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
        public java.nio.ByteBuffer internalByteBuffer() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public long unpackLong() throws IOException {
            byte[] b = buf;
            int p = pos;
            long ret = 0;
            byte v;
            do{
                //$DELAY$
                v = b[p++];
                ret = (ret<<7 ) | (v & 0x7F);
            }while((v&0x80)==0);
            pos = p;
            return ret;
        }

        @Override
        public void unpackLongSkip(int count) {
            byte[] b = buf;
            int pos2 = this.pos;
            while(count>0){
                count -= (b[pos2++]&0x80)>>7;
            }
            this.pos = pos2;
        }


        @Override
        public int unpackInt() throws IOException {
            byte[] b = buf;
            int p = pos;
            int ret = 0;
            byte v;
            do{
                //$DELAY$
                v = b[p++];
                ret = (ret<<7 ) | (v & 0x7F);
            }while((v&0x80)==0);
            pos = p;
            return ret;
        }

        @Override
        public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
            long[] ret = new long[size];
            int pos2 = pos;
            byte[] buf2 = buf;
            long prev =0;
            byte v;
            for(int i=0;i<size;i++){
                long r = 0;
                do {
                    //$DELAY$
                    v = buf2[pos2++];
                    r = (r << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                prev+=r;
                ret[i]=prev;
            }
            pos = pos2;
            return ret;
        }

        @Override
        public void unpackLongArray(long[] array, int start, int end) {
            int pos2 = pos;
            byte[] buf2 = buf;
            long ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2[pos2++];
                    ret = (ret << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                array[start]=ret;
            }
            pos = pos2;
        }

        @Override
        public void unpackIntArray(int[] array, int start, int end) {
            int pos2 = pos;
            byte[] buf2 = buf;
            int ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2[pos2++];
                    ret = (ret << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                array[start]=ret;
            }
            pos = pos2;
        }

    }


    /**
     * Wraps {@code DataInput} into {@code InputStream}
     */
    public static final class DataInputToStream extends InputStream {

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
            //$DELAY$
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

    /**
     * Wraps {@link java.nio.ByteBuffer} and provides {@link DataInput}
     *
     * @author Jan Kotek
     */
    public static final class ByteBuffer extends DataInput2 {

        public final java.nio.ByteBuffer buf;
        public int pos;

        public ByteBuffer(final java.nio.ByteBuffer buf, final int pos) {
            //$DELAY$
            this.buf = buf;
            this.pos = pos;
        }


        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            java.nio.ByteBuffer clone = buf.duplicate();
            clone.position(pos);
            //$DELAY$
            pos+=len;
            clone.get(b, off, len);
        }

        @Override
        public int skipBytes(final int n) throws IOException {
            pos +=n;
            //$DELAY$
            return n;
        }

        @Override
        public boolean readBoolean() throws IOException {
            //$DELAY$
            return buf.get(pos++) ==1;
        }

        @Override
        public byte readByte() throws IOException {
            //$DELAY$
            return buf.get(pos++);
        }

        @Override
        public int readUnsignedByte() throws IOException {
            //$DELAY$
            return buf.get(pos++)& 0xff;
        }

        @Override
        public short readShort() throws IOException {
            final short ret = buf.getShort(pos);
            //$DELAY$
            pos+=2;
            return ret;
        }
        @Override
        public char readChar() throws IOException {
            //$DELAY$
            return (char) (
                    ((buf.get(pos++) & 0xff) << 8) |
                            (buf.get(pos++) & 0xff));
        }

        @Override
        public int readInt() throws IOException {
            final int ret = buf.getInt(pos);
            //$DELAY$
            pos+=4;
            return ret;
        }

        @Override
        public long readLong() throws IOException {
            final long ret = buf.getLong(pos);
            //$DELAY$
            pos+=8;
            return ret;
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
            return null;
        }

        @Override
        public java.nio.ByteBuffer internalByteBuffer() {
            return buf;
        }

        @Override
        public void close() {
        }

        @Override
        public long unpackLong() throws IOException {
            long ret = 0;
            byte v;
            do{
                v = buf.get(pos++);
                ret = (ret<<7 ) | (v & 0x7F);
            }while((v&0x80)==0);

            return ret;
        }

        @Override
        public int unpackInt() throws IOException {
            int ret = 0;
            byte v;
            do{
                v = buf.get(pos++);
                ret = (ret<<7 ) | (v & 0x7F);
            }while((v&0x80)==0);

            return ret;
        }


        @Override
        public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
            long[] ret = new long[size];
            int pos2 = pos;
            java.nio.ByteBuffer buf2 = buf;
            long prev=0;
            byte v;
            for(int i=0;i<size;i++){
                long r = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    r = (r << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                prev+=r;
                ret[i]=prev;
            }
            pos = pos2;
            return ret;
        }

        @Override
        public void unpackLongArray(long[] array, int start, int end) {
            int pos2 = pos;
            java.nio.ByteBuffer buf2 = buf;
            long ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    ret = (ret << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                array[start] = ret;
            }
            pos = pos2;

        }

        @Override
        public void unpackLongSkip(int count) {
            java.nio.ByteBuffer buf2 = buf;
            int pos2 = pos;
            while(count>0){
                count -= (buf2.get(pos2++)&0x80)>>7;
            }
            pos = pos2;
        }


        @Override
        public void unpackIntArray(int[] array, int start, int end) {
            int pos2 = pos;
            java.nio.ByteBuffer buf2 = buf;
            int ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    ret = (ret << 7) | (v & 0x7F);
                }while((v&0x80)==0);
                array[start] = ret;
            }
            pos = pos2;
        }

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
        final int len = unpackInt();
        char[] b = new char[len];
        for (int i = 0; i < len; i++)
            //$DELAY$
            b[i] = (char) unpackInt();
        return new String(b);
    }

    @Override
    public int readUnsignedShort() throws IOException {
        //$DELAY$
        return readChar();
    }


    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }



    public abstract int getPos();
    public abstract void setPos(int pos);

    /** @return underlying {@code byte[]} or null if it does not exist*/
    public abstract byte[] internalByteArray();

    /** @return underlying {@code ByteBuffer} or null if it does not exist*/
    public abstract java.nio.ByteBuffer internalByteBuffer();


    public abstract void close();

    public abstract long unpackLong() throws IOException;

    public abstract int unpackInt() throws IOException;

    public abstract long[] unpackLongArrayDeltaCompression(int size) throws IOException;

    public abstract void unpackLongArray(long[] ret, int i, int len) throws IOException;
    public abstract void unpackIntArray(int[] ret, int i, int len) throws IOException;

    public abstract void unpackLongSkip(int count) throws IOException;


    public static final class Stream extends DataInput2 {

        private final InputStream ins;

        public Stream(InputStream ins) {
            this.ins = ins;
        }


        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            DBUtil.readFully(ins, b, off, len);
        }

        @Override
        public int skipBytes(final int n) throws IOException {
            return (int) ins.skip(n);
        }

        @Override
        public boolean readBoolean() throws IOException {
            //$DELAY$
            return readByte() == 1;
        }

        @Override
        public byte readByte() throws IOException {
            //$DELAY$
            int read = ins.read();
            if(read==-1)
                throw new EOFException();
            return (byte)(read&0xFF);
        }

        @Override
        public int readUnsignedByte() throws IOException {
            //$DELAY$
            int read = ins.read();
            if(read==-1)
                throw new EOFException();
            return read;
        }

        @Override
        public short readShort() throws IOException {
            //$DELAY$
            return (short)((readUnsignedByte() << 8) | (readUnsignedByte()));
        }

        @Override
        public char readChar() throws IOException {
            //$DELAY$
            return (char) (
                    (readUnsignedByte() << 8) |
                    (readUnsignedByte() & 0xff));
        }

        @Override
        public int readInt() throws IOException {
            final int ret =
                            ((readUnsignedByte() << 24) |
                            ((readUnsignedByte() & 0xFF) << 16) |
                            ((readUnsignedByte() & 0xFF) <<  8) |
                            ((readUnsignedByte() & 0xFF)));
            return ret;
        }

        @Override
        public long readLong() throws IOException {
            final long ret =
                            ((((long)readUnsignedByte()) << 56) |
                            (((long)readUnsignedByte() & 0xFF) << 48) |
                            (((long)readUnsignedByte() & 0xFF) << 40) |
                            (((long)readUnsignedByte() & 0xFF) << 32) |
                            (((long)readUnsignedByte() & 0xFF) << 24) |
                            (((long)readUnsignedByte() & 0xFF) << 16) |
                            (((long)readUnsignedByte() & 0xFF) <<  8) |
                            (((long)readUnsignedByte() & 0xFF)));
            return ret;
        }


        @Override
        public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
            long[] ret = new long[size];
            long prev =0;
            for(int i=0;i<size;i++){
                prev+=unpackLong();
                ret[i]=prev;
            }
            return ret;
        }

        @Override
        public void unpackLongArray(long[] array, int start, int end) throws IOException {
            for(;start<end;start++) {
                array[start]=unpackLong();
            }
        }

        @Override
        public void unpackIntArray(int[] array, int start, int end) throws IOException {
            for(;start<end;start++) {
                array[start]=unpackInt();
            }
        }

        @Override
        public void unpackLongSkip(int count) throws IOException {
            while(count-->0){
                unpackLong();
            }
        }




        @Override
        public int getPos() {
            throw new UnsupportedOperationException("InputStream does not support pos");
        }

        @Override
        public void setPos(int pos) {
            throw new UnsupportedOperationException("InputStream does not support pos");
        }

        @Override
        public byte[] internalByteArray() {
            return null;
        }

        @Override
        public java.nio.ByteBuffer internalByteBuffer() {
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public long unpackLong() throws IOException {
            return DBUtil.unpackLong(ins);
        }

        @Override
        public int unpackInt() throws IOException {
            return DBUtil.unpackInt(ins);
        }

    }
}
