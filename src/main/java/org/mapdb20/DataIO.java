package org.mapdb20;

import java.io.*;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.lang.Long.rotateLeft;

/**
 * Various IO classes and utilities..
 */
public final class DataIO {

    private DataIO(){}

    /**
     * Unpack int value from the input stream.
     *
     * @param is The input stream.
     * @return The long value.
     *
     * @throws java.io.IOException in case of IO error
     */
    static public int unpackInt(DataInput is) throws IOException {
        int ret = 0;
        byte v;
        do{
            v = is.readByte();
            ret = (ret<<7 ) | (v & 0x7F);
        }while(v<0);

        return ret;
    }

    /**
     * Unpack long value from the input stream.
     *
     * @param in The input stream.
     * @return The long value.
     *
     * @throws java.io.IOException in case of IO error
     */
    static public long unpackLong(DataInput in) throws IOException {
        long ret = 0;
        byte v;
        do{
            v = in.readByte();
            ret = (ret<<7 ) | (v & 0x7F);
        }while(v<0);

        return ret;
    }


    /**
     * Unpack long value from the input stream.
     *
     * @param in The input stream.
     * @return The long value.
     *
     * @throws java.io.IOException in case of IO error
     */
    static public long unpackLong(InputStream in) throws IOException {
        long ret = 0;
        int v;
        do{
            v = in.read();
            if(v==-1)
                throw new EOFException();
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)!=0);

        return ret;
    }

    /**
     * Pack long into output.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     *
     * @throws java.io.IOException in case of IO error
     */
    static public void packLong(DataOutput out, long value) throws IOException {
        //$DELAY$
        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            out.writeByte((byte) (((value>>>shift) & 0x7F) | 0x80));
            //$DELAY$
            shift-=7;
        }
        out.writeByte((byte) (value & 0x7F));
    }


    /**
     * Pack long into output.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out OutputStream to put value into
     * @param value to be serialized, must be non-negative
     *
     * @throws java.io.IOException in case of IO error
     */
    static public void packLong(OutputStream out, long value) throws IOException {
        //$DELAY$
        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            out.write((int) (((value>>>shift) & 0x7F) | 0x80));
            //$DELAY$
            shift-=7;
        }
        out.write((int) (value & 0x7F));
    }

    /**
     * Calculate how much bytes packed long consumes.
     *
     * @param value to calculate
     * @return number of bytes used in packed form
     */
    public static int packLongSize(long value) {
        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        int ret = 1;
        while(shift!=0){
            //TODO remove cycle, just count zeroes
            shift-=7;
            ret++;
        }
        return ret;
    }


    /**
     * Unpack RECID value from the input stream with 3 bit checksum.
     *
     * @param in The input stream.
     * @return The long value.
     * @throws java.io.IOException in case of IO error
     */
    static public long unpackRecid(DataInput in) throws IOException {
        long val = unpackLong(in);
        val = DataIO.parity3Get(val);
        return val >>> 3;
    }


    /**
     * Pack RECID into output stream with 3 bit checksum.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException in case of IO error
     */
    static public void packRecid(DataOutput out, long value) throws IOException {
        value = DataIO.parity3Set(value<<3);
        packLong(out,value);
    }


    /**
     * Pack int into an output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException in case of IO error
     */

    static public void packInt(DataOutput out, int value) throws IOException {
       // Optimize for the common case where value is small. This is particular important where our caller
       // is SerializerBase.SER_STRING.serialize because most chars will be ASCII characters and hence in this range.
       // credit Max Bolingbroke https://github.com/jankotek/MapDB/pull/489

        int shift = (value & ~0x7F); //reuse variable
        if (shift != 0) {
            //$DELAY$
            shift = 31-Integer.numberOfLeadingZeros(value);
            shift -= shift%7; // round down to nearest multiple of 7
            while(shift!=0){
                out.writeByte((byte) (((value>>>shift) & 0x7F) | 0x80));
                //$DELAY$
                shift-=7;
            }
        }
        //$DELAY$
        out.writeByte((byte) (value & 0x7F));
    }

    /**
     * Pack int into an output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * This method is same as {@link #packInt(DataOutput, int)},
     * but is optimized for values larger than 127. Usually it is recids.
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException in case of IO error
     */

    static public void packIntBigger(DataOutput out, int value) throws IOException {
        //$DELAY$
        int shift = 31-Integer.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            out.writeByte((byte) (((value>>>shift) & 0x7F) | 0x80));
            //$DELAY$
            shift-=7;
        }
        //$DELAY$
        out.writeByte((byte) (value & 0x7F));
    }

    public static int longHash(long h) {
        //$DELAY$
        h = h * -7046029254386353131L;
        h ^= h >> 32;
        return (int)(h ^ h >> 16);
    }

    public static int intHash(int h) {
        //$DELAY$
        h = h * -1640531527;
        return h ^ h >> 16;
    }

    public static final long PACK_LONG_RESULT_MASK = 0xFFFFFFFFFFFFFFFL;


    public static int packLongBidi(DataOutput out, long value) throws IOException {
        out.write((((int) value & 0x7F)) | 0x80);
        value >>>= 7;
        int counter = 2;

        //$DELAY$
        while ((value & ~0x7FL) != 0) {
            out.write((((int) value & 0x7F)));
            value >>>= 7;
            //$DELAY$
            counter++;
        }
        //$DELAY$
        out.write((byte) value| 0x80);
        return counter;
    }

    public static int packLongBidi(byte[] buf, int pos, long value) {
        buf[pos++] = (byte) ((((int) value & 0x7F))| 0x80);
        value >>>= 7;
        int counter = 2;

        //$DELAY$
        while ((value & ~0x7FL) != 0) {
            buf[pos++] = (byte) (((int) value & 0x7F));
            value >>>= 7;
            //$DELAY$
            counter++;
        }
        //$DELAY$
        buf[pos++] = (byte) ((byte) value| 0x80);
        return counter;
    }


    public static long unpackLongBidi(byte[] bb, int pos){
        //$DELAY$
        long b = bb[pos++];
        if(CC.ASSERT && (b&0x80)==0)
            throw new DBException.DataCorruption("long pack bidi wrong header");
        long result = (b & 0x7F) ;
        int offset = 7;
        do {
            //$DELAY$
            b = bb[pos++];
            result |= (b & 0x7F) << offset;
            if(CC.ASSERT && offset>64)
                throw new DBException.DataCorruption("long pack bidi too long");
            offset += 7;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)(offset/7))<<60) | result;
    }


    public static long unpackLongBidiReverse(byte[] bb, int pos){
        //$DELAY$
        long b = bb[--pos];
        if(CC.ASSERT && (b&0x80)==0)
            throw new DBException.DataCorruption("long pack bidi wrong header");
        long result = (b & 0x7F) ;
        int counter = 1;
        do {
            //$DELAY$
            b = bb[--pos];
            result = (b & 0x7F) | (result<<7);
            if(CC.ASSERT && counter>8)
                throw new DBException.DataCorruption("long pack bidi too long");
            counter++;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)counter)<<60) | result;
    }

    public static long getLong(byte[] buf, int pos) {
       return
               ((((long)buf[pos++]) << 56) |
                (((long)buf[pos++] & 0xFF) << 48) |
                (((long)buf[pos++] & 0xFF) << 40) |
                (((long)buf[pos++] & 0xFF) << 32) |
                (((long)buf[pos++] & 0xFF) << 24) |
                (((long)buf[pos++] & 0xFF) << 16) |
                (((long)buf[pos++] & 0xFF) <<  8) |
                (((long)buf[pos] & 0xFF)));

    }

    public static void putLong(byte[] buf, int pos,long v) {
        buf[pos++] = (byte) (0xff & (v >> 56));
        buf[pos++] = (byte) (0xff & (v >> 48));
        buf[pos++] = (byte) (0xff & (v >> 40));
        buf[pos++] = (byte) (0xff & (v >> 32));
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos] = (byte) (0xff & (v));
    }


    public static long getSixLong(byte[] buf, int pos) {
        return
                        ((long) (buf[pos++] & 0xff) << 40) |
                        ((long) (buf[pos++] & 0xff) << 32) |
                        ((long) (buf[pos++] & 0xff) << 24) |
                        ((long) (buf[pos++] & 0xff) << 16) |
                        ((long) (buf[pos++] & 0xff) << 8) |
                        ((long) (buf[pos] & 0xff));
    }

    public static void putSixLong(byte[] buf, int pos, long value) {
        if(CC.ASSERT && (value>>>48!=0))
            throw new AssertionError();

        buf[pos++] = (byte) (0xff & (value >> 40));
        buf[pos++] = (byte) (0xff & (value >> 32));
        buf[pos++] = (byte) (0xff & (value >> 24));
        buf[pos++] = (byte) (0xff & (value >> 16));
        buf[pos++] = (byte) (0xff & (value >> 8));
        buf[pos] = (byte) (0xff & (value));
    }



    public static long nextPowTwo(final long a)
    {
        return 1L << (64 - Long.numberOfLeadingZeros(a - 1L));
    }

    public static int nextPowTwo(final int a)
    {
        return 1 << (32 - Integer.numberOfLeadingZeros(a - 1));
    }

    public static void readFully(InputStream in, byte[] data) throws IOException {
        int len = data.length;
        for(int read=0; read<len;){
            int c = in.read(data, read, len - read);
            if(c<0)
                throw new EOFException();
            read+=c;
        }
    }
    
    public static void skipFully(InputStream in, long length) throws IOException {
        while ((length -= in.skip(length)) > 0);
    }


    /**
     * Give access to internal byte[] or ByteBuffer in DataInput2..
     * Should not be used unless you are writing MapDB extension and needs some performance bonus
     */
    public interface DataInputInternal extends DataInput,Closeable {

        int getPos();
        void setPos(int pos);

        /** @return underlying {@code byte[]} or null if it does not exist*/
        byte[] internalByteArray();

        /** @return underlying {@code ByteBuffer} or null if it does not exist*/
        ByteBuffer internalByteBuffer();


        void close();

        long unpackLong() throws IOException;

        int unpackInt() throws IOException;

        long[] unpackLongArrayDeltaCompression(int size) throws IOException;

        void unpackLongArray(long[] ret, int i, int len);
        void unpackIntArray(int[] ret, int i, int len);
    }

    /** DataInput on top of {@code byte[]} */
    static public final class DataInputByteArray implements DataInput, DataInputInternal {
        protected final byte[] buf;
        protected int pos;


        public DataInputByteArray(byte[] b) {
            this(b, 0);
        }

        public DataInputByteArray(byte[] bb, int pos) {
            //$DELAY$
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
        public int readUnsignedShort() throws IOException {
            //$DELAY$
            return readChar();
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
            }while(v<0);
            pos = p;
            return ret;
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
            }while(v<0);
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
                } while (v < 0);
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
                } while (v < 0);
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
                } while (v < 0);
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
     * Wraps {@link java.nio.ByteBuffer} and provides {@link java.io.DataInput}
     *
     * @author Jan Kotek
     */
    public static final class DataInputByteBuffer implements DataInput, DataInputInternal {

        public final ByteBuffer buf;
        public int pos;

        public DataInputByteBuffer(final ByteBuffer buf, final int pos) {
            //$DELAY$
            this.buf = buf;
            this.pos = pos;
        }

        /**
         * @param b byte buffer
         * @deprecated  use {@link org.mapdb20.DataIO.DataInputByteArray}
         */
        public DataInputByteBuffer(byte[] b) {
            this(ByteBuffer.wrap(b),0);
        }

        @Override
        public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) throws IOException {
            ByteBuffer clone = buf.duplicate();
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
        public int readUnsignedShort() throws IOException {
            return readChar();
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
        public float readFloat() throws IOException {
            final float ret = buf.getFloat(pos);
            //$DELAY$
            pos+=4;
            return ret;
        }

        @Override
        public double readDouble() throws IOException {
            final double ret = buf.getDouble(pos);
            //$DELAY$
            pos+=8;
            return ret;
        }

        @Override
        public String readLine() throws IOException {
            return readUTF();
        }

        @Override
        public String readUTF() throws IOException {
            //TODO verify this method accross multiple serializers
            final int size = unpackInt();
            //$DELAY$
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
            return null;
        }

        @Override
        public ByteBuffer internalByteBuffer() {
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
            }while(v<0);

            return ret;
        }

        @Override
        public int unpackInt() throws IOException {
            int ret = 0;
            byte v;
            do{
                v = buf.get(pos++);
                ret = (ret<<7 ) | (v & 0x7F);
            }while(v<0);

            return ret;
        }


        @Override
        public long[] unpackLongArrayDeltaCompression(final int size) throws IOException {
            long[] ret = new long[size];
            int pos2 = pos;
            ByteBuffer buf2 = buf;
            long prev=0;
            byte v;
            for(int i=0;i<size;i++){
                long r = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    r = (r << 7) | (v & 0x7F);
                } while (v < 0);
                prev+=r;
                ret[i]=prev;
            }
            pos = pos2;
            return ret;
        }

        @Override
        public void unpackLongArray(long[] array, int start, int end) {
            int pos2 = pos;
            ByteBuffer buf2 = buf;
            long ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    ret = (ret << 7) | (v & 0x7F);
                } while (v < 0);
                array[start] = ret;
            }
            pos = pos2;

        }

        @Override
        public void unpackIntArray(int[] array, int start, int end) {
            int pos2 = pos;
            ByteBuffer buf2 = buf;
            int ret;
            byte v;
            for(;start<end;start++) {
                ret = 0;
                do {
                    //$DELAY$
                    v = buf2.get(pos2++);
                    ret = (ret << 7) | (v & 0x7F);
                } while (v < 0);
                array[start] = ret;
            }
            pos = pos2;
        }

    }

    /**
     * Provides {@link java.io.DataOutput} implementation on top of growable {@code byte[]}
     *  {@link java.io.ByteArrayOutputStream} is not used as it requires {@code byte[]} copying
     *
     * @author Jan Kotek
     */
    public static final class DataOutputByteArray extends OutputStream implements DataOutput {

        public byte[] buf;
        public int pos;
        public int sizeMask;


        public DataOutputByteArray(){
            pos = 0;
            buf = new byte[128]; //TODO take hint from serializer for initial size
            sizeMask = 0xFFFFFFFF-(buf.length-1);
        }


        public byte[] copyBytes(){
            return Arrays.copyOf(buf, pos);
        }

        /**
         * make sure there will be enough space in buffer to write N bytes
         * @param n number of bytes which can be safely written after this method returns
         */
        public void ensureAvail(int n) {
            //$DELAY$
            n+=pos;
            if ((n&sizeMask)!=0) {
                grow(n);
            }
        }

        private void grow(int n) {
            //$DELAY$
            int newSize = Math.max(nextPowTwo(n),buf.length);
            sizeMask = 0xFFFFFFFF-(newSize-1);
            buf = Arrays.copyOf(buf, newSize);
        }


        @Override
        public void write(final int b) throws IOException {
            ensureAvail(1);
            //$DELAY$
            buf[pos++] = (byte) b;
        }

        @Override
        public void write(final byte[] b, final int off, final int len) throws IOException {
            ensureAvail(len);
            //$DELAY$
            System.arraycopy(b, off, buf, pos, len);
            pos += len;
        }

        @Override
        public void writeBoolean(final boolean v) throws IOException {
            ensureAvail(1);
            //$DELAY$
            buf[pos++] = (byte) (v ? 1 : 0);
        }

        @Override
        public void writeByte(final int v) throws IOException {
            ensureAvail(1);
            //$DELAY$
            buf[pos++] = (byte) (v);
        }

        @Override
        public void writeShort(final int v) throws IOException {
            ensureAvail(2);
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v >> 8));
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v));
        }

        @Override
        public void writeChar(final int v) throws IOException {
            ensureAvail(2);
            buf[pos++] = (byte) (v>>>8);
            buf[pos++] = (byte) (v);
        }

        @Override
        public void writeInt(final int v) throws IOException {
            ensureAvail(4);
            buf[pos++] = (byte) (0xff & (v >> 24));
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v >> 16));
            buf[pos++] = (byte) (0xff & (v >> 8));
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v));
        }

        @Override
        public void writeLong(final long v) throws IOException {
            ensureAvail(8);
            buf[pos++] = (byte) (0xff & (v >> 56));
            buf[pos++] = (byte) (0xff & (v >> 48));
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v >> 40));
            buf[pos++] = (byte) (0xff & (v >> 32));
            buf[pos++] = (byte) (0xff & (v >> 24));
            //$DELAY$
            buf[pos++] = (byte) (0xff & (v >> 16));
            buf[pos++] = (byte) (0xff & (v >> 8));
            buf[pos++] = (byte) (0xff & (v));
            //$DELAY$
        }

        @Override
        public void writeFloat(final float v) throws IOException {
            writeInt(Float.floatToIntBits(v));
        }

        @Override
        public void writeDouble(final double v) throws IOException {
            writeLong(Double.doubleToLongBits(v));
        }

        @Override
        public void writeBytes(final String s) throws IOException {
            writeUTF(s);
        }

        @Override
        public void writeChars(final String s) throws IOException {
            writeUTF(s);
        }

        @Override
        public void writeUTF(final String s) throws IOException {
            final int len = s.length();
            packInt(len);
            for (int i = 0; i < len; i++) {
                //$DELAY$
                int c = (int) s.charAt(i);
                packInt(c);
            }
        }

        public void packInt(int value) throws IOException {
            ensureAvail(5); //ensure worst case bytes

            // Optimize for the common case where value is small. This is particular important where our caller
            // is SerializerBase.SER_STRING.serialize because most chars will be ASCII characters and hence in this range.
            // credit Max Bolingbroke https://github.com/jankotek/MapDB/pull/489
            int shift = (value & ~0x7F); //reuse variable
            if (shift != 0) {
                shift = 31 - Integer.numberOfLeadingZeros(value);
                shift -= shift % 7; // round down to nearest multiple of 7
                while (shift != 0) {
                    buf[pos++] = (byte) (((value >>> shift) & 0x7F) | 0x80);
                    shift -= 7;
                }
            }
            buf[pos++] = (byte) (value & 0x7F);
        }

        public void packIntBigger(int value) throws IOException {
            ensureAvail(5); //ensure worst case bytes
            int shift = 31-Integer.numberOfLeadingZeros(value);
            shift -= shift%7; // round down to nearest multiple of 7
            while(shift!=0){
                buf[pos++] = (byte) (((value>>>shift) & 0x7F) | 0x80);
                shift-=7;
            }
            buf[pos++] = (byte) (value & 0x7F);
        }

        public void packLong(long value) {
            ensureAvail(10); //ensure worst case bytes
            int shift = 63-Long.numberOfLeadingZeros(value);
            shift -= shift%7; // round down to nearest multiple of 7
            while(shift!=0){
                buf[pos++] = (byte) (((value>>>shift) & 0x7F) | 0x80);
                shift-=7;
            }
            buf[pos++] = (byte) (value & 0x7F);
        }
    }


    public static long parity1Set(long i) {
        if(CC.ASSERT && (i&1)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Long.bitCount(i)+1)%2);
    }

    public static long parity1Get(long i) {
        if(Long.bitCount(i)%2!=1){
            throw new DBException.PointerChecksumBroken();
        }
        return i&0xFFFFFFFFFFFFFFFEL;
    }

    public static long parity3Set(long i) {
        if(CC.ASSERT && (i&0x7)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Long.bitCount(i)+1)%8);
    }

    public static long parity3Get(long i) {
        long ret = i&0xFFFFFFFFFFFFFFF8L;
        if((Long.bitCount(ret)+1)%8!=(i&0x7)){
            throw new DBException.PointerChecksumBroken();
        }
        return ret;
    }

    public static long parity4Set(long i) {
        if(CC.ASSERT && (i&0xF)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Long.bitCount(i)+1)%16);
    }

    public static long parity4Get(long i) {
        long ret = i&0xFFFFFFFFFFFFFFF0L;
        if((Long.bitCount(ret)+1)%16!=(i&0xF)){
            throw new DBException.PointerChecksumBroken();
        }
        return ret;
    }


    public static long parity16Set(long i) {
        if(CC.ASSERT && (i&0xFFFF)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | (DataIO.longHash(i)&0xFFFFL);
    }

    public static long parity16Get(long i) {
        long ret = i&0xFFFFFFFFFFFF0000L;
        if((DataIO.longHash(ret)&0xFFFFL) != (i&0xFFFFL)){
            throw new DBException.PointerChecksumBroken();
        }
        return ret;
    }


    /**
     * Converts binary array into its hexadecimal representation.
     *
     * @param bb binary data
     * @return hexadecimal string
     */
    public static String toHexa( byte [] bb ) {
        char[] HEXA_CHARS = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] ret = new char[bb.length*2];
        for(int i=0;i<bb.length;i++){
            ret[i*2] =HEXA_CHARS[((bb[i]& 0xF0) >> 4)];
            ret[i*2+1] = HEXA_CHARS[((bb[i] & 0x0F))];
        }
        return new String(ret);
    }

    /**
     * Converts hexadecimal string into binary data
     * @param s hexadecimal string
     * @return binary data
     * @throws NumberFormatException in case of string format error
     */
    public static byte[] fromHexa(String s ) {
        byte[] ret = new byte[s.length()/2];
        for(int i=0;i<ret.length;i++){
            ret[i] = (byte) Integer.parseInt(s.substring(i*2,i*2+2),16);
        }
        return ret;
    }

    /**
     * File locking mechanism. Creates '*.lock' file and starts background thread to periodically modify it.
     * If JVM dies, file gets old and expires.
     *
     * @see DBMaker.Maker#fileLockHeartbeatEnable()
     */
    public static final class HeartbeatFileLock{

        /*
         * This class originally comes from H2 Database and was relicensed
         * under Apache 2.0 license with Thomas Mueller permission.
         *
         * Original copyright notice:
         *
         * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
         * Version 1.0, and under the Eclipse Public License, Version 1.0
         * (http://h2database.com/html/license.html).
         * Initial Developer: H2 Group
         */


        private static final Logger LOG = Logger.getLogger(HeartbeatFileLock.class.getName());

        private static final int SLEEP_GAP = 25;
        private static final int TIME_GRANULARITY = 2000;

        /** quit hearbeat after this was GCed */
        private  WeakReference quitAfterGCed = null;

        final private long id = new SecureRandom().nextLong();
        volatile private File file;

        /**
         * Whether the file is locked.
         */
        private volatile boolean locked;

        /**
         * The number of milliseconds to sleep after checking a file.
         */
        private final int sleep;


        /**
         * The last time the lock file was written.
         */
        private long lastWrite;


        private Thread watchdog;

        private final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                HeartbeatFileLock.this.run();
            }
        };

        public HeartbeatFileLock(File file, int sleep) {
            this.file = file;
            this.sleep = sleep;
        }

        public void setQuitAfterGCed(Object object){
            this.quitAfterGCed = object==null?
                    null:
                    new WeakReference(object);
        }
        private void run(){
            LOG.fine("Lock Watchdog start");
            try {
                while (locked && file != null) {
                    if( LOG.isLoggable(Level.FINE))
                        LOG.fine("watchdog check");
                    try {
                        WeakReference quitAfterGCed = this.quitAfterGCed;
                        if(quitAfterGCed!=null && quitAfterGCed.get()==null)
                            return;
                        if (!file.exists() ||
                                file.lastModified() != lastWrite) {
                            save();
                        }
                        Thread.sleep(sleep);
                    } catch (OutOfMemoryError e) {
                        // ignore
                    } catch (InterruptedException e) {
                        // ignore
                    } catch (NullPointerException e) {
                        // ignore
                    } catch (Exception e) {
                        LOG.log(Level.FINE,"MapDB Lock Watchdog", e);
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.WARNING, "MapDB Lock Watchdog failed", e);
            }finally {
                LOG.fine("Lock Watcher end");
            }
        }

        private void waitUntilOld() {
            for (int i = 0; i < 2 * TIME_GRANULARITY / SLEEP_GAP; i++) {
                long last = file.lastModified();
                long dist = System.currentTimeMillis() - last;
                if (dist < -TIME_GRANULARITY) {
                    // lock file modified in the future -
                    // wait for a bit longer than usual
                    sleep(10 * sleep);

                    return;
                } else if (dist > TIME_GRANULARITY) {
                    return;
                }

                sleep(SLEEP_GAP);
            }
            throw new DBException.FileLocked("Lock file recently modified");
        }

        public synchronized void lock(){
            if (locked) {
                throw new DBException.FileLocked("Already locked, cannot call lock() twice");
            }

            try {
                // TODO is this needed?: FileUtils.createDirectories(FileUtils.getParent(fileName));
                if (!file.createNewFile()) {

                    waitUntilOld();
                    save();

                    sleep(10 * sleep);

                    if (load() != id) {
                        throw new DBException.FileLocked("Locked by another process");
                    }
                    delete();
                    if (!file.createNewFile()) {
                        throw new DBException.FileLocked("Another process was faster");
                    }
                }
                save();
                sleep(SLEEP_GAP);
                if (load() != id) {
                    file = null;
                    throw new DBException.FileLocked("Concurrent update");
                }

                //TODO use MapDB Executor Service if available
                watchdog = new Thread(runnable,
                        "MapDB File Lock Watchdog " + file.getAbsolutePath());

                watchdog.setDaemon(true);
                try {
                    watchdog.setPriority(Thread.MAX_PRIORITY - 1);
                }catch(Exception e){
                    LOG.log(Level.FINE,"Could not set thread priority",e);
                }
                watchdog.start();

            }catch(IOException e){
                throw new DBException.FileLocked("Could not lock file: " + file, e);
            }
            locked = true;
        }

        /**
         * Unlock the file. The watchdog thread is stopped. This method does nothing
         * if the file is already unlocked.
         */
        public synchronized void unlock() {
            if (!locked) {
                return;
            }
            locked = false;
            try {
                if (watchdog != null) {
                    watchdog.interrupt();
                }
            } catch (Exception e) {
                LOG.log(Level.FINE, "unlock interrupt", e);
            }
            try {
                if (file != null) {
                    if (load() == id) {
                        delete();
                    }
                }
            } catch (Exception e) {
                LOG.log(Level.FINE, "unlock", e);
            } finally {
                file = null;
            }
            try {
                if (watchdog != null) {
                    watchdog.join();
                }
            } catch (Exception e) {
                LOG.log(Level.FINE, "unlock", e);
            } finally {
                watchdog = null;
            }
        }


        private void save() throws IOException {
            //save file
            RandomAccessFile raf = new RandomAccessFile(file,"rw");
            raf.seek(0);
            raf.writeLong(id);
            raf.getFD().sync(); //TODO is raf synced on close? In that case this is redundant, it applies to Volumes etc
            raf.close();
            lastWrite = file.lastModified();
        }

        private long load() throws IOException{
            //load file
            RandomAccessFile raf = new RandomAccessFile(file,"r");
            raf.seek(0);
            long ret = raf.readLong();
            raf.close();
            return ret;
        }

        private static void sleep(int delay){
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                throw new DBException.Interrupted(e);
            }
        }

        protected void delete() {
            for (int i = 0; i < CC.FILE_RETRY; i++) { //TODO use delete/retry mapdb wide, in compaction!
                boolean ok = file.delete();
                if (ok || !file.exists()) {
                    return;
                }
                wait(i);
            }
            throw new DBException.FileDeleteFailed(file);
        }

        //TODO h2 code, check context and errors. what it is ????
        private static void wait(int i) {
            if (i == 8) {
                System.gc();
            }
            try {
                // sleep at most 256 ms
                long sleep = Math.min(256, i * i);
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        public boolean isLocked() {
            return locked;
        }

        public File getFile() {
            return file;
        }
    }

    static final long PRIME64_1 = -7046029288634856825L; //11400714785074694791
    static final long PRIME64_2 = -4417276706812531889L; //14029467366897019727
    static final long PRIME64_3 = 1609587929392839161L;
    static final long PRIME64_4 = -8796714831421723037L; //9650029242287828579
    static final long PRIME64_5 = 2870177450012600261L;

    /**
     * <p>
     * Calculates XXHash64 from given {@code byte[]} buffer.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param buf to calculate hash from
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public static long hash(byte[] buf, int off, int len, long seed) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }

        if(off<0 || off>buf.length || off+len<0 || off+len>buf.length){
            throw new IndexOutOfBoundsException();
        }

        final int end = off + len;
        long h64;

        if (len >= 32) {
            final int limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += readLongLE(buf, off) * PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 8;

                v2 += readLongLE(buf, off) * PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 8;

                v3 += readLongLE(buf, off) * PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 8;

                v4 += readLongLE(buf, off) * PRIME64_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                off += 8;
            } while (off <= limit);

            h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2; v2 = rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2; v3 = rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2; v4 = rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
            h64 = h64 * PRIME64_1 + PRIME64_4;
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (off <= end - 8) {
            long k1 = readLongLE(buf, off);
            k1 *= PRIME64_2; k1 = rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 8;
        }

        if (off <= end - 4) {
            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 4;
        }

        while (off < end) {
            h64 ^= (buf[off] & 0xFF) * PRIME64_5;
            h64 = rotateLeft(h64, 11) * PRIME64_1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }


    static long readLongLE(byte[] buf, int i) {
        return (buf[i] & 0xFFL) | ((buf[i+1] & 0xFFL) << 8) | ((buf[i+2] & 0xFFL) << 16) | ((buf[i+3] & 0xFFL) << 24)
                | ((buf[i+4] & 0xFFL) << 32) | ((buf[i+5] & 0xFFL) << 40) | ((buf[i+6] & 0xFFL) << 48) | ((buf[i+7] & 0xFFL) << 56);
    }


    static int readIntLE(byte[] buf, int i) {
        return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
    }


    /**
     * <p>
     * Calculates XXHash64 from given {@code char[]} buffer.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param buf to calculate hash from
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public static long hash(char[] buf, int off, int len, long seed) {
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        if(off<0 || off>buf.length || off+len<0 || off+len>buf.length){
            throw new IndexOutOfBoundsException();
        }

        final int end = off + len;
        long h64;

        if (len >= 16) {
            final int limit = end - 16;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += readLongLE(buf, off) * PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 4;

                v2 += readLongLE(buf, off) * PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 4;

                v3 += readLongLE(buf, off) * PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 4;

                v4 += readLongLE(buf, off) * PRIME64_2;
                v4 = rotateLeft(v4, 31);
                v4 *= PRIME64_1;
                off += 4;
            } while (off <= limit);

            h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

            v1 *= PRIME64_2; v1 = rotateLeft(v1, 31); v1 *= PRIME64_1; h64 ^= v1;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v2 *= PRIME64_2; v2 = rotateLeft(v2, 31); v2 *= PRIME64_1; h64 ^= v2;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v3 *= PRIME64_2; v3 = rotateLeft(v3, 31); v3 *= PRIME64_1; h64 ^= v3;
            h64 = h64 * PRIME64_1 + PRIME64_4;

            v4 *= PRIME64_2; v4 = rotateLeft(v4, 31); v4 *= PRIME64_1; h64 ^= v4;
            h64 = h64 * PRIME64_1 + PRIME64_4;
        } else {
            h64 = seed + PRIME64_5;
        }

        h64 += len;

        while (off <= end - 4) {
            long k1 = readLongLE(buf, off);
            k1 *= PRIME64_2; k1 = rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 4;
        }

        if (off <= end - 2) {
            h64 ^= (readIntLE(buf, off) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 2;
        }

        while (off < end) {
            h64 ^= (readCharLE(buf,off) & 0xFFFF) * PRIME64_5;
            h64 = rotateLeft(h64, 11) * PRIME64_1;
            ++off;
        }

        h64 ^= h64 >>> 33;
        h64 *= PRIME64_2;
        h64 ^= h64 >>> 29;
        h64 *= PRIME64_3;
        h64 ^= h64 >>> 32;

        return h64;
    }

    static long readLongLE(char[] buf, int i) {
        return (buf[i] & 0xFFFFL) |
                ((buf[i+1] & 0xFFFFL) << 16) |
                ((buf[i+2] & 0xFFFFL) << 32) |
                ((buf[i+3] & 0xFFFFL) << 48);

    }


    static int readIntLE(char[] buf, int i) {
        return (buf[i] & 0xFFFF) |
                ((buf[i+1] & 0xFFFF) << 16);
    }

    static int readCharLE(char[] buf, int i) {
        return buf[i];
    }

}
