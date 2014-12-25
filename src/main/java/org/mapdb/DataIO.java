package org.mapdb;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Various IO classes and utilities..
 */
public final class DataIO {

    private DataIO(){}


    /* unpackInt,unpackLong, packInt, and packLong originally come from Kryo framework
     * and were written by Nathan Sweet.
     * It was modified to fit MapDB purposes.
     * It is relicensed from BSD to Apache 2 with his permission:
     *
     * Date: 27.5.2014 12:44
     *
     *   Hi Jan,
     *
     *   I'm fine with you putting code from the Kryo under Apache 2.0, as long as you keep the copyright and author. :)
     *
     *   Cheers!
     *   -Nate
     *
     * -----------------------------
     *
     *  Copyright (c) 2012 Nathan Sweet
     *
     *  Licensed under the Apache License, Version 2.0 (the "License");
     *  you may not use this file except in compliance with the License.
     *  You may obtain a copy of the License at
     *
     *    http://www.apache.org/licenses/LICENSE-2.0
     *
     *  Unless required by applicable law or agreed to in writing, software
     *  distributed under the License is distributed on an "AS IS" BASIS,
     *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     *  See the License for the specific language governing permissions and
     *  limitations under the License.
     */

    /**
     * Unpack int value from the input stream.
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was heavily modified to fit MapDB needs.
     *
     * @param is The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public int unpackInt(DataInput is) throws IOException {
        int offset = 0;
        int result=0;
        int b;
        do {
            b = is.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if(CC.PARANOID && offset>32)
                throw new AssertionError();
            offset += 7;
        }while((b & 0x80) != 0);
        return result;
    }

    /**
     * Unpack long value from the input stream.
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was heavily modified to fit MapDB needs.
     *
     * @param in The input stream.
     * @return The long value.
     * @throws java.io.IOException
     */
    static public long unpackLong(DataInput in) throws IOException {
        //$DELAY$
        int offset = 0;
        long result=0;
        long b;
        do {
            //$DELAY$
            b = in.readUnsignedByte();
            result |= (b & 0x7F) << offset;
            if(CC.PARANOID && offset>64)
                throw new AssertionError();
            offset += 7;
        }while((b & 0x80) != 0);
        //$DELAY$
        return result;
    }


    /**
     * Pack long into output stream.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was modified to fit MapDB needs.
     *
     * @param out DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException
     *
     */
    static public void packLong(DataOutput out, long value) throws IOException {
        //$DELAY$
        while ((value & ~0x7FL) != 0) {
            out.write((((int) value & 0x7F) | 0x80));
            value >>>= 7;
            //$DELAY$
        }
        //$DELAY$
        out.write((byte) value);
    }

    /**
     * Pack  int into output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * This method originally comes from Kryo Framework, author Nathan Sweet.
     * It was modified to fit MapDB needs.
     *
     * @param in DataOutput to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException
     */

    static public void packInt(DataOutput in, int value) throws IOException {
        //$DELAY$
        while ((value & ~0x7F) != 0) {
            //$DELAY$
            in.write(((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        //$DELAY$
        in.write((byte) value);
    }

    public static int longHash(final long key) {
        //$DELAY$
        int h = (int)(key ^ (key >>> 32));
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public static int intHash(int h) {
        //$DELAY$
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public static final long PACK_LONG_BIDI_MASK = 0xFFFFFFFFFFFFFFL;


    public static int packLongBidi(DataOutput out, long value) throws IOException {
        out.write((((int) value & 0x7F))| 0x80);
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
        if(CC.PARANOID && (b&0x80)==0)
            throw new AssertionError();
        long result = (b & 0x7F) ;
        int offset = 7;
        do {
            //$DELAY$
            b = bb[pos++];
            result |= (b & 0x7F) << offset;
            if(CC.PARANOID && offset>64)
                throw new AssertionError();
            offset += 7;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)(offset/7))<<56) | result;
    }


    public static long unpackLongBidiReverse(byte[] bb, int pos){
        //$DELAY$
        long b = bb[--pos];
        if(CC.PARANOID && (b&0x80)==0)
            throw new AssertionError();
        long result = (b & 0x7F) ;
        int counter = 1;
        do {
            //$DELAY$
            b = bb[--pos];
            result = (b & 0x7F) | (result<<7);
            if(CC.PARANOID && counter>8)
                throw new AssertionError();
            counter++;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)counter)<<56) | result;
    }

    public static long getLong(byte[] buf, int pos) {
        final int end = pos + 8;
        long ret = 0;
        for (; pos < end; pos++) {
            ret = (ret << 8) | (buf[pos] & 0xFF);
        }
        return ret;
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




    public static int nextPowTwo(final int a)
    {
        //$DELAY$
        int b = 1;
        while (b < a)
        {
            b = b << 1;
        }
        return b;
    }


    /**
     * Give access to internal byte[] or ByteBuffer in DataInput2..
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
            return (((buf[pos++] & 0xff) << 8) |
                    ((buf[pos++] & 0xff)));
        }

        @Override
        public char readChar() throws IOException {
            //$DELAY$
            // I know: 4 bytes, but char only consumes 2,
            // has to stay here for backward compatibility
            //TODO char 4 byte
            return (char) readInt();
        }

        @Override
        public int readInt() throws IOException {
            final int end = pos + 4;
            int ret = 0;
            for (; pos < end; pos++) {
                //$DELAY$
                ret = (ret << 8) | (buf[pos] & 0xFF);
            }
            return ret;
        }

        @Override
        public long readLong() throws IOException {
            final int end = pos + 8;
            long ret = 0;
            for (; pos < end; pos++) {
                //$DELAY$
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
            final int len = unpackInt();
            char[] b = new char[len];
            for (int i = 0; i < len; i++)
                //$DELAY$
                //TODO char 4 bytes
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

        protected int unpackInt() throws IOException {
            int offset = 0;
            int result=0;
            int b;
            do {
                //$DELAY$
                b = buf[pos++];
                result |= (b & 0x7F) << offset;
                offset += 7;
            }while((b & 0x80) != 0);
            //$DELAY$
            return result;
        }


    }


    /**
     * Wraps `DataInput` into `InputStream`
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
         * @deprecated  use {@link org.mapdb.DataIO.DataInputByteArray}
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
            //$DELAY$
            return (( (buf.get(pos++) & 0xff) << 8) |
                    ( (buf.get(pos++) & 0xff)));
        }

        @Override
        public char readChar() throws IOException {
            //$DELAY$
            // I know: 4 bytes, but char only consumes 2,
            // has to stay here for backward compatibility
            //TODO 4 byte char
            return (char) readInt();
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
            final int size = unpackInt(this);
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
    }

    /**
     * Provides {@link java.io.DataOutput} implementation on top of growable {@code byte[]}
     * <p>
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
            buf = new byte[16]; //TODO take hint from serializer for initial size
            sizeMask = 0xFFFFFFFF-(buf.length-1);
        }


        public byte[] copyBytes(){
            return Arrays.copyOf(buf, pos);
        }

        /**
         * make sure there will be enough space in buffer to write N bytes
         */
        public void ensureAvail(int n) {
            //$DELAY$
            n+=pos;
            if ((n&sizeMask)!=0) {
                grow(n);

            }
        }

        private void grow(long n) {
            //$DELAY$
            int newSize = buf.length;
            while(newSize<n){
                //$DELAY$
                newSize<<=2;
                sizeMask<<=2;
            }
            //$DELAY$
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
            // I know: 4 bytes, but char only consumes 2,
            // has to stay here for backward compatibility
            //TODO 4 byte char
            writeInt(v);
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

        //TODO remove pack methods  perhaps
        protected void packInt(int value) throws IOException {
            if(CC.PARANOID && value<0)
                throw new AssertionError("negative value: "+value);

            while ((value & ~0x7F) != 0) {
                ensureAvail(1);
                //$DELAY$
                buf[pos++]= (byte) ((value & 0x7F) | 0x80);
                value >>>= 7;
            }
            //$DELAY$
            ensureAvail(1);
            buf[pos++]= (byte) value;
        }

    }


    public static long parity1Set(long i) {
        if(CC.PARANOID && (i&1)!=0)
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
        if(CC.PARANOID && (i&0x7)!=0)
            throw new DBException.PointerChecksumBroken(); //TODO stronger parity
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
        if(CC.PARANOID && (i&0xF)!=0)
            throw new DBException.PointerChecksumBroken(); //TODO stronger parity
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
        if(CC.PARANOID && (i&0xFFFF)!=0)
            throw new DBException.PointerChecksumBroken(); //TODO stronger parity
        return i | ((Long.bitCount(i)+1)%2);
    }

    public static long parity16Get(long i) {
        if(Long.bitCount(i)%2!=1){
            throw new DBException.PointerChecksumBroken();
        }
        return i&0xFFFFFFFFFFFF0000L;
    }




}
