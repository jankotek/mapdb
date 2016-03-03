package org.mapdb;

import java.io.*;
import java.util.Arrays;

import static java.lang.Long.rotateLeft;

/**
 * Various IO classes and utilities..
 */
public final class DBUtil {

    private DBUtil(){}

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
        }while((v&0x80)==0);

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
        }while((v&0x80)==0);

        return ret;
    }


    /**
     * Unpack int value from the input stream.
     *
     * @param in The input stream.
     * @return The long value.
     *
     * @throws java.io.IOException in case of IO error
     */
    static public int unpackInt(InputStream in) throws IOException {
        int ret = 0;
        int v;
        do{
            v = in.read();
            if(v==-1)
                throw new EOFException();
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);

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
        }while((v&0x80)==0);

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
            out.writeByte((byte) ((value>>>shift) & 0x7F) );
            //$DELAY$
            shift-=7;
        }
        out.writeByte((byte) ((value & 0x7F)|0x80));
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
            out.write((int) ((value>>>shift) & 0x7F));
            //$DELAY$
            shift-=7;
        }
        out.write((int) ((value & 0x7F)|0x80));
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
            //PERF remove cycle, just count zeroes
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
    static public long unpackRecid(DataInput2 in) throws IOException {
        long val = in.unpackLong();
        val = DBUtil.parity1Get(val);
        return val >>> 1;
    }


    /**
     * Pack RECID into output stream with 3 bit checksum.
     * It will occupy 1-10 bytes depending on value (lower values occupy smaller space)
     *
     * @param out String to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException in case of IO error
     */
    static public void packRecid(DataOutput2 out, long value) throws IOException {
        value = DBUtil.parity1Set(value<<1);
        out.packLong(value);
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
                out.writeByte((byte) ((value>>>shift) & 0x7F));
                //$DELAY$
                shift-=7;
            }
        }
        //$DELAY$
        out.writeByte((byte) ((value & 0x7F)|0x80));
    }

    /**
     * Pack int into an output stream.
     * It will occupy 1-5 bytes depending on value (lower values occupy smaller space)
     *
     * This method is same as {@link #packInt(DataOutput, int)},
     * but is optimized for values larger than 127. Usually it is recids.
     *
     * @param out String to put value into
     * @param value to be serialized, must be non-negative
     * @throws java.io.IOException in case of IO error
     */

    static public void packIntBigger(DataOutput out, int value) throws IOException {
        //$DELAY$
        int shift = 31-Integer.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            out.writeByte((byte) ((value>>>shift) & 0x7F));
            //$DELAY$
            shift-=7;
        }
        //$DELAY$
        out.writeByte((byte) ((value & 0x7F)|0x80));
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


    public static int getInt(byte[] buf, int pos) {
       return
                (((int)buf[pos++]) << 24) |
                (((int)buf[pos++] & 0xFF) << 16) |
                (((int)buf[pos++] & 0xFF) <<  8) |
                (((int)buf[pos] & 0xFF));
    }

    public static void putInt(byte[] buf, int pos,int v) {
        buf[pos++] = (byte) (0xff & (v >> 24));
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        buf[pos] = (byte) (0xff & (v));
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

    public static void readFully(InputStream in, byte[] data, int offset, int  len) throws IOException {
        len+=offset;
        for(; offset<len;){
            int c = in.read(data, offset, len - offset);
            if(c<0)
                throw new EOFException();
            offset+=c;
        }
    }

    public static void readFully(InputStream in, byte[] data) throws IOException {
        readFully(in, data, 0, data.length);
    }


    public static void skipFully(InputStream in, long length) throws IOException {
        while ((length -= in.skip(length)) > 0);
    }

    public static long fillLowBits(int bitCount) {
        long ret = 0;
        for(;bitCount>0;bitCount--){
            ret = (ret<<1)|1;
        }
        return ret;
    }


    public static long parity1Set(long i) {
        if(CC.ASSERT && (i&1)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Long.bitCount(i)+1)%2);
    }

    public static int parity1Set(int i) {
        if(CC.ASSERT && (i&1)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Integer.bitCount(i)+1)%2);
    }

    public static long parity1Get(long i) {
        if(Long.bitCount(i)%2!=1){
            throw new DBException.PointerChecksumBroken();
        }
        return i&0xFFFFFFFFFFFFFFFEL;
    }


    public static int parity1Get(int i) {
        if(Integer.bitCount(i)%2!=1){
            throw new DBException.PointerChecksumBroken();
        }
        return i&0xFFFFFFFE;
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
        return i | (DBUtil.longHash(i+1)&0xFFFFL);
    }

    public static long parity16Get(long i) {
        long ret = i&0xFFFFFFFFFFFF0000L;
        if((DBUtil.longHash(ret+1)&0xFFFFL) != (i&0xFFFFL)){
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

    public static final long PRIME64_1 = -7046029288634856825L; //11400714785074694791
    public static final long PRIME64_2 = -4417276706812531889L; //14029467366897019727
    public static final long PRIME64_3 = 1609587929392839161L;
    public static final long PRIME64_4 = -8796714831421723037L; //9650029242287828579
    public static final long PRIME64_5 = 2870177450012600261L;

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

    /* expand array size by 1, and put value at given position. No items from original array are lost*/
    public static Object[] arrayPut(final Object[] array, final int pos, final Object value){
        final Object[] ret = Arrays.copyOf(array, array.length+1);
        if(pos<array.length){
            System.arraycopy(array, pos, ret, pos+1, array.length-pos);
        }
        ret[pos] = value;
        return ret;
    }


    public static Object[] arrayDelete(Object[] vals, int pos, int len) {
        Object[] vals2 = new Object[vals.length-len];
        System.arraycopy(vals,0,vals2, 0, pos-len);
        System.arraycopy(vals, pos, vals2, pos-len, vals2.length-(pos-len));
        return vals2;
    }

    public static long[] arrayDelete(long[] vals, int pos, int len) {
         long[] vals2 = new long[vals.length - len];
         System.arraycopy(vals, 0, vals2, 0, pos - len);
         System.arraycopy(vals, pos, vals2, pos - len, vals2.length - (pos - len));
         return vals2;
    }

    /** converts 4 int bytes to lowest 4 long bytes. Does not preserve negative flag */
    public static long intToLong(int i) {
        return i & 0xffffffffL;
    }

    public static long roundUp(long number, long roundUpToMultipleOf) {
        return ((number+roundUpToMultipleOf-1)/(roundUpToMultipleOf))*roundUpToMultipleOf;
    }

    public static long roundDown(long number, long roundDownToMultipleOf) {
        return number  - number % roundDownToMultipleOf;
    }

    public static int roundUp(int number, int roundUpToMultipleOf) {
        return ((number+roundUpToMultipleOf-1)/(roundUpToMultipleOf))*roundUpToMultipleOf;
    }

    public static int roundDown(int number, int roundDownToMultipleOf) {
        return number  - number % roundDownToMultipleOf;
    }

}
