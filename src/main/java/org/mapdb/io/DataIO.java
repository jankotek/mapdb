package org.mapdb.io;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

/**
 * Various IO classes and utilities..
 */
public final class DataIO {

    private DataIO(){}

    /**
     * Unpack int value from the input stream.
     *
     * @param in The input stream.
     * @return The long value.
     *a
     * @throws java.io.IOException in case of IO error
     */
    static public int unpackInt(DataInput2 in){
        int ret = 0;
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
     *a
     * @throws java.io.IOException in case of IO error
     */
    static public int unpackInt(DataInput in) throws IOException {
        int ret = 0;
        byte v;
        do{
            v = in.readByte();
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
    static public long unpackLong(DataInput2 in){
        long ret = 0;
        byte v;
        do{
            v = in.readByte();
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);

        return ret;
    }

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
    static public long unpackRecid(DataInput2 in){
        long val = in.readPackedLong();
        val = DataIO.parity1Get(val);
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
    static public void packRecid(DataOutput2 out, long value){
        value = DataIO.parity1Set(value<<1);
        out.writePackedLong(value);
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

    static public void packInt(DataOutput2 out, int value){
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
        buf[pos++] = (byte) (0xff & (v >> 24));  //TODO PERF is >>> faster here? Also invert 0xFF &?
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

    public static void putLong(byte[] buf, int pos,long v, int vSize) {
        for(int i=vSize-1; i>=0; i--){
            buf[i+pos] = (byte) (0xff & v);
            v >>>=8;
        }
    }


    public static int packInt(byte[] buf, int pos, int value){
        int pos2 = pos;
        int shift = 31-Integer.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            buf[pos++] = (byte) ((value>>>shift) & 0x7F);
            shift-=7;
        }
        buf[pos++] = (byte) ((value & 0x7F)|0x80);
        return pos-pos2;
    }

    public static int packLong(byte[] buf, int pos, long value){
        int pos2 = pos;

        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            buf[pos++] = (byte) ((value>>>shift) & 0x7F);
            shift-=7;
        }
        buf[pos++] = (byte) ((value & 0x7F) | 0x80);
        return pos - pos2;
    }


    public static int unpackInt(byte[] buf, int pos){
        int ret = 0;
        byte v;
        do{
            //$DELAY$
            v = buf[pos++];
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);
        return ret;
    }


    public static long unpackLong(byte[] buf, int pos){
        long ret = 0;
        byte v;
        do{
            //$DELAY$
            v = buf[pos++];
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);
        return ret;
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
        if(CC.PARANOID && (value>>>48!=0))
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

    public static void writeFully(FileChannel f, ByteBuffer buf ) throws IOException {
        int rem = buf.remaining();
        while(rem>0)
            rem-=f.write(buf);
    }


    public static void readFully(@NotNull  FileChannel f, @NotNull ByteBuffer buf, long offset ) throws IOException {
        int rem = buf.remaining();
        while(rem>0) {
            int read = f.read(buf, offset);
            if(read<0)
                throw new EOFException();
            rem-=read;
            offset+=read;
        }
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
        if(CC.PARANOID && (i&1)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | ((Long.bitCount(i)+1)%2);
    }

    public static int parity1Set(int i) {
        if(CC.PARANOID && (i&1)!=0)
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
        if(CC.PARANOID && (i&0x7)!=0)
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
        if(CC.PARANOID && (i&0xF)!=0)
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
        if(CC.PARANOID && (i&0xFFFF)!=0)
            throw new DBException.PointerChecksumBroken();
        return i | (DataIO.longHash(i+1)&0xFFFFL);
    }

    public static long parity16Get(long i) {
        long ret = i&0xFFFFFFFFFFFF0000L;
        if((DataIO.longHash(ret+1)&0xFFFFL) != (i&0xFFFFL)){
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


    public static int shift(int value) {
        return 31-Integer.numberOfLeadingZeros(value);
    }


    /** return true if operating system is Windows*/
    public static boolean isWindows(){
        String os = System.getProperty("os.name");
        return os!=null && os.toLowerCase().startsWith("windows");
    }

    /**
     * Check if large files can be mapped into memory.
     * For example 32bit JVM can only address 2GB and large files can not be mapped,
     * so for 32bit JVM this function returns false.
     *
     */
    public static boolean JVMSupportsLargeMappedFiles() {
        String arch = System.getProperty("os.arch");
        if(arch==null || !arch.contains("64")) {
            return false;
        }

        if(isWindows())
            return false;

        //TODO better check for 32bit JVM
        return true;
    }

    @NotNull
    public static int readInt(@NotNull FileChannel c, long offset) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(4);
        readFully(c, b, offset);
        return getInt(b.array(),0);
    }
}
