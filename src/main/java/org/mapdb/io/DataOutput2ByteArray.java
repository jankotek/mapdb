package org.mapdb.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Output of serialization
 */
public class DataOutput2ByteArray extends OutputStream implements DataOutput2{

    public byte[] buf;
    public int pos; //TODO private fields vs getters?
    public int sizeMask;


    public DataOutput2ByteArray(){
        pos = 0;
        buf = new byte[128]; //PERF take hint from serializer for initial size
        sizeMask = 0xFFFFFFFF-(buf.length-1);
    }


    @Override public byte[] copyBytes(){
        return Arrays.copyOf(buf, pos);
    }

    /**
     * make sure there will be enough space in buffer to write N bytes
     * @param n number of bytes which can be safely written after this method returns
     */
    protected void ensureAvail(int n) {
        //$DELAY$
        n+=pos;
        if ((n&sizeMask)!=0) {
            grow(n);
        }
    }

    private void grow(int n) {
        //$DELAY$
        int newSize = Math.max(DataIO.nextPowTwo(n),buf.length);
        sizeMask = 0xFFFFFFFF-(newSize-1);
        buf = Arrays.copyOf(buf, newSize);
    }


    @Override
    public void write(final int b){
        ensureAvail(1);
        //$DELAY$
        buf[pos++] = (byte) b;
    }

    @Override
    public void write(byte[] b){
        write(b,0,b.length);
    }

    @Override
    public void write(final byte[] b, final int off, final int len){
        ensureAvail(len);
        //$DELAY$
        System.arraycopy(b, off, buf, pos, len);
        pos += len;
    }

    @Override
    public void writeBoolean(final boolean v){
        ensureAvail(1);
        //$DELAY$
        buf[pos++] = (byte) (v ? 1 : 0);
    }

    @Override
    public void writeByte(final int v){
        ensureAvail(1);
        //$DELAY$
        buf[pos++] = (byte) (v);
    }

    @Override
    public void writeShort(final int v){
        ensureAvail(2);
        //$DELAY$
        buf[pos++] = (byte) (0xff & (v >> 8));
        //$DELAY$
        buf[pos++] = (byte) (0xff & (v));
    }

    @Override
    public void writeChar(final int v){
        ensureAvail(2);
        buf[pos++] = (byte) (v>>>8);
        buf[pos++] = (byte) (v);
    }

    @Override
    public void writeInt(final int v){
        ensureAvail(4);
        buf[pos++] = (byte) (0xff & (v >> 24));
        //$DELAY$
        buf[pos++] = (byte) (0xff & (v >> 16));
        buf[pos++] = (byte) (0xff & (v >> 8));
        //$DELAY$
        buf[pos++] = (byte) (0xff & (v));
    }

    @Override
    public void writeLong(final long v){
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
    public void writeFloat(final float v){
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(final double v){
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(final String s){
        writeUTF(s);
    }

    @Override
    public void writeChars(final String s){
        writeUTF(s);
    }

    @Override
    public void writeUTF(final String s){
        final int len = s.length();
        writePackedInt(len);
        for (int i = 0; i < len; i++) {
            //$DELAY$
            int c = (int) s.charAt(i);
            writePackedInt(c);
        }
    }


    //TODO evaluate  packed methods
    public void packInt(int value){
        writeInt(value);
    }
//    public void packInt(int value){
//        ensureAvail(5); //ensure worst case bytes
//
//        // Optimize for the common case where value is small. This is particular important where our caller
//        // is SerializerBase.SER_STRING.serialize because most chars will be ASCII characters and hence in this range.
//        // credit Max Bolingbroke https://github.com/jankotek/MapDB/pull/489
//        int shift = (value & ~0x7F); //reuse variable
//        if (shift != 0) {
//            shift = 31 - Integer.numberOfLeadingZeros(value);
//            shift -= shift % 7; // round down to nearest multiple of 7
//            while (shift != 0) {
//                buf[pos++] = (byte) ((value >>> shift) & 0x7F);
//                shift -= 7;
//            }
//        }
//        buf[pos++] = (byte) ((value & 0x7F)| 0x80);
//    }
//
//    public void packIntBigger(int value){
//        ensureAvail(5); //ensure worst case bytes
//        int shift = 31-Integer.numberOfLeadingZeros(value);
//        shift -= shift%7; // round down to nearest multiple of 7
//        while(shift!=0){
//            buf[pos++] = (byte) ((value>>>shift) & 0x7F);
//            shift-=7;
//        }
//        buf[pos++] = (byte) ((value & 0x7F)|0x80);
//    }
//
//    public void packLong(long value) {
//        ensureAvail(10); //ensure worst case bytes
//        int shift = 63-Long.numberOfLeadingZeros(value);
//        shift -= shift%7; // round down to nearest multiple of 7
//        while(shift!=0){
//            buf[pos++] = (byte) ((value>>>shift) & 0x7F);
//            shift-=7;
//        }
//        buf[pos++] = (byte) ((value & 0x7F) | 0x80);
//    }
//
//
//    public void packLongArray(long[] array, int fromIndex, int toIndex  ) {
//        for(int i=fromIndex;i<toIndex;i++){
//            long value = array[i];
//            ensureAvail(10); //ensure worst case bytes
//            int shift = 63-Long.numberOfLeadingZeros(value);
//            shift -= shift%7; // round down to nearest multiple of 7
//            while(shift!=0){
//                buf[pos++] = (byte) ((value>>>shift) & 0x7F);
//                shift-=7;
//            }
//            buf[pos++] = (byte) ((value & 0x7F) | 0x80);
//        }
//    }

    @Override
    public void sizeHint(int size) {
        ensureAvail(size);
    }

    @Override
    public void writePackedInt(int value){
        //TODO packed int and long
        writeInt(value);
    }

    @Override
    public void writePackedLong(long value){
        writeLong(value);
    }

    @Override
    public void writeRecid(long recid){
        //TODO 6 bytes
        //TODO parity bit
        writeLong(recid);
    }

    @Override
    public void writePackedRecid(long recid){
        //TODO parity bit
        writePackedLong(recid);
    }
}
