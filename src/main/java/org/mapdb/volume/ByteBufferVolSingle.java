package org.mapdb.volume;

import org.mapdb.CC;
import org.mapdb.DataInput2;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Abstract Volume over single ByteBuffer, maximal size is 2GB (32bit limit).
 * It leaves ByteBufferVol details (allocation, disposal) on subclasses.
 * Most methods are final for better performance (JIT compiler can inline those).
 */
abstract public class ByteBufferVolSingle extends Volume {

    protected final  boolean cleanerHackEnabled;

    protected ByteBuffer buffer;

    protected final boolean readOnly;
    protected final long maxSize;


    protected ByteBufferVolSingle(boolean readOnly, long maxSize, boolean cleanerHackEnabled) {
        //TODO assert size
        this.readOnly = readOnly;
        this.maxSize = maxSize;
        this.cleanerHackEnabled = cleanerHackEnabled;
    }

    @Override
    public void ensureAvailable(long offset) {
        //TODO throw error if too big
    }

    @Override public final void putLong(final long offset, final long value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
            new IOException("VOL STACK:").printStackTrace();
        }

        buffer.putLong((int) offset, value);
    }

    @Override public final void putInt(final long offset, final int value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
            new IOException("VOL STACK:").printStackTrace();
        }

        buffer.putInt((int) (offset), value);
    }


    @Override public final void putByte(final long offset, final byte value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+1){
            new IOException("VOL STACK:").printStackTrace();
        }

        buffer.put((int) offset, value);
    }



    @Override public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+srcSize){
            new IOException("VOL STACK:").printStackTrace();
        }


        final ByteBuffer b1 = buffer.duplicate();
        final int bufPos = (int) offset;

        b1.position(bufPos);
        b1.put(src, srcPos, srcSize);
    }


    @Override public final void putData(final long offset, final ByteBuffer buf) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+buf.remaining()){
            new IOException("VOL STACK:").printStackTrace();
        }

        final ByteBuffer b1 = buffer.duplicate();
        final int bufPos = (int) offset;
        //no overlap, so just write the value
        b1.position(bufPos);
        b1.put(buf);
    }

    @Override
    public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
        final ByteBuffer b1 = buffer.duplicate();
        final int bufPos = (int) inputOffset;

        b1.position(bufPos);
        //TODO size>Integer.MAX_VALUE
        b1.limit((int) (bufPos + size));
        target.putData(targetOffset, b1);
    }

    @Override public void getData(final long offset, final byte[] src, int srcPos, int srcSize){
        final ByteBuffer b1 = buffer.duplicate();
        final int bufPos = (int) offset;

        b1.position(bufPos);
        b1.get(src, srcPos, srcSize);
    }


    @Override final public long getLong(long offset) {
        return buffer.getLong((int) offset);
    }

    @Override final public int getInt(long offset) {
        return buffer.getInt((int) offset);
    }


    @Override public final byte getByte(long offset) {
        return buffer.get((int) offset);
    }


    @Override
    public final DataInput2.ByteBuffer getDataInput(long offset, int size) {
        return new DataInput2.ByteBuffer(buffer, (int) (offset));
    }



    @Override
    public void putDataOverlap(long offset, byte[] data, int pos, int len) {
        putData(offset,data,pos,len);
    }

    @Override
    public DataInput2 getDataInputOverlap(long offset, int size) {
        //return mapped buffer
        return getDataInput(offset,size);
    }


    @Override
    public void clear(long startOffset, long endOffset) {
        int start = (int) (startOffset);
        int end = (int) (endOffset);

        ByteBuffer buf = buffer;

        int pos = start;
        while(pos<end){
            buf = buf.duplicate();
            buf.position(pos);
            buf.put(CLEAR, 0, Math.min(CLEAR.length, end-pos));
            pos+=CLEAR.length;
        }
    }



    @Override
    public int sliceSize() {
        return -1;
    }

    @Override
    public boolean isSliced() {
        return false;
    }


}
