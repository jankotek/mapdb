package org.mapdb.volume;

import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DataInput2;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract Volume over bunch of ByteBuffers
 * It leaves ByteBufferVol details (allocation, disposal) on subclasses.
 * Most methods are final for better performance (JIT compiler can inline those).
 */
abstract public class ByteBufferVol extends Volume {

    protected final  boolean cleanerHackEnabled;

    protected final ReentrantLock growLock = new ReentrantLock();
    protected final int sliceShift;
    protected final int sliceSizeModMask;
    protected final int sliceSize;

    protected volatile ByteBuffer[] slices = new ByteBuffer[0];
    protected final boolean readOnly;

    protected ByteBufferVol(boolean readOnly, int sliceShift, boolean cleanerHackEnabled) {
        this.readOnly = readOnly;
        this.sliceShift = sliceShift;
        this.cleanerHackEnabled = cleanerHackEnabled;
        this.sliceSize = 1<< sliceShift;
        this.sliceSizeModMask = sliceSize -1;
    }


    protected final ByteBuffer getSlice(long offset){
        ByteBuffer[] slices = this.slices;
        int pos = (int)(offset >>> sliceShift);
        if(pos>=slices.length)
            throw new DBException.VolumeEOF("Get/Set beyond file size. Requested offset: "+offset+", volume size: "+length());
        return slices[pos];
    }

    @Override public final void putLong(final long offset, final long value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
            new IOException("VOL STACK:").printStackTrace();
        }

        getSlice(offset).putLong((int) (offset & sliceSizeModMask), value);
    }

    @Override public final void putInt(final long offset, final int value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
            new IOException("VOL STACK:").printStackTrace();
        }

        getSlice(offset).putInt((int) (offset & sliceSizeModMask), value);
    }


    @Override public final void putByte(final long offset, final byte value) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+1){
            new IOException("VOL STACK:").printStackTrace();
        }

        getSlice(offset).put((int) (offset & sliceSizeModMask), value);
    }



    @Override public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+srcSize){
            new IOException("VOL STACK:").printStackTrace();
        }


        final ByteBuffer b1 = getSlice(offset).duplicate();
        final int bufPos = (int) (offset& sliceSizeModMask);

        b1.position(bufPos);
        b1.put(src, srcPos, srcSize);
    }


    @Override public final void putData(final long offset, final ByteBuffer buf) {
        if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+buf.remaining()){
            new IOException("VOL STACK:").printStackTrace();
        }

        final ByteBuffer b1 = getSlice(offset).duplicate();
        final int bufPos = (int) (offset& sliceSizeModMask);
        //no overlap, so just write the value
        b1.position(bufPos);
        b1.put(buf);
    }

    @Override
    public void copyTo(long inputOffset, Volume target, long targetOffset, long size) {
        final ByteBuffer b1 =getSlice(inputOffset).duplicate();
        final int bufPos = (int) (inputOffset& sliceSizeModMask);

        b1.position(bufPos);
        //TODO size>Integer.MAX_VALUE
        b1.limit((int) (bufPos+size));
        target.putData(targetOffset, b1);
    }

    @Override public void getData(final long offset, final byte[] src, int srcPos, int srcSize){
        final ByteBuffer b1 = getSlice(offset).duplicate();
        final int bufPos = (int) (offset& sliceSizeModMask);

        b1.position(bufPos);
        b1.get(src, srcPos, srcSize);
    }


    @Override final public long getLong(long offset) {
        return getSlice(offset).getLong((int) (offset & sliceSizeModMask));
    }

    @Override final public int getInt(long offset) {
        return getSlice(offset).getInt((int) (offset & sliceSizeModMask));
    }


    @Override public final byte getByte(long offset) {
        return getSlice(offset).get((int) (offset & sliceSizeModMask));
    }


    @Override
    public final DataInput2.ByteBuffer getDataInput(long offset, int size) {
        return new DataInput2.ByteBuffer(getSlice(offset), (int) (offset& sliceSizeModMask));
    }



    @Override
    public void putDataOverlap(long offset, byte[] data, int pos, int len) {
        boolean overlap = (offset>>>sliceShift != (offset+len)>>>sliceShift);

        if(overlap){
            while(len>0){
                ByteBuffer b = getSlice(offset).duplicate();
                b.position((int) (offset&sliceSizeModMask));

                int toPut = Math.min(len,sliceSize - b.position());

                b.limit(b.position()+toPut);
                b.put(data, pos, toPut);

                pos+=toPut;
                len-=toPut;
                offset+=toPut;
            }
        }else{
            putData(offset,data,pos,len);
        }
    }

    @Override
    public DataInput2 getDataInputOverlap(long offset, int size) {
        boolean overlap = (offset>>>sliceShift != (offset+size)>>>sliceShift);
        if(overlap){
            byte[] bb = new byte[size];
            final int origLen = size;
            while(size>0){
                ByteBuffer b = getSlice(offset).duplicate();
                b.position((int) (offset&sliceSizeModMask));

                int toPut = Math.min(size,sliceSize - b.position());

                b.limit(b.position()+toPut);
                b.get(bb,origLen-size,toPut);
                size -=toPut;
                offset+=toPut;
            }
            return new DataInput2.ByteArray(bb);
        }else{
            //return mapped buffer
            return getDataInput(offset,size);
        }
    }


    @Override
    public void putUnsignedShort(long offset, int value) {
        final ByteBuffer b = getSlice(offset);
        int bpos = (int) (offset & sliceSizeModMask);

        b.put(bpos++, (byte) (value >> 8));
        b.put(bpos, (byte) (value));
    }

    @Override
    public int getUnsignedShort(long offset) {
        final ByteBuffer b = getSlice(offset);
        int bpos = (int) (offset & sliceSizeModMask);

        return (( (b.get(bpos++) & 0xff) << 8) |
                ( (b.get(bpos) & 0xff)));
    }

    @Override
    public int getUnsignedByte(long offset) {
        final ByteBuffer b = getSlice(offset);
        int bpos = (int) (offset & sliceSizeModMask);

        return b.get(bpos) & 0xff;
    }

    @Override
    public void putUnsignedByte(long offset, int byt) {
        final ByteBuffer b = getSlice(offset);
        int bpos = (int) (offset & sliceSizeModMask);

        b.put(bpos, toByte(byt));
    }

    protected static byte toByte(int byt) {
        return (byte) (byt & 0xff);
    }


    protected static byte toByte(long l) {
        return (byte) (l & 0xff);
    }
    @Override
    public long getSixLong(long pos) {
        final ByteBuffer bb = getSlice(pos);
        int bpos = (int) (pos & sliceSizeModMask);

        return
                ((long) (bb.get(bpos++) & 0xff) << 40) |
                        ((long) (bb.get(bpos++) & 0xff) << 32) |
                        ((long) (bb.get(bpos++) & 0xff) << 24) |
                        ((long) (bb.get(bpos++) & 0xff) << 16) |
                        ((long) (bb.get(bpos++) & 0xff) << 8) |
                        ((long) (bb.get(bpos) & 0xff));
    }

    @Override
    public void putSixLong(long pos, long value) {
        final ByteBuffer b = getSlice(pos);
        int bpos = (int) (pos & sliceSizeModMask);

        if(CC.ASSERT && (value >>>48!=0))
            throw new DBException.DataCorruption("six long out of range");

        b.put(bpos++, (byte) (0xff & (value >> 40)));
        b.put(bpos++, (byte) (0xff & (value >> 32)));
        b.put(bpos++, (byte) (0xff & (value >> 24)));
        b.put(bpos++, (byte) (0xff & (value >> 16)));
        b.put(bpos++, (byte) (0xff & (value >> 8)));
        b.put(bpos, (byte) (0xff & (value)));
    }

    @Override
    public int putPackedLong(long pos, long value) {
        final ByteBuffer b = getSlice(pos);
        int bpos = (int) (pos & sliceSizeModMask);

        //$DELAY$
        int ret = 0;
        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            b.put(bpos + (ret++), (byte) (((value >>> shift) & 0x7F) ));
            //$DELAY$
            shift-=7;
        }
        b.put(bpos +(ret++),(byte) ((value & 0x7F) | 0x80));
        return ret;
    }

    @Override
    public long getPackedLong(long position) {
        final ByteBuffer b = getSlice(position);
        int bpos = (int) (position & sliceSizeModMask);

        long ret = 0;
        int pos2 = 0;
        byte v;
        do{
            v = b.get(bpos +(pos2++));
            ret = (ret<<7 ) | (v & 0x7F);
        }while((v&0x80)==0);

        return (((long)pos2)<<60) | ret;
    }

    @Override
    public void clear(long startOffset, long endOffset) {
        if(CC.ASSERT && (startOffset >>> sliceShift) != ((endOffset-1) >>> sliceShift))
            throw new AssertionError();
        ByteBuffer buf = getSlice(startOffset);
        int start = (int) (startOffset&sliceSizeModMask);
        int end = (int) (start+(endOffset-startOffset));

        int pos = start;
        while(pos<end){
            buf = buf.duplicate();
            buf.position(pos);
            buf.put(CLEAR, 0, Math.min(CLEAR.length, end-pos));
            pos+=CLEAR.length;
        }
    }

    @Override
    public boolean isSliced(){
        return true;
    }

    @Override
    public int sliceSize() {
        return sliceSize;
    }

    /**
     * Hack to unmap MappedByteBuffer.
     * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
     * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
     * Any error is silently ignored (for example SUN API does not exist on Android).
     */
    protected static boolean unmap(MappedByteBuffer b){
        if(!unmapHackSupported) {
            return false;
        }

        if(!(b instanceof DirectBuffer))
            return false;

        // need to dispose old direct buffer, see bug
        // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
        DirectBuffer bb = (DirectBuffer) b;
        Cleaner c = bb.cleaner();
        if(c!=null){
            c.clean();
            return true;
        }
        Object attachment = bb.attachment();
        return attachment!=null &&
                attachment instanceof DirectBuffer &&
                attachment!=b &&
                unmap((MappedByteBuffer) attachment);

    }

    private static boolean unmapHackSupported = true;
    static{
        try{
            //TODO use better way to recognize class?
            unmapHackSupported = Thread.currentThread().getContextClassLoader().loadClass("sun.nio.ch.DirectBuffer")!=null;
        }catch(Exception e){
            LOG.warning("mmap file unmap hack not supported, mmap files will not be closed, sun.nio.ch.DirectBuffer not found");
            unmapHackSupported = false;
        }
    }

    // Workaround for https://github.com/jankotek/MapDB/issues/326
    // File locking after .close() on Windows.
    static boolean windowsWorkaround = System.getProperty("os.name").toLowerCase().startsWith("win");


}
