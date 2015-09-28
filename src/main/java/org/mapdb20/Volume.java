/*
 *  Copyright (c) 2012 Jan Kotek
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

package org.mapdb20;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Long.rotateLeft;
import static org.mapdb20.DataIO.PRIME64_1;
import static org.mapdb20.DataIO.PRIME64_2;
import static org.mapdb20.DataIO.PRIME64_3;
import static org.mapdb20.DataIO.PRIME64_4;
import static org.mapdb20.DataIO.PRIME64_5;


/**
 * <p>
 * MapDB abstraction over raw storage (file, disk partition, memory etc...).
 * </p><p>
 *
 * Implementations needs to be thread safe (especially
 * 'ensureAvailable') operation.
 * However updates do not have to be atomic, it is clients responsibility
 * to ensure two threads are not writing/reading into the same location.
 * </p>
 *
 * @author Jan Kotek
 */
public abstract class Volume implements Closeable{

    static int sliceShiftFromSize(long sizeIncrement) {
        //TODO optimize this method with bitcount operation
        sizeIncrement = DataIO.nextPowTwo(sizeIncrement);
        for(int i=0;i<32;i++){
            if((1L<<i)==sizeIncrement){
                return i;
            }
        }
        throw new AssertionError("Could not find sliceShift");
    }

    static boolean isEmptyFile(String fileName) {
        if(fileName==null || fileName.length()==0)
            return true;
        File f = new File(fileName);
        return !f.exists() || f.length()==0;
    }

    /**
     * <p>
     * If underlying storage is memory-mapped-file, this method will try to
     * load and precache all file data into disk cache.
     * Most likely it will call {@link MappedByteBuffer#load()},
     * but could also read content of entire file etc
     * This method will not pin data into memory, they might be removed at any time.
     * </p>
     *
     * @return true if this method did something, false if underlying storage does not support loading
     */
    public  boolean fileLoad(){
        return false;
    }

    /**
     * Check that all bytes between given offsets are zero. This might cross 1MB boundaries
     * @param startOffset
     * @param endOffset
     *
     * @throws org.mapdb20.DBException.DataCorruption if some byte is not zero
     */
    public void assertZeroes(long startOffset, long endOffset) throws DBException.DataCorruption{
        for(long offset=startOffset;offset<endOffset;offset++){
            if(getUnsignedByte(offset)!=0)
                throw new DBException.DataCorruption("Not zero at offset: "+offset );
        }
    }


    public static abstract class VolumeFactory{
        public abstract Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled,
                                          int sliceShift, long initSize, boolean fixedSize);

        public Volume makeVolume(String file, boolean readOnly){
            return makeVolume(file,readOnly,false);
        }


        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable){
            return makeVolume(file,readOnly,fileLockDisable,CC.VOLUME_PAGE_SHIFT, 0, false);
        }
    }

    private static final byte[] CLEAR = new byte[1024];

    protected static final Logger LOG = Logger.getLogger(Volume.class.getName());

    /**
     * If {@code sun.misc.Unsafe} is available it will use Volume based on Unsafe.
     * If Unsafe is not available for some reason (Android), use DirectByteBuffer instead.
     */
    public static final VolumeFactory UNSAFE_VOL_FACTORY = new VolumeFactory() {

        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            String packageName = Volume.class.getPackage().getName();
            Class clazz;
            try {
                clazz = Class.forName(packageName+".UnsafeStuff$UnsafeVolume");
            } catch (ClassNotFoundException e) {
                clazz = null;
            }

            if(clazz!=null){
                try {
                    return (Volume) clazz.getConstructor(long.class, int.class, long.class)
                            .newInstance(0L, sliceShift, initSize);
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Could not invoke UnsafeVolume constructor. " +
                            "Falling back to DirectByteBuffer",e);

                }
            }

            return MemoryVol.FACTORY.makeVolume(file, readOnly, fileLockDisabled, sliceShift, initSize, fixedSize);
        }
    };

    protected volatile boolean closed;

    public boolean isClosed(){
        return closed;
    }

    //uncomment to get stack trace on Volume leak warning
//    final private Throwable constructorStackTrace = new AssertionError();

    @Override protected void finalize(){
        if(CC.ASSERT){
            if(!closed
                    && !(this instanceof ByteArrayVol)
                    && !(this instanceof SingleByteArrayVol)){
                LOG.log(Level.WARNING, "Open Volume was GCed, possible file handle leak."
//                        ,constructorStackTrace
                );
            }
        }
    }

    /**
     * Check space allocated by Volume is bigger or equal to given offset.
     * So it is safe to write into smaller offsets.
     *
     * @param offset
     */
    abstract public void ensureAvailable(final long offset);


    public abstract void truncate(long size);


    abstract public void putLong(final long offset, final long value);
    abstract public void putInt(long offset, int value);
    abstract public void putByte(final long offset, final byte value);

    abstract public void putData(final long offset, final byte[] src, int srcPos, int srcSize);
    abstract public void putData(final long offset, final ByteBuffer buf);

    public void putDataOverlap(final long offset, final byte[] src, int srcPos, int srcSize){
        putData(offset,src,srcPos,srcSize);
    }


    abstract public long getLong(final long offset);
    abstract public int getInt(long offset);
    abstract public byte getByte(final long offset);



    abstract public DataInput getDataInput(final long offset, final int size);
    public DataInput getDataInputOverlap(final long offset, final int size){
        return getDataInput(offset,size);
    }

    abstract public void getData(long offset, byte[] bytes, int bytesPos, int size);

    abstract public void close();

    abstract public void sync();

    /**
     *
     * @return slice size or {@code -1} if not sliced
     */
    abstract public int sliceSize();

    public void deleteFile(){
        File f = getFile();
        if(f!=null && f.isFile() && !f.delete()){
            LOG.warning("Could not delete file: "+f);
        }
    }

    public abstract boolean isSliced();

    public abstract long length();

    public void putUnsignedShort(final long offset, final int value){
        putByte(offset, (byte) (value>>8));
        putByte(offset+1, (byte) (value));
    }

    public int getUnsignedShort(long offset) {
        return (( (getByte(offset) & 0xff) << 8) |
                ( (getByte(offset+1) & 0xff)));
    }

    public int getUnsignedByte(long offset) {
        return getByte(offset) & 0xff;
    }

    public void putUnsignedByte(long offset, int b) {
        putByte(offset, (byte) (b & 0xff));
    }


    public int putLongPackBidi(long offset, long value) {
        putUnsignedByte(offset++, (((int) value & 0x7F)) | 0x80);
        value >>>= 7;
        int counter = 2;

        //$DELAY$
        while ((value & ~0x7FL) != 0) {
            putUnsignedByte(offset++, (((int) value & 0x7F)));
            value >>>= 7;
            //$DELAY$
            counter++;
        }
        //$DELAY$
        putUnsignedByte(offset, (byte) value | 0x80);
        return counter;
    }

    public long getLongPackBidi(long offset){
        //$DELAY$
        long b = getUnsignedByte(offset++); //TODO this could be inside loop, change all implementations
        if(CC.ASSERT && (b&0x80)==0)
            throw new DBException.DataCorruption();
        long result = (b & 0x7F) ;
        int shift = 7;
        do {
            //$DELAY$
            b = getUnsignedByte(offset++);
            result |= (b & 0x7F) << shift;
            if(CC.ASSERT && shift>64)
                throw new DBException.DataCorruption();
            shift += 7;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)(shift/7))<<60) | result;
    }

    public long getLongPackBidiReverse(long offset){
        //$DELAY$
        long b = getUnsignedByte(--offset);
        if(CC.ASSERT && (b&0x80)==0)
            throw new DBException.DataCorruption();
        long result = (b & 0x7F) ;
        int counter = 1;
        do {
            //$DELAY$
            b = getUnsignedByte(--offset);
            result = (b & 0x7F) | (result<<7);
            if(CC.ASSERT && counter>8)
                throw new DBException.DataCorruption();
            counter++;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)counter)<<60) | result;
    }

    public long getSixLong(long pos) {
        return
                ((long) (getByte(pos++) & 0xff) << 40) |
                        ((long) (getByte(pos++) & 0xff) << 32) |
                        ((long) (getByte(pos++) & 0xff) << 24) |
                        ((long) (getByte(pos++) & 0xff) << 16) |
                        ((long) (getByte(pos++) & 0xff) << 8) |
                        ((long) (getByte(pos) & 0xff));
    }

    public void putSixLong(long pos, long value) {
        if(CC.ASSERT && (value>>>48!=0))
            throw new DBException.DataCorruption();

        putByte(pos++, (byte) (0xff & (value >> 40)));
        putByte(pos++, (byte) (0xff & (value >> 32)));
        putByte(pos++, (byte) (0xff & (value >> 24)));
        putByte(pos++, (byte) (0xff & (value >> 16)));
        putByte(pos++, (byte) (0xff & (value >> 8)));
        putByte(pos, (byte) (0xff & (value)));
    }


    /**
     * Put packed long at given position.
     *
     * @param value to be written
     * @return number of bytes consumed by packed value
     */
    public int putPackedLong(long pos, long value){
        //$DELAY$
        int ret = 0;
        int shift = 63-Long.numberOfLeadingZeros(value);
        shift -= shift%7; // round down to nearest multiple of 7
        while(shift!=0){
            putByte(pos + (ret++), (byte) (((value >>> shift) & 0x7F) | 0x80));
            //$DELAY$
            shift-=7;
        }
        putByte(pos+(ret++),(byte) (value & 0x7F));
        return ret;
    }



    /**
     * Unpack long value from the Volume. Highest 4 bits reused to indicate number of bytes read from Volume.
     * One can use {@code result & DataIO.PACK_LONG_RESULT_MASK} to remove size;
     *
     * @param position to read value from
     * @return The long value, minus highest byte
     */
    public long getPackedLong(long position){
        long ret = 0;
        long pos2 = 0;
        byte v;
        do{
            v = getByte(position+(pos2++));
            ret = (ret<<7 ) | (v & 0x7F);
        }while(v<0);

        return (pos2<<60) | ret;
    }


    /** returns underlying file if it exists */
    abstract public File getFile();

    /** return true if this Volume holds exclusive lock over its file */
    abstract public boolean getFileLocked();

    /**
     * Transfers data from this Volume into target volume.
     * If its possible, the implementation should override this method to enable direct memory transfer.
     *
     * Caller must respect slice boundaries. ie it is not possible to transfer data which cross slice boundaries.
     *
     * @param inputOffset offset inside this Volume, ie data will be read from this offset
     * @param target Volume to copy data into
     * @param targetOffset position in target volume where data will be copied into
     * @param size size of data to copy
     */
    public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
        //TODO size>Integer.MAX_VALUE

        byte[] data = new byte[(int) size];
        try {
            getDataInput(inputOffset, (int) size).readFully(data);
        }catch(IOException e){
            throw new DBException.VolumeIOError(e);
        }
        target.putData(targetOffset,data,0, (int) size);
    }


    /**
     * Set all bytes between {@code startOffset} and {@code endOffset} to zero.
     * Area between offsets must be ready for write once clear finishes.
     */
    public abstract void clear(long startOffset, long endOffset);



    /**
     * Copy content of this volume to another.
     * Target volume might grow, but is never shrank.
     * Target is also not synced
     */
    public void copyEntireVolumeTo(Volume to) {
        final long volSize = length();
        final long bufSize = 1L<<CC.VOLUME_PAGE_SHIFT;

        to.ensureAvailable(volSize);

        for(long offset=0;offset<volSize;offset+=bufSize){
            long size = Math.min(volSize,offset+bufSize)-offset;
            if(CC.ASSERT && (size<0))
                throw new AssertionError();
            transferInto(offset,to,offset, size);
        }

    }

    /**
     * <p>
     * Calculates XXHash64 from this Volume content.
     * </p><p>
     * This code comes from <a href="https://github.com/jpountz/lz4-java">LZ4-Java</a> created
     * by Adrien Grand.
     * </p>
     *
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public long hash(long off, long len, long seed){
        if (len < 0) {
            throw new IllegalArgumentException("lengths must be >= 0");
        }
        long bufLen = length();
        if(off<0 || off>=bufLen || off+len<0 || off+len>bufLen){
            throw new IndexOutOfBoundsException();
        }

        final long end = off + len;
        long h64;

        if (len >= 32) {
            final long limit = end - 32;
            long v1 = seed + PRIME64_1 + PRIME64_2;
            long v2 = seed + PRIME64_2;
            long v3 = seed + 0;
            long v4 = seed - PRIME64_1;
            do {
                v1 += Long.reverseBytes(getLong(off)) * PRIME64_2;
                v1 = rotateLeft(v1, 31);
                v1 *= PRIME64_1;
                off += 8;

                v2 += Long.reverseBytes(getLong(off)) * PRIME64_2;
                v2 = rotateLeft(v2, 31);
                v2 *= PRIME64_1;
                off += 8;

                v3 += Long.reverseBytes(getLong(off)) * PRIME64_2;
                v3 = rotateLeft(v3, 31);
                v3 *= PRIME64_1;
                off += 8;

                v4 += Long.reverseBytes(getLong(off)) * PRIME64_2;
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
            long k1 = Long.reverseBytes(getLong(off));
            k1 *= PRIME64_2; k1 = rotateLeft(k1, 31); k1 *= PRIME64_1; h64 ^= k1;
            h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
            off += 8;
        }

        if (off <= end - 4) {
            h64 ^= (Integer.reverseBytes(getInt(off)) & 0xFFFFFFFFL) * PRIME64_1;
            h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
            off += 4;
        }

        while (off < end) {
            h64 ^= (getByte(off) & 0xFF) * PRIME64_5;
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

    /**
     * Abstract Volume over bunch of ByteBuffers
     * It leaves ByteBufferVol details (allocation, disposal) on subclasses.
     * Most methods are final for better performance (JIT compiler can inline those).
     */
    abstract static public class ByteBufferVol extends Volume{

        protected final  boolean cleanerHackEnabled;

        protected final ReentrantLock growLock = new ReentrantLock(CC.FAIR_LOCKS);
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
                throw new DBException.VolumeEOF("Get/Set beyong file size. Requested offset: "+offset+", volume size: "+length());
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
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
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
        public final DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            return new DataIO.DataInputByteBuffer(getSlice(offset), (int) (offset& sliceSizeModMask));
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
        public DataInput getDataInputOverlap(long offset, int size) {
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
                return new DataIO.DataInputByteArray(bb);
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
        public int putLongPackBidi(long offset, long value) {
            final ByteBuffer b = getSlice(offset);
            int bpos = (int) (offset & sliceSizeModMask);

            b.put(bpos++, toByte((value & 0x7F) | 0x80));
            value >>>= 7;
            int counter = 2;

            //$DELAY$
            while ((value & ~0x7FL) != 0) {
                b.put(bpos++, toByte(value & 0x7F));
                value >>>= 7;
                //$DELAY$
                counter++;
            }
            //$DELAY$
            b.put(bpos, toByte(value | 0x80));
            return counter;
        }

        @Override
        public long getLongPackBidi(long offset) {
            final ByteBuffer bb = getSlice(offset);
            int bpos = (int) (offset & sliceSizeModMask);

            //$DELAY$
            long b = bb.get(bpos++) & 0xffL; //TODO this could be inside loop, change all implementations
            if(CC.ASSERT && (b&0x80)==0)
                throw new DBException.DataCorruption();
            long result = (b & 0x7F) ;
            int shift = 7;
            do {
                //$DELAY$
                b = bb.get(bpos++) & 0xffL;
                result |= (b & 0x7F) << shift;
                if(CC.ASSERT && shift>64)
                    throw new DBException.DataCorruption();
                shift += 7;
            }while((b & 0x80) == 0);
            //$DELAY$
            return (((long)(shift/7))<<60) | result;
        }

        @Override
        public long getLongPackBidiReverse(long offset) {
            final ByteBuffer bb = getSlice(offset);
            int bpos = (int) (offset & sliceSizeModMask);

            //$DELAY$
            long b = bb.get(--bpos) & 0xffL;
            if(CC.ASSERT && (b&0x80)==0)
                throw new DBException.DataCorruption();
            long result = (b & 0x7F) ;
            int counter = 1;
            do {
                //$DELAY$
                b = bb.get(--bpos) & 0xffL;
                result = (b & 0x7F) | (result<<7);
                if(CC.ASSERT && counter>8)
                    throw new DBException.DataCorruption();
                counter++;
            }while((b & 0x80) == 0);
            //$DELAY$
            return (((long)counter)<<60) | result;
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
                throw new DBException.DataCorruption();

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
                b.put(bpos + (ret++), (byte) (((value >>> shift) & 0x7F) | 0x80));
                //$DELAY$
                shift-=7;
            }
            b.put(bpos +(ret++),(byte) (value & 0x7F));
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
            }while(v<0);

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
            try{
                if(unmapHackSupported){

                    // need to dispose old direct buffer, see bug
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
                    Method cleanerMethod = b.getClass().getMethod("cleaner", new Class[0]);
                    cleanerMethod.setAccessible(true);
                    if(cleanerMethod!=null){
                        Object cleaner = cleanerMethod.invoke(b);
                        if(cleaner!=null){
                            Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                            if(clearMethod!=null) {
                                clearMethod.invoke(cleaner);
                                return true;
                            }
                        }else{
                            //cleaner is null, try fallback method for readonly buffers
                            Method attMethod = b.getClass().getMethod("attachment", new Class[0]);
                            attMethod.setAccessible(true);
                            Object att = attMethod.invoke(b);
                            return att instanceof MappedByteBuffer &&
                                    unmap((MappedByteBuffer) att);
                        }
                    }
                }
            }catch(Exception e){
                unmapHackSupported = false;
                LOG.log(Level.WARNING, "Unmap failed", e);
            }
            return false;
        }

        private static boolean unmapHackSupported = true;
        static{
            try{
                unmapHackSupported =
                        SerializerPojo.DEFAULT_CLASS_LOADER.run("sun.nio.ch.DirectBuffer")!=null;
            }catch(Exception e){
                unmapHackSupported = false;
            }
        }

        // Workaround for https://github.com/jankotek/MapDB/issues/326
        // File locking after .close() on Windows.
        private static boolean windowsWorkaround = System.getProperty("os.name").toLowerCase().startsWith("win");


    }

    /**
     * Abstract Volume over single ByteBuffer, maximal size is 2GB (32bit limit).
     * It leaves ByteBufferVol details (allocation, disposal) on subclasses.
     * Most methods are final for better performance (JIT compiler can inline those).
     */
    abstract static public class ByteBufferVolSingle extends Volume{

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
        public final DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            return new DataIO.DataInputByteBuffer(buffer, (int) (offset));
        }



        @Override
        public void putDataOverlap(long offset, byte[] data, int pos, int len) {
            putData(offset,data,pos,len);
        }

        @Override
        public DataInput getDataInputOverlap(long offset, int size) {
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


    public static final class MappedFileVol extends ByteBufferVol {

        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                return factory(file, readOnly, fileLockDisabled, sliceShift, false, initSize);
            }
        };


        public static final VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                return factory(file, readOnly, fileLockDisabled, sliceShift, true,initSize);
            }
        };


        private static Volume factory(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift,
                                      boolean cleanerHackEnabled, long initSize) {
            File f = new File(file);
            if(readOnly){
                long flen = f.length();
                if(flen <= Integer.MAX_VALUE) {
                    return new MappedFileVolSingle(f, readOnly, fileLockDisabled,
                            Math.max(flen,initSize),
                            cleanerHackEnabled);
                }
            }
            //TODO prealocate initsize
            return new MappedFileVol(f,readOnly,fileLockDisabled, sliceShift,cleanerHackEnabled,initSize);
        }


        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;
        protected final java.io.RandomAccessFile raf;
        protected final FileLock fileLock;

        public MappedFileVol(File file, boolean readOnly, boolean fileLockDisable,
                             int sliceShift, boolean cleanerHackEnabled, long initSize) {
            super(readOnly,sliceShift, cleanerHackEnabled);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                FileChannelVol.checkFolder(file, readOnly);
                this.raf = new java.io.RandomAccessFile(file, readOnly?"r":"rw");
                this.fileChannel = raf.getChannel();

                fileLock = Volume.lockFile(file,raf,readOnly,fileLockDisable);

                final long fileSize = fileChannel.size();
                long endSize = fileSize;
                if(initSize>fileSize && !readOnly)
                    endSize = initSize; //allocate more data

                if(endSize>0){
                    //map data
                    int chunksSize = (int) ((Fun.roundUp(endSize,sliceSize)>>> sliceShift));
                    if(endSize>fileSize && !readOnly){
                        RandomAccessFileVol.clearRAF(raf,fileSize, endSize);
                        raf.getFD().sync();
                    }

                    slices = new ByteBuffer[chunksSize];
                    for(int i=0;i<slices.length;i++){
                        ByteBuffer b = fileChannel.map(mapMode,1L*sliceSize*i, sliceSize);
                        if(CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                            throw new AssertionError("Little-endian");
                        slices[i] = b;
                    }
                }else{
                    slices = new ByteBuffer[0];
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public final void ensureAvailable(long offset) {
            offset=Fun.roundUp(offset,1L<<sliceShift);
            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos <= slices.length)
                    return;

                int oldSize = slices.length;

                // fill with zeroes from  old size to new size
                // this will prevent file from growing via mmap operation
                RandomAccessFileVol.clearRAF(raf, 1L*oldSize*sliceSize, offset);
                raf.getFD().sync();

                //grow slices
                ByteBuffer[] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, slicePos);

                for(int pos=oldSize;pos<slices2.length;pos++) {
                    ByteBuffer b = fileChannel.map(mapMode,1L*sliceSize*pos, sliceSize);
                    if(CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                        throw new AssertionError("Little-endian");
                    slices2[pos] = b;
                }

                slices = slices2;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }finally{
                growLock.unlock();
            }
        }


        @Override
        public void close() {
            growLock.lock();
            try{
                if(closed)
                    return;

                closed = true;
                if(fileLock!=null && fileLock.isValid()){
                    fileLock.release();
                }
                fileChannel.close();
                raf.close();
                //TODO not sure if no sync causes problems while unlocking files
                //however if it is here, it causes slow commits, sync is called on write-ahead-log just before it is deleted and closed
//                if(!readOnly)
//                    sync();

                if(cleanerHackEnabled) {
                    for (ByteBuffer b : slices) {
                        if (b != null && (b instanceof MappedByteBuffer)) {
                            unmap((MappedByteBuffer) b);
                        }
                    }
                }
                Arrays.fill(slices,null);
                slices = null;

            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }finally{
                growLock.unlock();
            }

        }

        @Override
        public void sync() {
            if(readOnly)
                return;
            growLock.lock();
            try{
                ByteBuffer[] slices = this.slices;
                if(slices==null)
                    return;

                // Iterate in reverse order.
                // In some cases if JVM crashes during iteration,
                // first part of the file would be synchronized,
                // while part of file would be missing.
                // It is better if end of file is synchronized first, since it has less sensitive data,
                // and it increases chance to detect file corruption.
                for(int i=slices.length-1;i>=0;i--){
                    ByteBuffer b = slices[i];
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        MappedByteBuffer bb = ((MappedByteBuffer) b);
                        bb.force();
                    }
                }
            }finally{
                growLock.unlock();
            }

        }




        @Override
        public long length() {
            return file.length();
        }

        @Override
        public File getFile() {
            return file;
        }


        @Override
        public boolean getFileLocked() {
            return fileLock!=null && fileLock.isValid();
        }

        @Override
        public void truncate(long size) {
            final int maxSize = 1+(int) (size >>> sliceShift);
            if(maxSize== slices.length)
                return;
            if(maxSize> slices.length) {
                ensureAvailable(size);
                return;
            }
            growLock.lock();
            try{
                if(maxSize>= slices.length)
                    return;
                ByteBuffer[] old = slices;
                slices = Arrays.copyOf(slices,maxSize);

                //unmap remaining buffers
                for(int i=maxSize;i<old.length;i++){
                    if(cleanerHackEnabled) {
                        unmap((MappedByteBuffer) old[i]);
                    }
                    old[i] = null;
                }

                if (ByteBufferVol.windowsWorkaround) {
                    for(int i=0;i<maxSize;i++){
                        if(cleanerHackEnabled) {
                            unmap((MappedByteBuffer) old[i]);
                        }
                        old[i] = null;
                    }
                }

                try {
                    fileChannel.truncate(1L * sliceSize *maxSize);
                } catch (IOException e) {
                    throw new DBException.VolumeIOError(e);
                }

                if (ByteBufferVol.windowsWorkaround) {
                    for(int pos=0;pos<maxSize;pos++) {
                        ByteBuffer b = fileChannel.map(mapMode,1L*sliceSize*pos, sliceSize);
                        if(CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                            throw new AssertionError("Little-endian");
                        slices[pos] = b;
                    }
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }finally {
                growLock.unlock();
            }
        }

        @Override
        public boolean fileLoad() {
            ByteBuffer[] slices = this.slices;
            for(ByteBuffer b:slices){
                if(b instanceof MappedByteBuffer){
                    ((MappedByteBuffer)b).load();
                }
            }
            return true;
        }
    }



    public static final class MappedFileVolSingle extends ByteBufferVolSingle {


        protected final static VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                if(initSize>Integer.MAX_VALUE)
                    throw new IllegalArgumentException("startSize larger 2GB");
                return new MappedFileVolSingle(
                        new File(file),
                        readOnly,
                        fileLockDisabled,
                        initSize,
                        false);
            }
        };

        protected final static VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                if(initSize>Integer.MAX_VALUE)
                    throw new IllegalArgumentException("startSize larger 2GB");
                return new MappedFileVolSingle(
                        new File(file),
                        readOnly,
                        fileLockDisabled,
                        initSize,
                        true);
            }
        };


        protected final File file;
        protected final FileChannel.MapMode mapMode;
        protected final RandomAccessFile raf;
        protected final FileLock fileLock;

        public MappedFileVolSingle(File file, boolean readOnly, boolean fileLockDisabled, long maxSize,
                                   boolean cleanerHackEnabled) {
            super(readOnly,maxSize, cleanerHackEnabled);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                FileChannelVol.checkFolder(file,readOnly);
                raf = new java.io.RandomAccessFile(file, readOnly?"r":"rw");

                fileLock = Volume.lockFile(file, raf, readOnly, fileLockDisabled);


                final long fileSize = raf.length();
                if(readOnly) {
                    maxSize = Math.min(maxSize, fileSize);
                }else if(fileSize<maxSize){
                    //zero out data between fileSize and maxSize, so mmap file operation does not expand file
                    raf.seek(fileSize);
                    long offset = fileSize;
                    do{
                        raf.write(CLEAR,0, (int) Math.min(CLEAR.length, maxSize-offset));
                        offset+=CLEAR.length;
                    }while(offset<maxSize);
                }
                buffer = raf.getChannel().map(mapMode, 0, maxSize);

                if(readOnly)
                    buffer = buffer.asReadOnlyBuffer();
                //TODO assert endianess
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        synchronized public void close() {
            if(closed) {
                return;
            }
            closed = true;
            //TODO not sure if no sync causes problems while unlocking files
            //however if it is here, it causes slow commits, sync is called on write-ahead-log just before it is deleted and closed
//                if(!readOnly)
//                    sync();

            try {
                if(fileLock!=null && fileLock.isValid()) {
                    fileLock.release();
                }
                raf.close();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

            if (cleanerHackEnabled && buffer != null && (buffer instanceof MappedByteBuffer)) {
                ByteBufferVol.unmap((MappedByteBuffer) buffer);
            }
            buffer = null;
        }

        @Override
        synchronized public void sync() {
            if(readOnly)
                return;
            if(buffer instanceof MappedByteBuffer)
                ((MappedByteBuffer)buffer).force();
        }


        @Override
        public long length() {
            return file.length();
        }

        @Override
        public File getFile() {
            return file;
        }

        @Override
        public boolean getFileLocked() {
            return fileLock!=null && fileLock.isValid();
        }

        @Override
        public void truncate(long size) {
            //TODO truncate
        }

        @Override
        public boolean fileLoad() {
            ((MappedByteBuffer)buffer).load();
            return true;
        }
    }


    public static final class MemoryVol extends ByteBufferVol {

        /** factory for DirectByteBuffer storage*/
        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                //TODO optimize for fixedSize smaller than 2GB
                return new MemoryVol(true,sliceShift,false, initSize);
            }
        };


        /** factory for DirectByteBuffer storage*/
        public static final VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {//TODO prealocate initSize
                //TODO optimize for fixedSize smaller than 2GB
                return new MemoryVol(true,sliceShift,true, initSize);
            }
        };

        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString()+",direct="+useDirectBuffer;
        }

        public MemoryVol(final boolean useDirectBuffer, final int sliceShift,boolean cleanerHackEnabled, long initSize) {
            super(false, sliceShift, cleanerHackEnabled);
            this.useDirectBuffer = useDirectBuffer;
            if(initSize!=0)
                ensureAvailable(initSize);
        }


        @Override
        public final void ensureAvailable(long offset) {
            offset=Fun.roundUp(offset,1L<<sliceShift);
            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos <= slices.length)
                    return;

                int oldSize = slices.length;
                ByteBuffer[] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, slicePos);

                for(int pos=oldSize;pos<slices2.length;pos++) {
                    ByteBuffer b =  useDirectBuffer ?
                            ByteBuffer.allocateDirect(sliceSize) :
                            ByteBuffer.allocate(sliceSize);
                    if(CC.ASSERT && b.order()!= ByteOrder.BIG_ENDIAN)
                        throw new AssertionError("little-endian");
                    slices2[pos]=b;
                }

                slices = slices2;
            }catch(OutOfMemoryError e){
                throw new DBException.OutOfMemory(e);
            }finally{
                growLock.unlock();
            }
        }


        @Override
        public void truncate(long size) {
            final int maxSize = 1+(int) (size >>> sliceShift);
            if(maxSize== slices.length)
                return;
            if(maxSize> slices.length) {
                ensureAvailable(size);
                return;
            }
            growLock.lock();
            try{
                if(maxSize>= slices.length)
                    return;
                ByteBuffer[] old = slices;
                slices = Arrays.copyOf(slices,maxSize);

                //unmap remaining buffers
                for(int i=maxSize;i<old.length;i++){
                    if(cleanerHackEnabled && old[i] instanceof  MappedByteBuffer)
                        unmap((MappedByteBuffer) old[i]);
                    old[i] = null;
                }

            }finally {
                growLock.unlock();
            }
        }

        @Override public void close() {
            growLock.lock();
            try{
                closed = true;
                if(cleanerHackEnabled) {
                    for (ByteBuffer b : slices) {
                        if (b != null && (b instanceof MappedByteBuffer)) {
                            unmap((MappedByteBuffer) b);
                        }
                    }
                }
                Arrays.fill(slices,null);
                slices = null;
            }finally{
                growLock.unlock();
            }
        }

        @Override public void sync() {}

        @Override
        public long length() {
            return ((long)slices.length)*sliceSize;
        }

        @Override
        public File getFile() {
            return null;
        }

        @Override
        public boolean getFileLocked() {
            return false;
        }
    }


    public static final class MemoryVolSingle extends ByteBufferVolSingle {

        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString() + ",direct=" + useDirectBuffer;
        }

        public MemoryVolSingle(final boolean useDirectBuffer, final long maxSize, boolean cleanerHackEnabled) {
            super(false, maxSize, cleanerHackEnabled);
            this.useDirectBuffer = useDirectBuffer;
            this.buffer = useDirectBuffer?
                    ByteBuffer.allocateDirect((int) maxSize):
                    ByteBuffer.allocate((int) maxSize);
        }

        @Override
        public void truncate(long size) {
            //TODO truncate
        }

        @Override
        synchronized public void close() {
            if(closed)
                return;

            if(cleanerHackEnabled && buffer instanceof MappedByteBuffer){
                ByteBufferVol.unmap((MappedByteBuffer) buffer);
            }
            buffer = null;
            closed = true;
        }

        @Override
        public void sync() {
        }

        @Override
        public long length() {
            return maxSize;
        }

        @Override
        public File getFile() {
            return null;
        }

        @Override
        public boolean getFileLocked() {
            return false;
        }
    }


    /**
     * Volume which uses FileChannel.
     * Uses global lock and does not use mapped memory.
     */
    public static final class FileChannelVol extends Volume {

        public static final VolumeFactory FACTORY = new VolumeFactory() {

            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                return new FileChannelVol(new File(file),readOnly, fileLockDisabled, sliceShift,initSize);
            }
        };

        protected final File file;
        protected final int sliceSize;
        protected RandomAccessFile raf;
        protected FileChannel channel;
        protected final boolean readOnly;
        protected final FileLock fileLock;

        protected volatile long size;
        protected final Lock growLock = new ReentrantLock(CC.FAIR_LOCKS);

        public FileChannelVol(File file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize){
            this.file = file;
            this.readOnly = readOnly;
            this.sliceSize = 1<<sliceShift;
            try {
                checkFolder(file, readOnly);
                if (readOnly && !file.exists()) {
                    raf = null;
                    channel = null;
                    size = 0;
                } else {
                    raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
                    channel = raf.getChannel();
                    size = channel.size();
                }

                fileLock = Volume.lockFile(file,raf,readOnly,fileLockDisabled);

                if(initSize!=0 && !readOnly){
                    long oldSize = channel.size();
                    if(initSize>oldSize){
                        raf.setLength(initSize);
                        clear(oldSize,initSize);
                    }
                }


            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        public FileChannelVol(File file) {
            this(file, false, false, CC.VOLUME_PAGE_SHIFT,0L);
        }

        protected static void checkFolder(File file, boolean readOnly) throws IOException {
            File parent = file.getParentFile();
            if(parent == null) {
                parent = file.getCanonicalFile().getParentFile();
            }
            if (parent == null) {
                throw new IOException("Parent folder could not be determined for: "+file);
            }
            if(!parent.exists() || !parent.isDirectory())
                throw new IOException("Parent folder does not exist: "+file);
            if(!parent.canRead())
                throw new IOException("Parent folder is not readable: "+file);
            if(!readOnly && !parent.canWrite())
                throw new IOException("Parent folder is not writable: "+file);
        }

        @Override
        public void ensureAvailable(long offset) {
            offset=Fun.roundUp(offset,sliceSize);

            if(offset>size){
                growLock.lock();
                try {
                    raf.setLength(offset);
                    size = offset;
                } catch (IOException e) {
                    throw new DBException.VolumeIOError(e);
                }finally {
                    growLock.unlock();
                }
            }
        }

        @Override
        public void truncate(long size) {
            growLock.lock();
            try {
                this.size = size;
                channel.truncate(size);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }finally{
                growLock.unlock();
            }
        }

        protected void writeFully(long offset, ByteBuffer buf){
            int remaining = buf.limit()-buf.position();
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+remaining){
                new IOException("VOL STACK:").printStackTrace();
            }
            try {
                while(remaining>0){
                    int write = channel.write(buf, offset);
                    if(write<0) throw new EOFException();
                    remaining-=write;
                }
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public void putLong(long offset, long value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
                new IOException("VOL STACK:").printStackTrace();
            }


            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(0, value);
            writeFully(offset, buf);
        }

        @Override
        public void putInt(long offset, int value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
                new IOException("VOL STACK:").printStackTrace();
            }

            ByteBuffer buf = ByteBuffer.allocate(4);
            buf.putInt(0, value);
            writeFully(offset, buf);
        }

        @Override
        public void putByte(long offset, byte value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+1){
                new IOException("VOL STACK:").printStackTrace();
            }


            ByteBuffer buf = ByteBuffer.allocate(1);
            buf.put(0, value);
            writeFully(offset, buf);
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            ByteBuffer buf = ByteBuffer.wrap(src,srcPos, srcSize);
            writeFully(offset, buf);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            writeFully(offset,buf);
        }

        protected void readFully(long offset, ByteBuffer buf){
            int remaining = buf.limit()-buf.position();
            try{
                while(remaining>0){
                    int read = channel.read(buf, offset);
                    if(read<0)
                        throw new EOFException();
                    remaining-=read;
                }
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public long getLong(long offset) {
            ByteBuffer buf = ByteBuffer.allocate(8);
            readFully(offset, buf);
            return buf.getLong(0);
        }

        @Override
        public int getInt(long offset) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            readFully(offset,buf);
            return buf.getInt(0);
        }

        @Override
        public byte getByte(long offset) {
            ByteBuffer buf = ByteBuffer.allocate(1);
            readFully(offset,buf);
            return buf.get(0);
        }

        @Override
        public DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            ByteBuffer buf = ByteBuffer.allocate(size);
            readFully(offset,buf);
            return new DataIO.DataInputByteBuffer(buf,0);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int size) {
            ByteBuffer buf = ByteBuffer.wrap(bytes,bytesPos,size);
            readFully(offset,buf);
        }

        @Override
        public synchronized void close() {
            try{
                if(closed) {
                    return;
                }
                closed = true;

                if(fileLock!=null && fileLock.isValid()){
                    fileLock.release();
                }

                if(channel!=null)
                    channel.close();
                channel = null;
                if (raf != null)
                    raf.close();
                raf = null;
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void sync() {
            try{
                channel.force(true);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
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

        @Override
        public long length() {
            try {
                return channel.size();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public File getFile() {
            return file;
        }

        @Override
        public boolean getFileLocked() {
            return fileLock!=null && fileLock.isValid();
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            try {
                while(startOffset<endOffset){
                    ByteBuffer b = ByteBuffer.wrap(CLEAR);
                    b.limit((int) Math.min(CLEAR.length, endOffset - startOffset));
                    channel.write(b, startOffset);
                    startOffset+=CLEAR.length;
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }
    }


    /** transfer data from one volume to second. Second volume will be expanded if needed*/
    public static void volumeTransfer(long size, Volume from, Volume to){
        int bufSize = Math.min(from.sliceSize(),to.sliceSize());

        if(bufSize<0 || bufSize>1024*1024*128){
            bufSize = 64 * 1024; //something strange, set safe limit
        }
        to.ensureAvailable(size);

        for(long offset=0;offset<size;offset+=bufSize){
            int bb = (int) Math.min(bufSize, size-offset);
            from.transferInto(offset,to,offset,bb);
        }
    }


    public static final class ByteArrayVol extends Volume{

        public static final VolumeFactory FACTORY = new VolumeFactory() {

            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
                //TODO optimize for fixedSize if bellow 2GB
                return new ByteArrayVol(sliceShift, initSize);
            }
        };

        protected final ReentrantLock growLock = new ReentrantLock(CC.FAIR_LOCKS);

        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected volatile byte[][] slices = new byte[0][];

        protected ByteArrayVol() {
            this(CC.VOLUME_PAGE_SHIFT, 0L);
        }

        protected ByteArrayVol(int sliceShift, long initSize) {
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;

            if(initSize!=0){
                ensureAvailable(initSize);
            }
        }

        protected final byte[] getSlice(long offset){
            byte[][] slices = this.slices;
            int pos = ((int) (offset >>> sliceShift));
            if(pos>=slices.length)
                throw new DBException.VolumeEOF();
            return slices[pos];
        }

        @Override
        public final void ensureAvailable(long offset) {
            offset=Fun.roundUp(offset,1L<<sliceShift);
            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return;
            }

            growLock.lock();
            try {
                //check second time
                if (slicePos <= slices.length)
                    return;

                int oldSize = slices.length;
                byte[][] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, slicePos);

                for (int pos = oldSize; pos < slices2.length; pos++) {
                    slices2[pos] = new byte[sliceSize];
                }


                slices = slices2;
            }catch(OutOfMemoryError e){
                throw new DBException.OutOfMemory(e);
            }finally{
                growLock.unlock();
            }
        }


        @Override
        public void truncate(long size) {
            final int maxSize = 1+(int) (size >>> sliceShift);
            if(maxSize== slices.length)
                return;
            if(maxSize> slices.length) {
                ensureAvailable(size);
                return;
            }
            growLock.lock();
            try{
                if(maxSize>= slices.length)
                    return;
                slices = Arrays.copyOf(slices,maxSize);
            }finally {
                growLock.unlock();
            }
        }

        @Override
        public void putLong(long offset, long v) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            DataIO.putLong(buf,pos,v);
        }


        @Override
        public void putInt(long offset, int value) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            buf[pos++] = (byte) (0xff & (value >> 24));
            buf[pos++] = (byte) (0xff & (value >> 16));
            buf[pos++] = (byte) (0xff & (value >> 8));
            buf[pos++] = (byte) (0xff & (value));
        }

        @Override
        public void putByte(long offset, byte value) {
            final byte[] b = getSlice(offset);
            b[((int) (offset & sliceSizeModMask))] = value;
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            System.arraycopy(src,srcPos,buf,pos,srcSize);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] dst = getSlice(offset);
            buf.get(dst, pos, buf.remaining());
        }


        @Override
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
            int pos = (int) (inputOffset & sliceSizeModMask);
            byte[] buf = getSlice(inputOffset);

            //TODO size>Integer.MAX_VALUE
            target.putData(targetOffset, buf, pos, (int) size);
        }



        @Override
        public void putDataOverlap(long offset, byte[] data, int pos, int len) {
            boolean overlap = (offset>>>sliceShift != (offset+len)>>>sliceShift);

            if(overlap){
                while(len>0){
                    byte[] b = getSlice(offset);
                    int pos2 = (int) (offset&sliceSizeModMask);

                    int toPut = Math.min(len,sliceSize - pos2);

                    System.arraycopy(data, pos, b, pos2, toPut);

                    pos+=toPut;
                    len -=toPut;
                    offset+=toPut;
                }
            }else{
                putData(offset,data,pos,len);
            }
        }

        @Override
        public DataInput getDataInputOverlap(long offset, int size) {
            boolean overlap = (offset>>>sliceShift != (offset+size)>>>sliceShift);
            if(overlap){
                byte[] bb = new byte[size];
                final int origLen = size;
                while(size>0){
                    byte[] b = getSlice(offset);
                    int pos = (int) (offset&sliceSizeModMask);

                    int toPut = Math.min(size,sliceSize - pos);

                    System.arraycopy(b,pos, bb,origLen-size,toPut);

                    size -=toPut;
                    offset+=toPut;
                }
                return new DataIO.DataInputByteArray(bb);
            }else{
                //return mapped buffer
                return getDataInput(offset,size);
            }
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            if(CC.ASSERT && (startOffset >>> sliceShift) != ((endOffset-1) >>> sliceShift))
                throw new AssertionError();
            byte[] buf = getSlice(startOffset);
            int start = (int) (startOffset&sliceSizeModMask);
            int end = (int) (start+(endOffset-startOffset));

            int pos = start;
            while(pos<end){
                System.arraycopy(CLEAR,0,buf,pos, Math.min(CLEAR.length, end-pos));
                pos+=CLEAR.length;
            }
        }

        @Override
        public long getLong(long offset) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            return DataIO.getLong(buf,pos);
        }

        @Override
        public int getInt(long offset) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);

            //TODO verify loop
            final int end = pos + 4;
            int ret = 0;
            for (; pos < end; pos++) {
                ret = (ret << 8) | (buf[pos] & 0xFF);
            }
            return ret;
        }

        @Override
        public byte getByte(long offset) {
            final byte[] b = getSlice(offset);
            return b[((int) (offset & sliceSizeModMask))];
        }

        @Override
        public DataInput getDataInput(long offset, int size) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            return new DataIO.DataInputByteArray(buf,pos);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int length) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = getSlice(offset);
            System.arraycopy(buf,pos,bytes,bytesPos,length);
        }

        @Override
        public void close() {
            closed = true;
            slices =null;
        }

        @Override
        public void sync() {

        }


        @Override
        public int sliceSize() {
            return sliceSize;
        }

        @Override
        public boolean isSliced() {
            return true;
        }

        @Override
        public long length() {
            return ((long)slices.length)*sliceSize;
        }

        @Override
        public File getFile() {
            return null;
        }

        @Override
        public boolean getFileLocked() {
            return false;
        }

    }

    /**
     * Volume backed by on-heap byte[] with maximal fixed size 2GB.
     * For thread-safety it can not be grown
      */
    public static final class SingleByteArrayVol extends Volume{

        protected final static VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                if(initSize>Integer.MAX_VALUE)
                    throw new IllegalArgumentException("startSize larger 2GB");
                return new SingleByteArrayVol((int) initSize);
            }
        };

        protected final byte[] data;

        public SingleByteArrayVol(int size) {
            this(new byte[size]);
        }

        public SingleByteArrayVol(byte[] data){
            this.data = data;
        }


        @Override
        public void ensureAvailable(long offset) {
            if(offset >= data.length){
                throw new DBException.VolumeMaxSizeExceeded(data.length, offset);
            }
        }

        @Override
        public void truncate(long size) {
            //unsupported
            //TODO throw an exception?
        }

        @Override
        public void putLong(long offset, long v) {
            DataIO.putLong(data, (int) offset, v);
        }


        @Override
        public void putInt(long offset, int value) {
            int pos = (int) offset;
            data[pos++] = (byte) (0xff & (value >> 24));
            data[pos++] = (byte) (0xff & (value >> 16));
            data[pos++] = (byte) (0xff & (value >> 8));
            data[pos++] = (byte) (0xff & (value));
        }

        @Override
        public void putByte(long offset, byte value) {
            data[(int) offset] = value;
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            System.arraycopy(src, srcPos, data, (int) offset, srcSize);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
             buf.get(data, (int) offset, buf.remaining());
        }


        @Override
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
            //TODO size>Integer.MAX_VALUE
            target.putData(targetOffset,data, (int) inputOffset, (int) size);
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            int start = (int) startOffset;
            int end = (int) endOffset;

            int pos = start;
            while(pos<end){
                System.arraycopy(CLEAR,0,data,pos, Math.min(CLEAR.length, end-pos));
                pos+=CLEAR.length;
            }
        }

        @Override
        public long getLong(long offset) {
            return DataIO.getLong(data, (int) offset);
        }



        @Override
        public int getInt(long offset) {
            int pos = (int) offset;
            //TODO verify loop
            final int end = pos + 4;
            int ret = 0;
            for (; pos < end; pos++) {
                ret = (ret << 8) | (data[pos] & 0xFF);
            }
            return ret;
        }

        @Override
        public byte getByte(long offset) {
            return data[((int) offset)];
        }

        @Override
        public DataInput getDataInput(long offset, int size) {
             return new DataIO.DataInputByteArray(data, (int) offset);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int length) {
            System.arraycopy(data, (int) offset,bytes,bytesPos,length);
        }

        @Override
        public void close() {
            closed = true;
            //TODO perhaps set `data` to null? what are performance implications for non-final fieldd?
        }

        @Override
        public void sync() {
        }

        @Override
        public int sliceSize() {
            return -1;
        }

        @Override
        public boolean isSliced() {
            return false;
        }

        @Override
        public long length() {
            return data.length;
        }

        @Override
        public File getFile() {
            return null;
        }

        @Override
        public boolean getFileLocked() {
            return false;
        }

    }


    public static final class ReadOnly extends Volume{

        protected final Volume vol;

        public ReadOnly(Volume vol) {
            this.vol = vol;
        }

        @Override
        public void ensureAvailable(long offset) {
            //TODO some error handling here?
            return;
        }

        @Override
        public void truncate(long size) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putLong(long offset, long value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putInt(long offset, int value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putByte(long offset, byte value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public void putDataOverlap(long offset, byte[] src, int srcPos, int srcSize) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public long getLong(long offset) {
            return vol.getLong(offset);
        }

        @Override
        public int getInt(long offset) {
            return vol.getInt(offset);
        }

        @Override
        public byte getByte(long offset) {
            return vol.getByte(offset);
        }

        @Override
        public DataInput getDataInput(long offset, int size) {
            return vol.getDataInput(offset,size);
        }

        @Override
        public DataInput getDataInputOverlap(long offset, int size) {
            return vol.getDataInputOverlap(offset, size);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int size) {
            vol.getData(offset,bytes,bytesPos,size);
        }

        @Override
        public void close() {
            closed = true;
            vol.close();
        }

        @Override
        public void sync() {
            vol.sync();
        }

        @Override
        public int sliceSize() {
            return vol.sliceSize();
        }


        @Override
        public void deleteFile() {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public boolean isSliced() {
            return vol.isSliced();
        }

        @Override
        public long length() {
            return vol.length();
        }

        @Override
        public void putUnsignedShort(long offset, int value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public int getUnsignedShort(long offset) {
            return vol.getUnsignedShort(offset);
        }

        @Override
        public int getUnsignedByte(long offset) {
            return vol.getUnsignedByte(offset);
        }

        @Override
        public void putUnsignedByte(long offset, int b) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public int putLongPackBidi(long offset, long value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public long getLongPackBidi(long offset) {
            return vol.getLongPackBidi(offset);
        }

        @Override
        public long getLongPackBidiReverse(long offset) {
            return vol.getLongPackBidiReverse(offset);
        }

        @Override
        public long getSixLong(long pos) {
            return vol.getSixLong(pos);
        }

        @Override
        public void putSixLong(long pos, long value) {
            throw new IllegalAccessError("read-only");
        }

        @Override
        public File getFile() {
            return vol.getFile();
        }

        @Override
        public boolean getFileLocked() {
            return vol.getFileLocked();
        }

        @Override
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
            vol.transferInto(inputOffset, target, targetOffset, size);
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            throw new IllegalAccessError("read-only");
        }
    }


    public static final class RandomAccessFileVol extends Volume{


        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
                //TODO allocate initSize
                return new RandomAccessFileVol(new File(file), readOnly, fileLockDisable, initSize);
            }
        };
        protected final File file;
        protected final RandomAccessFile raf;
        protected final FileLock fileLock;


        public RandomAccessFileVol(File file, boolean readOnly, boolean fileLockDisable, long initSize) {
            this.file = file;
            try {
                this.raf = new RandomAccessFile(file,readOnly?"r":"rw");
                this.fileLock = Volume.lockFile(file, raf, readOnly, fileLockDisable);

                //grow file if needed
                if(initSize!=0 && !readOnly){
                    long oldLen = raf.length();
                    if(initSize>raf.length()) {
                        raf.setLength(initSize);
                        clear(oldLen,initSize);
                    }
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void ensureAvailable(long offset) {
            try {
                if(raf.length()<offset)
                    raf.setLength(offset);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void truncate(long size) {
            try {
                raf.setLength(size);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void putLong(long offset, long value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
                new IOException("VOL STACK:").printStackTrace();
            }

            try {
                raf.seek(offset);
                raf.writeLong(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public synchronized  void putInt(long offset, int value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
                new IOException("VOL STACK:").printStackTrace();
            }

            try {
                raf.seek(offset);
                raf.writeInt(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public  synchronized void putByte(long offset, byte value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET==offset){
                new IOException("VOL STACK:").printStackTrace();
            }

            try {
                raf.seek(offset);
                raf.writeByte(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public  synchronized void putData(long offset, byte[] src, int srcPos, int srcSize) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+srcSize){
                new IOException("VOL STACK:").printStackTrace();
            }

            try {
                raf.seek(offset);
                raf.write(src,srcPos,srcSize);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void putData(long offset, ByteBuffer buf) {
            byte[] bb = buf.array();
            int pos = buf.position();
            int size = buf.limit()-pos;
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+size){
                new IOException("VOL STACK:").printStackTrace();
            }

            if(bb==null) {
                bb = new byte[size];
                buf.get(bb);
                pos = 0;
            }
            putData(offset,bb,pos, size);
        }

        @Override
        public synchronized long getLong(long offset) {
            try {
                raf.seek(offset);
                return raf.readLong();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized int getInt(long offset) {
            try {
                raf.seek(offset);
                return raf.readInt();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public synchronized byte getByte(long offset) {
            try {
                raf.seek(offset);
                return raf.readByte();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized DataInput getDataInput(long offset, int size) {
            try {
                raf.seek(offset);
                byte[] b = new byte[size];
                raf.readFully(b);
                return new DataIO.DataInputByteArray(b);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void getData(long offset, byte[] bytes, int bytesPos, int size) {
            try {
                raf.seek(offset);
                raf.readFully(bytes,bytesPos,size);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void close() {
            if(closed)
                return;

            closed = true;
            try {
                if(fileLock!=null && fileLock.isValid()){
                    fileLock.release();
                }
                raf.close();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void sync() {
            try {
                raf.getFD().sync();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public int sliceSize() {
            return 0;
        }

        @Override
        public boolean isSliced() {
            return false;
        }

        @Override
        public synchronized long length() {
            try {
                return raf.length();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public File getFile() {
            return file;
        }

        @Override
        public synchronized boolean getFileLocked() {
            return fileLock!=null && fileLock.isValid();
        }

        @Override
        public synchronized void clear(long startOffset, long endOffset) {
            try {
                clearRAF(raf, startOffset, endOffset);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        protected static void clearRAF(RandomAccessFile raf, long startOffset, long endOffset) throws IOException {
            raf.seek(startOffset);
            while(startOffset<endOffset){
                long remaining = Math.min(CLEAR.length, endOffset - startOffset);
                raf.write(CLEAR, 0, (int)remaining);
                startOffset+=CLEAR.length;
            }
        }

        @Override
        public synchronized void putUnsignedShort(long offset, int value) {
            try {
                raf.seek(offset);
                raf.write(value >> 8);
                raf.write(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized int getUnsignedShort(long offset) {
            try {
                raf.seek(offset);
                return (raf.readUnsignedByte() << 8) |
                        raf.readUnsignedByte();

            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized int putLongPackBidi(long offset, long value) {
            try {
                raf.seek(offset);
                raf.write((((int) value & 0x7F)) | 0x80);
                value >>>= 7;
                int counter = 2;

                //$DELAY$
                while ((value & ~0x7FL) != 0) {
                    raf.write(((int) value & 0x7F));
                    value >>>= 7;
                    //$DELAY$
                    counter++;
                }
                //$DELAY$
                raf.write((int) (value | 0x80));
                return counter;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized long getLongPackBidi(long offset) {
            try {
                raf.seek(offset);
                //$DELAY$
                long b = raf.readUnsignedByte(); //TODO this could be inside loop, change all implementations
                if(CC.ASSERT && (b&0x80)==0)
                    throw new DBException.DataCorruption();
                long result = (b & 0x7F) ;
                int shift = 7;
                do {
                    //$DELAY$
                    b = raf.readUnsignedByte();
                    result |= (b & 0x7F) << shift;
                    if(CC.ASSERT && shift>64)
                        throw new DBException.DataCorruption();
                    shift += 7;
                }while((b & 0x80) == 0);
                //$DELAY$
                return (((long)(shift/7))<<60) | result;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public synchronized long getLongPackBidiReverse(long offset) {
            try {
                //$DELAY$
                raf.seek(--offset);
                long b = raf.readUnsignedByte();
                if(CC.ASSERT && (b&0x80)==0)
                    throw new DBException.DataCorruption();
                long result = (b & 0x7F) ;
                int counter = 1;
                do {
                    //$DELAY$
                    raf.seek(--offset);
                    b = raf.readUnsignedByte();
                    result = (b & 0x7F) | (result<<7);
                    if(CC.ASSERT && counter>8)
                        throw new DBException.DataCorruption();
                    counter++;
                }while((b & 0x80) == 0);
                //$DELAY$
                return (((long)counter)<<60) | result;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public synchronized long getSixLong(long offset) {
            try {
                raf.seek(offset);
                return
                        (((long) raf.readUnsignedByte()) << 40) |
                                (((long) raf.readUnsignedByte()) << 32) |
                                (((long) raf.readUnsignedByte()) << 24) |
                                (raf.readUnsignedByte() << 16) |
                                (raf.readUnsignedByte() << 8) |
                                raf.readUnsignedByte();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void putSixLong(long pos, long value) {
            if(CC.ASSERT && (value >>>48!=0))
                throw new DBException.DataCorruption();
            try {
                raf.seek(pos);

                raf.write((int) (value >>> 40));
                raf.write((int) (value >>> 32));
                raf.write((int) (value >>> 24));
                raf.write((int) (value >>> 16));
                raf.write((int) (value >>> 8));
                raf.write((int) (value));
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public int putPackedLong(long pos, long value) {
            try {
                raf.seek(pos);

                //$DELAY$
                int ret = 1;
                int shift = 63-Long.numberOfLeadingZeros(value);
                shift -= shift%7; // round down to nearest multiple of 7
                while(shift!=0){
                    ret++;
                    raf.write((int) (((value >>> shift) & 0x7F) | 0x80));
                    //$DELAY$
                    shift-=7;
                }
                raf.write ((int) (value & 0x7F));
                return ret;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }



        @Override
        public long getPackedLong(long pos) {
            try {
                raf.seek(pos);

                long ret = 0;
                long pos2 = 0;
                byte v;
                do{
                    pos2++;
                    v = raf.readByte();
                    ret = (ret<<7 ) | (v & 0x7F);
                }while(v<0);

                return (pos2<<60) | ret;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public synchronized long hash(long off, long len, long seed){
            if (len < 0) {
                throw new IllegalArgumentException("lengths must be >= 0");
            }
            long bufLen = length();
            if(off<0 || off>=bufLen || off+len<0 || off+len>bufLen){
                throw new IndexOutOfBoundsException();
            }

            final long end = off + len;
            long h64;

            try {
                raf.seek(off);

                if (len >= 32) {
                    final long limit = end - 32;
                    long v1 = seed + PRIME64_1 + PRIME64_2;
                    long v2 = seed + PRIME64_2;
                    long v3 = seed + 0;
                    long v4 = seed - PRIME64_1;
                    do {
                        v1 += Long.reverseBytes(raf.readLong()) * PRIME64_2;
                        v1 = rotateLeft(v1, 31);
                        v1 *= PRIME64_1;
                        off += 8;

                        v2 += Long.reverseBytes(raf.readLong()) * PRIME64_2;
                        v2 = rotateLeft(v2, 31);
                        v2 *= PRIME64_1;
                        off += 8;

                        v3 += Long.reverseBytes(raf.readLong()) * PRIME64_2;
                        v3 = rotateLeft(v3, 31);
                        v3 *= PRIME64_1;
                        off += 8;

                        v4 += Long.reverseBytes(raf.readLong()) * PRIME64_2;
                        v4 = rotateLeft(v4, 31);
                        v4 *= PRIME64_1;
                        off += 8;
                    } while (off <= limit);

                    h64 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);

                    v1 *= PRIME64_2;
                    v1 = rotateLeft(v1, 31);
                    v1 *= PRIME64_1;
                    h64 ^= v1;
                    h64 = h64 * PRIME64_1 + PRIME64_4;

                    v2 *= PRIME64_2;
                    v2 = rotateLeft(v2, 31);
                    v2 *= PRIME64_1;
                    h64 ^= v2;
                    h64 = h64 * PRIME64_1 + PRIME64_4;

                    v3 *= PRIME64_2;
                    v3 = rotateLeft(v3, 31);
                    v3 *= PRIME64_1;
                    h64 ^= v3;
                    h64 = h64 * PRIME64_1 + PRIME64_4;

                    v4 *= PRIME64_2;
                    v4 = rotateLeft(v4, 31);
                    v4 *= PRIME64_1;
                    h64 ^= v4;
                    h64 = h64 * PRIME64_1 + PRIME64_4;
                } else {
                    h64 = seed + PRIME64_5;
                }

                h64 += len;

                while (off <= end - 8) {
                    long k1 = Long.reverseBytes(raf.readLong());
                    k1 *= PRIME64_2;
                    k1 = rotateLeft(k1, 31);
                    k1 *= PRIME64_1;
                    h64 ^= k1;
                    h64 = rotateLeft(h64, 27) * PRIME64_1 + PRIME64_4;
                    off += 8;
                }

                if (off <= end - 4) {
                    h64 ^= (Integer.reverseBytes(raf.readInt()) & 0xFFFFFFFFL) * PRIME64_1;
                    h64 = rotateLeft(h64, 23) * PRIME64_2 + PRIME64_3;
                    off += 4;
                }

                while (off < end) {
                    h64 ^= (raf.readByte() & 0xFF) * PRIME64_5;
                    h64 = rotateLeft(h64, 11) * PRIME64_1;
                    ++off;
                }

                h64 ^= h64 >>> 33;
                h64 *= PRIME64_2;
                h64 ^= h64 >>> 29;
                h64 *= PRIME64_3;
                h64 ^= h64 >>> 32;

                return h64;
            }catch(IOException e){
                throw new DBException.VolumeIOError(e);
            }
        }

    }

    private static FileLock lockFile(File file, RandomAccessFile raf, boolean readOnly, boolean fileLockDisable) {
        if(fileLockDisable || readOnly){
            return null;
        }else {
            try {
                return raf.getChannel().lock();
            } catch (Exception e) {
                throw new DBException.FileLocked("Can not lock file, perhaps other DB is already using it. File: " + file, e);
            }
        }

    }
}

