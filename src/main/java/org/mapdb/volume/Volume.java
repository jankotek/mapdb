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

package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DBUtil;
import org.mapdb.DataInput2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileLock;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Long.rotateLeft;
import static org.mapdb.DBUtil.*;


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
        //PERF optimize this method with bitcount operation
        sizeIncrement = DBUtil.nextPowTwo(sizeIncrement);
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
     * @throws DBException.DataCorruption if some byte is not zero
     */
    public void assertZeroes(long startOffset, long endOffset) throws DBException.DataCorruption{
        for(long offset=startOffset;offset<endOffset;offset++){
            if(getUnsignedByte(offset)!=0)
                throw new DBException.DataCorruption("Not zero at offset: "+offset );
        }
    }


    static final byte[] CLEAR = new byte[1024];

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

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
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



    abstract public DataInput2 getDataInput(final long offset, final int size);
    public DataInput2 getDataInputOverlap(final long offset, final int size){
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
            throw new DBException.DataCorruption("six long illegal value");

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
     * One can use {@code result & DBUtil.PACK_LONG_RESULT_MASK} to remove size;
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
    public abstract void clear(final long startOffset, final long endOffset);

    public void clearOverlap(final long startOffset, final long endOffset) {
        if (CC.ASSERT && startOffset > endOffset)
            throw new AssertionError();

        final long bufSize = 1L << CC.PAGE_SHIFT;

        long offset = Math.min(endOffset, DBUtil.roundUp(startOffset, bufSize));
        if (offset != startOffset) {
            clear(startOffset, offset);
        }

        long prevOffset = offset;
        offset = Math.min(endOffset, DBUtil.roundUp(offset + 1, bufSize));

        while (prevOffset < endOffset){
            clear(prevOffset, offset);
            prevOffset = offset;
            offset = Math.min(endOffset, DBUtil.roundUp(offset + 1, bufSize));
        }

        if(CC.ASSERT && prevOffset!=endOffset)
            throw new AssertionError();
}


    /**
     * Copy content of this volume to another.
     * Target volume might grow, but is never shrank.
     * Target is also not synced
     */
    public void copyEntireVolumeTo(Volume to) {
        final long volSize = length();
        final long bufSize = 1L<< CC.PAGE_SHIFT;

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
        if(len==0)
            return seed;

        long bufLen = length();
        if(off<0 || off>=bufLen || off+len<0 || off+len>bufLen){
            throw new IndexOutOfBoundsException();
        }

        while((off&0x7)!=0 && len>0){
            //scroll until offset is not dividable by 8
            seed = (seed<<8) | getUnsignedByte(off);
            off++;
            len--;
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


    public static final class MemoryVol extends ByteBufferVol {

        /** factory for DirectByteBuffer storage*/
        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
                //TODO optimize for fixedSize smaller than 2GB
                return new MemoryVol(true,sliceShift,false, initSize);
            }

            @NotNull
            @Override
            public boolean exists(@Nullable String file) {
                return false;
            }
        };


        /** factory for DirectByteBuffer storage*/
        public static final VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {//TODO prealocate initSize
                //TODO optimize for fixedSize smaller than 2GB
                return new MemoryVol(true,sliceShift,true, initSize);
            }

            @NotNull
            @Override
            public boolean exists(@Nullable String file) {
                return false;
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
            offset= DBUtil.roundUp(offset,1L<<sliceShift);
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


    static FileLock lockFile(File file, RandomAccessFile raf, boolean readOnly, boolean fileLockDisable) {
        if(fileLockDisable || readOnly){
            return null;
        }else {
            try {
                return raf.getChannel().lock();
            } catch (Exception e) {
                throw new DBException.FileLocked(file.toPath(), e);
            }
        }

    }
}

