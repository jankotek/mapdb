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

import net.jpountz.xxhash.StreamingXXHash64;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DataIO;
import org.mapdb.DataInput2;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;


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
        public Volume makeVolume(String file, boolean readOnly, long fileLockWait, int sliceShift, long initSize, boolean fixedSize) {
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

            return ByteBufferMemoryVol.FACTORY.makeVolume(file, readOnly, fileLockWait, sliceShift, initSize, fixedSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
        }

        @Override
        public boolean handlesReadonly() {
            return false; //TODO unsafe and reaodnly
        }
    };

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public boolean isClosed(){
        return closed.get();
    }

    //uncomment to get stack trace on Volume leak warning
//    final private Throwable constructorStackTrace = new AssertionError();

    @Override protected void finalize(){
        if(CC.LOG_VOLUME_GCED){
            if(!closed.get()
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
            putByte(pos + (ret++), (byte) ((value >>> shift) & 0x7F));
            //$DELAY$
            shift-=7;
        }
        putByte(pos+(ret++),(byte) ((value & 0x7F)| 0x80));
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
        }while((v&0x80)==0);

        return (pos2<<60) | ret;
    }

    abstract public boolean isReadOnly();

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
    public void copyTo(long inputOffset, Volume target, long targetOffset, long size) {
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

        long offset = Math.min(endOffset, DataIO.roundUp(startOffset, bufSize));
        if (offset != startOffset) {
            clear(startOffset, offset);
        }

        long prevOffset = offset;
        offset = Math.min(endOffset, DataIO.roundUp(offset + 1, bufSize));

        while (prevOffset < endOffset){
            clear(prevOffset, offset);
            prevOffset = offset;
            offset = Math.min(endOffset, DataIO.roundUp(offset + 1, bufSize));
        }

        if(CC.ASSERT && prevOffset!=endOffset)
            throw new AssertionError();
}


    /**
     * Copy content of this volume to another.
     * Target volume might grow, but is never shrank.
     * Target is also not synced
     */
    public void copyTo(Volume to) {
        final long volSize = length();
        final long bufSize = 1L<< CC.PAGE_SHIFT;

        to.ensureAvailable(volSize);

        for(long offset=0;offset<volSize;offset+=bufSize){
            long size = Math.min(volSize,offset+bufSize)-offset;
            if(CC.ASSERT && (size<0))
                throw new AssertionError();
            copyTo(offset,to,offset, size);
        }
    }


    /**
     * Copy content from InputStream into this Volume.
     */
    public void copyFrom(InputStream input) {
        byte[] buf = new byte[1024];
        long offset = 0;
        try {
            while(true){
                int read = input.read(buf);
                if(read==-1)
                    return;
                ensureAvailable(offset+read);
                putData(offset, buf, 0, read);
                offset+=read;
            }
        } catch (IOException e) {
            throw new IOError(e);
        }
    }


    /**
     * Copy content of this volume to OutputStream.
     */
    public void copyTo(OutputStream output) {
        final long volSize = length();

        byte[] buf = new byte[1024];
        for(long offset=0;offset<volSize;offset+=buf.length){
            int size = (int)(Math.min(volSize,offset+buf.length)-offset);
            if(CC.ASSERT && (size<0))
                throw new AssertionError();

            getData(offset, buf, 0, size);
            try {
                output.write(buf, 0, size);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    /**
     * <p>
     * Calculates XXHash64 from this Volume content.
     * </p>
     * @param off offset to start calculation from
     * @param len length of data to calculate hash
     * @param seed  hash seed
     * @return XXHash.
     */
    public long hash(long off, long len, long seed){
        final int blen = 128;
        byte[] b = new byte[blen];
        StreamingXXHash64 s = CC.HASH_FACTORY.newStreamingHash64(seed);
        len +=off;

        //round size to multiple of blen
        int size = (int)Math.min(len-off,Math.min(blen, DataIO.roundUp(off, blen) - off));
        getData(off,b,0,size);
        s.update(b,0,size);
        off+=size;

        //read rest of the data
        while (off<len) {
            size = (int)Math.min(blen, len-off);
            getData(off,b,0,size);
            s.update(b,0,size);
            off+=size;
        }

        return s.getValue();
    }

//
//    /** transfer data from one volume to second. Second volume will be expanded if needed*/
//    public static void copyTo(long size, Volume from, Volume to){
//        int bufSize = Math.min(from.sliceSize(),to.sliceSize());
//
//        if(bufSize<0 || bufSize>1024*1024*128){
//            bufSize = 64 * 1024; //something strange, set safe limit
//        }
//        to.ensureAvailable(size);
//
//        for(long offset=0;offset<size;offset+=bufSize){
//            int bb = (int) Math.min(bufSize, size-offset);
//            from.copyTo(offset,to,offset,bb);
//        }
//    }


    static FileLock lockFile(File file, FileChannel channel, boolean readOnly, long fileLockWait) {
        if(fileLockWait<0 || readOnly){
            return null;
        }
        while(true) {
            try {
                FileLock lock = channel.tryLock();
                if(lock != null)
                    return lock;
            } catch (OverlappingFileLockException e) {
                if (fileLockWait <= 0) {
                    throw new DBException.FileLocked(file.toPath(), e);
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

            if (fileLockWait <= 0) {
                throw new DBException.FileLocked(file.toPath(), null);
            }

            // wait until file becomes unlocked
            try {
                Thread.sleep(100);
            } catch (InterruptedException e1) {
                throw new DBException.Interrupted(e1);
            }
            fileLockWait -= 100;
        }
    }
}

