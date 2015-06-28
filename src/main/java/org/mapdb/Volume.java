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

package org.mapdb;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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

    public static abstract class VolumeFactory{
        public abstract Volume makeVolume(String file, boolean readOnly,
                                          int sliceShift, long initSize, boolean fixedSize);

        public Volume makeVolume(String file, boolean readOnly){
            return makeVolume(file,readOnly,CC.VOLUME_PAGE_SHIFT, 0, false);
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
        public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
            String packageName = Volume.class.getPackage().getName();
            Class clazz;
            try {
                clazz = Class.forName(packageName+".UnsafeStuff$UnsafeVolume");
            } catch (ClassNotFoundException e) {
                clazz = null;
            }

            if(clazz!=null){
                try {
                    return (Volume) clazz.getConstructor(long.class, int.class).newInstance(0L, sliceShift);
                } catch (Exception e) {
                    LOG.log(Level.WARNING, "Could not invoke UnsafeVolume constructor. " +
                            "Falling back to DirectByteBuffer",e);

                }
            }

            return MemoryVol.FACTORY.makeVolume(file, readOnly, sliceShift, initSize, fixedSize);
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

    public abstract boolean isEmpty();

    public void deleteFile(){
        File f = getFile();
        if(f!=null && !f.delete()){
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
        return (((long)(shift/7))<<56) | result;
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
        return (((long)counter)<<56) | result;
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
     * Abstract Volume over bunch of ByteBuffers
     * It leaves ByteBufferVol details (allocation, disposal) on subclasses.
     * Most methods are final for better performance (JIT compiler can inline those).
     */
    abstract static public class ByteBufferVol extends Volume{

        protected final ReentrantLock growLock = new ReentrantLock(CC.FAIR_LOCKS);
        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected volatile ByteBuffer[] slices = new ByteBuffer[0];
        protected final boolean readOnly;

        protected ByteBufferVol(boolean readOnly,  int sliceShift) {
            this.readOnly = readOnly;
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;
        }


        @Override
        public final void ensureAvailable(long offset) {
            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos< slices.length)
                    return;

                int oldSize = slices.length;
                ByteBuffer[] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, Math.max(slicePos+1, slices2.length + slices2.length/1000));

                for(int pos=oldSize;pos<slices2.length;pos++) {
                    slices2[pos]=makeNewBuffer(1L* sliceSize *pos);
                }


                slices = slices2;
            }finally{
                growLock.unlock();
            }
        }

        protected abstract ByteBuffer makeNewBuffer(long offset);

        @Override public final void putLong(final long offset, final long value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
                new IOException("VOL STACK:").printStackTrace();
            }

            slices[(int)(offset >>> sliceShift)].putLong((int) (offset & sliceSizeModMask), value);
        }

        @Override public final void putInt(final long offset, final int value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
                new IOException("VOL STACK:").printStackTrace();
            }

            slices[(int)(offset >>> sliceShift)].putInt((int) (offset & sliceSizeModMask), value);
        }


        @Override public final void putByte(final long offset, final byte value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+1){
                new IOException("VOL STACK:").printStackTrace();
            }

            slices[(int)(offset >>> sliceShift)].put((int) (offset & sliceSizeModMask), value);
        }



        @Override public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+srcSize){
                new IOException("VOL STACK:").printStackTrace();
            }


            final ByteBuffer b1 = slices[(int)(offset >>> sliceShift)].duplicate();
            final int bufPos = (int) (offset& sliceSizeModMask);

            b1.position(bufPos);
            b1.put(src, srcPos, srcSize);
        }


        @Override public final void putData(final long offset, final ByteBuffer buf) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+buf.remaining()){
                new IOException("VOL STACK:").printStackTrace();
            }

            final ByteBuffer b1 = slices[(int)(offset >>> sliceShift)].duplicate();
            final int bufPos = (int) (offset& sliceSizeModMask);
            //no overlap, so just write the value
            b1.position(bufPos);
            b1.put(buf);
        }

        @Override
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
            final ByteBuffer b1 = slices[(int)(inputOffset >>> sliceShift)].duplicate();
            final int bufPos = (int) (inputOffset& sliceSizeModMask);

            b1.position(bufPos);
            //TODO size>Integer.MAX_VALUE
            b1.limit((int) (bufPos+size));
            target.putData(targetOffset,b1);
        }

        @Override public void getData(final long offset, final byte[] src, int srcPos, int srcSize){
            final ByteBuffer b1 = slices[(int)(offset >>> sliceShift)].duplicate();
            final int bufPos = (int) (offset& sliceSizeModMask);

            b1.position(bufPos);
            b1.get(src, srcPos, srcSize);
        }


        @Override final public long getLong(long offset) {
            return slices[(int)(offset >>> sliceShift)].getLong((int) (offset& sliceSizeModMask));
        }

        @Override final public int getInt(long offset) {
            return slices[(int)(offset >>> sliceShift)].getInt((int) (offset& sliceSizeModMask));
        }


        @Override public final byte getByte(long offset) {
            return slices[(int)(offset >>> sliceShift)].get((int) (offset& sliceSizeModMask));
        }


        @Override
        public final DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            return new DataIO.DataInputByteBuffer(slices[(int)(offset >>> sliceShift)], (int) (offset& sliceSizeModMask));
        }



        @Override
        public void putDataOverlap(long offset, byte[] data, int pos, int len) {
            boolean overlap = (offset>>>sliceShift != (offset+len)>>>sliceShift);

            if(overlap){
                while(len>0){
                    ByteBuffer b = slices[((int) (offset >>> sliceShift))].duplicate();
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
                    ByteBuffer b = slices[((int) (offset >>> sliceShift))].duplicate();
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
        public void clear(long startOffset, long endOffset) {
            if(CC.ASSERT && (startOffset >>> sliceShift) != ((endOffset-1) >>> sliceShift))
                throw new AssertionError();
            ByteBuffer buf = slices[(int)(startOffset >>> sliceShift)];
            int start = (int) (startOffset&sliceSizeModMask);
            int end = (int) (endOffset&sliceSizeModMask);

            int pos = start;
            while(pos<end){
                buf = buf.duplicate();
                buf.position(pos);
                buf.put(CLEAR, 0, Math.min(CLEAR.length, end-pos));
                pos+=CLEAR.length;
            }
        }

        @Override
        public boolean isEmpty() {
            return slices==null || slices.length==0;
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
        protected boolean unmap(MappedByteBuffer b){
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
                        SerializerPojo.classForName("sun.nio.ch.DirectBuffer")!=null;
            }catch(Exception e){
                unmapHackSupported = false;
            }
        }

        // Workaround for https://github.com/jankotek/MapDB/issues/326
        // File locking after .close() on Windows.
        private static boolean windowsWorkaround = System.getProperty("os.name").toLowerCase().startsWith("win");


    }

    public static final class MappedFileVol extends ByteBufferVol {

        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                //TODO optimize if fixedSize is bellow 2GB
                //TODO prealocate initsize
                return new MappedFileVol(new File(file),readOnly,sliceShift);
            }
        };

        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;
        protected final java.io.RandomAccessFile raf;


        public MappedFileVol(File file, boolean readOnly, int sliceShift) {
            super(readOnly,sliceShift);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                FileChannelVol.checkFolder(file,readOnly);
                this.raf = new java.io.RandomAccessFile(file, readOnly?"r":"rw");
                this.fileChannel = raf.getChannel();

                final long fileSize = fileChannel.size();
                if(fileSize>0){
                    //map existing data
                    slices = new ByteBuffer[(int) ((fileSize>>> sliceShift))];
                    for(int i=0;i< slices.length;i++){
                        slices[i] = makeNewBuffer(1L*i* sliceSize);
                    }
                }else{
                    slices = new ByteBuffer[0];
                }
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void close() {
            growLock.lock();
            try{
                closed = true;
                fileChannel.close();
                raf.close();
                //TODO not sure if no sync causes problems while unlocking files
                //however if it is here, it causes slow commits, sync is called on write-ahead-log just before it is deleted and closed
//                if(!readOnly)
//                    sync();

                for(ByteBuffer b: slices){
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        unmap((MappedByteBuffer) b);
                    }
                }

                slices = null;

            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }finally{
                growLock.unlock();
            }

        }

        @Override
        public void sync() {
            if(readOnly) return;
            growLock.lock();
            try{
                for(ByteBuffer b: slices){
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
        public int sliceSize() {
            return sliceSize;
        }

        @Override
        protected ByteBuffer makeNewBuffer(long offset) {
            try {
                if(CC.ASSERT && ! ((offset& sliceSizeModMask)==0))
                    throw new AssertionError();
                if(CC.ASSERT && ! (offset>=0))
                    throw new AssertionError();
                ByteBuffer ret = fileChannel.map(mapMode,offset, sliceSize);
                if(CC.ASSERT && ret.order() != ByteOrder.BIG_ENDIAN)
                    throw new AssertionError("Little-endian");
                if(mapMode == FileChannel.MapMode.READ_ONLY) {
                    ret = ret.asReadOnlyBuffer();
                }
                return ret;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public boolean isEmpty() {
            return length()<=0;
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
                    unmap((MappedByteBuffer) old[i]);
                    old[i] = null;
                }

                if (ByteBufferVol.windowsWorkaround) {
                    for(int i=0;i<maxSize;i++){
                        unmap((MappedByteBuffer) old[i]);
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
                        slices[pos]=makeNewBuffer(1L* sliceSize *pos);
                    }
                }

            }finally {
                growLock.unlock();
            }
        }

    }

    public static final class MemoryVol extends ByteBufferVol {

        /** factory for DirectByteBuffer storage*/
        public static final VolumeFactory FACTORY = new VolumeFactory() {
            @Override
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                //TODO prealocate initSize
                //TODO optimize for fixedSize smaller than 2GB
                return new MemoryVol(true,sliceShift);
            }
        }
                ;
        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString()+",direct="+useDirectBuffer;
        }

        public MemoryVol(final boolean useDirectBuffer, final int sliceShift) {
            super(false, sliceShift);
            this.useDirectBuffer = useDirectBuffer;
        }

        @Override
        protected ByteBuffer makeNewBuffer(long offset) {
            try {
                ByteBuffer b =  useDirectBuffer ?
                        ByteBuffer.allocateDirect(sliceSize) :
                        ByteBuffer.allocate(sliceSize);
                if(CC.ASSERT && b.order()!= ByteOrder.BIG_ENDIAN)
                    throw new AssertionError("little-endian");
                return b;
            }catch(OutOfMemoryError e){
                throw new DBException.OutOfMemory(e);
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
                    if(old[i] instanceof  MappedByteBuffer)
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
                for(ByteBuffer b: slices){
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        unmap((MappedByteBuffer)b);
                    }
                }
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
    }


    /**
     * Volume which uses FileChannel.
     * Uses global lock and does not use mapped memory.
     */
    public static final class FileChannelVol extends Volume {

        public static final VolumeFactory FACTORY = new VolumeFactory() {

            @Override
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                return new FileChannelVol(new File(file),readOnly, sliceShift);
            }
        };

        protected final File file;
        protected final int sliceSize;
        protected RandomAccessFile raf;
        protected FileChannel channel;
        protected final boolean readOnly;

        protected volatile long size;
        protected final Lock growLock = new ReentrantLock(CC.FAIR_LOCKS);

        public FileChannelVol(File file, boolean readOnly, int sliceShift){
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
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        public FileChannelVol(File file) {
            this(file, false,CC.VOLUME_PAGE_SHIFT);
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
            if(offset% sliceSize !=0)
                offset += sliceSize - offset% sliceSize; //round up to multiply of slice size

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
        public void close() {
            try{
                closed = true;
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
        public boolean isEmpty() {
            try {
                return channel==null || channel.size()==0;
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
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                //TODO optimize for fixedSize if bellow 2GB
                //TODO preallocate minimal size
                return new ByteArrayVol(sliceShift);
            }
        };

        protected final ReentrantLock growLock = new ReentrantLock(CC.FAIR_LOCKS);

        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected volatile byte[][] slices = new byte[0][];

        protected ByteArrayVol(int sliceShift) {
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;
        }

        @Override
        public final void ensureAvailable(long offset) {

            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return;
            }

            growLock.lock();
            try {
                //check second time
                if (slicePos < slices.length)
                    return;

                int oldSize = slices.length;
                byte[][] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, Math.max(slicePos + 1, slices2.length + slices2.length / 1000));

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
            byte[] buf = slices[((int) (offset >>> sliceShift))];
            DataIO.putLong(buf,pos,v);
        }


        @Override
        public void putInt(long offset, int value) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];
            buf[pos++] = (byte) (0xff & (value >> 24));
            buf[pos++] = (byte) (0xff & (value >> 16));
            buf[pos++] = (byte) (0xff & (value >> 8));
            buf[pos++] = (byte) (0xff & (value));
        }

        @Override
        public void putByte(long offset, byte value) {
            final byte[] b = slices[((int) (offset >>> sliceShift))];
            b[((int) (offset & sliceSizeModMask))] = value;
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];

            System.arraycopy(src,srcPos,buf,pos,srcSize);
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] dst = slices[((int) (offset >>> sliceShift))];
            buf.get(dst, pos, buf.remaining());
        }


        @Override
        public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
            int pos = (int) (inputOffset & sliceSizeModMask);
            byte[] buf = slices[((int) (inputOffset >>> sliceShift))];

            //TODO size>Integer.MAX_VALUE
            target.putData(targetOffset,buf,pos, (int) size);
        }



        @Override
        public void putDataOverlap(long offset, byte[] data, int pos, int len) {
            boolean overlap = (offset>>>sliceShift != (offset+len)>>>sliceShift);

            if(overlap){
                while(len>0){
                    byte[] b = slices[((int) (offset >>> sliceShift))];
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
                    byte[] b = slices[((int) (offset >>> sliceShift))];
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
            byte[] buf = slices[(int)(startOffset >>> sliceShift)];
            int start = (int) (startOffset&sliceSizeModMask);
            int end = (int) (endOffset&sliceSizeModMask);

            int pos = start;
            while(pos<end){
                System.arraycopy(CLEAR,0,buf,pos, Math.min(CLEAR.length, end-pos));
                pos+=CLEAR.length;
            }
        }

        @Override
        public long getLong(long offset) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];
            return DataIO.getLong(buf,pos);
        }



        @Override
        public int getInt(long offset) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];

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
            final byte[] b = slices[((int) (offset >>> sliceShift))];
            return b[((int) (offset & sliceSizeModMask))];
        }

        @Override
        public DataInput getDataInput(long offset, int size) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];
            return new DataIO.DataInputByteArray(buf,pos);
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int length) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];
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
        public boolean isEmpty() {
            return slices.length==0;
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

    }

    /**
     * Volume backed by on-heap byte[] with maximal fixed size 2GB.
     * For thread-safety it can not be grown
      */
    public static final class SingleByteArrayVol extends Volume{

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
                //TODO throw an exception
            }
        }

        @Override
        public void truncate(long size) {
            //unsupported
            //TODO throw an exception?
        }

        @Override
        public void putLong(long offset, long v) {
            DataIO.putLong(data, (int) offset,v);
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
        public boolean isEmpty() {
            //TODO better way to check if data were written here, perhaps eliminate this method completely
            for(byte b:data){
                if(b!=0)
                    return false;
            }
            return true;
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
        public boolean isEmpty() {
            return vol.isEmpty();
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
            public Volume makeVolume(String file, boolean readOnly, int sliceShift, long initSize, boolean fixedSize) {
                //TODO allocate initSize
                return new RandomAccessFileVol(new File(file), readOnly);
            }
        };
        protected final File file;
        protected final RandomAccessFile raf;

        public RandomAccessFileVol(File file, boolean readOnly) {
            this.file = file;
            try {
                this.raf = new RandomAccessFile(file,readOnly?"r":"rw");
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void ensureAvailable(long offset) {
            //TODO ensure avail
        }

        @Override
        public void truncate(long size) {
            try {
                raf.setLength(size);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void putLong(long offset, long value) {
            try {
                raf.seek(offset);
                raf.writeLong(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public synchronized  void putInt(long offset, int value) {
            try {
                raf.seek(offset);
                raf.writeInt(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public  synchronized void putByte(long offset, byte value) {
            try {
                raf.seek(offset);
                raf.writeByte(value);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

        }

        @Override
        public  synchronized void putData(long offset, byte[] src, int srcPos, int srcSize) {
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
                raf.read(b);
                return new DataIO.DataInputByteArray(b);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public synchronized void getData(long offset, byte[] bytes, int bytesPos, int size) {
            try {
                raf.seek(offset);
                raf.read(bytes,bytesPos,size);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void close() {
            closed = true;
            try {
                raf.close();
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void sync() {
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
        public boolean isEmpty() {
            try {
                return isClosed() || raf.length()==0;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public boolean isSliced() {
            return false;
        }

        @Override
        public long length() {
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
        public synchronized void clear(long startOffset, long endOffset) {
            try {
                raf.seek(startOffset);
                while(startOffset<endOffset){
                    long remaining = Math.min(CLEAR.length, endOffset - startOffset);
                    raf.write(CLEAR, 0, (int)remaining);
                    startOffset+=CLEAR.length;
                }

            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }
    }
}

