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
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MapDB abstraction over raw storage (file, disk partition, memory etc...).
 * <p>
 * Implementations needs to be thread safe (especially
 'ensureAvailable') operation.
 * However updates do not have to be atomic, it is clients responsibility
 * to ensure two threads are not writing/reading into the same location.
 *
 * @author Jan Kotek
 */
public abstract class Volume implements Closeable{

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

    abstract public long getLong(final long offset);
    abstract public int getInt(long offset);
    abstract public byte getByte(final long offset);



    abstract public DataInput getDataInput(final long offset, final int size);
    abstract public void getData(long offset, byte[] bytes, int bytesPos, int size);

    abstract public void close();

    abstract public void sync();

    /**
     *
     * @return slice size or `-1` if not sliced
     */
    abstract public int sliceSize();

    public abstract boolean isEmpty();

    public abstract void deleteFile();

    public abstract boolean isSliced();


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
        putByte(offset, (byte)(b & 0xff));
    }


    public int putLongPackBidi(long offset, long value){
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
        long b = getUnsignedByte(offset++);
        if(CC.PARANOID && (b&0x80)==0)
            throw new AssertionError();
        long result = (b & 0x7F) ;
        int shift = 7;
        do {
            //$DELAY$
            b = getUnsignedByte(offset++);
            result |= (b & 0x7F) << shift;
            if(CC.PARANOID && shift>64)
                throw new AssertionError();
            shift += 7;
        }while((b & 0x80) == 0);
        //$DELAY$
        return (((long)(shift/7))<<56) | result;
    }

    public long getLongPackBidiReverse(long offset){
        //$DELAY$
        long b = getUnsignedByte(--offset);
        if(CC.PARANOID && (b&0x80)==0)
            throw new AssertionError();
        long result = (b & 0x7F) ;
        int counter = 1;
        do {
            //$DELAY$
            b = getUnsignedByte(--offset);
            result = (b & 0x7F) | (result<<7);
            if(CC.PARANOID && counter>8)
                throw new AssertionError();
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
        if(CC.PARANOID && (value>>>48!=0))
            throw new AssertionError();

        putByte(pos++, (byte) (0xff & (value >> 40)));
        putByte(pos++, (byte) (0xff & (value >> 32)));
        putByte(pos++, (byte) (0xff & (value >> 24)));
        putByte(pos++, (byte) (0xff & (value >> 16)));
        putByte(pos++, (byte) (0xff & (value >> 8)));
        putByte(pos, (byte) (0xff & (value)));
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
    public void transferInto(long inputOffset, Volume target, long targetOffset, int size) {
        byte[] data = new byte[size];
        try {
            getDataInput(inputOffset, size).readFully(data);
        }catch(IOException e){
            throw new DBException.VolumeIOError(e);
        }
        target.putData(targetOffset,data,0,size);
    }


    public static Volume volumeForFile(File f, boolean useRandomAccessFile, boolean readOnly,  int sliceShift, int sizeIncrement) {
        return useRandomAccessFile ?
                new FileChannelVol(f, readOnly, sliceShift, sizeIncrement):
                new MappedFileVol(f, readOnly,sliceShift, sizeIncrement);
    }

    /**
     * Set all bytes between {@code startOffset} and {@code endOffset} to zero.
     * Area between offsets must be ready for write once clear finishes.
     */
    public abstract void clear(long startOffset, long endOffset);



    public static Fun.Function1<Volume,String> fileFactory(){
        return fileFactory(false,false,CC.VOLUME_PAGE_SHIFT,0);
    }

    public static Fun.Function1<Volume,String> fileFactory(
            final boolean useRandomAccessFile,
            final boolean readOnly,
            final int sliceShift,
            final int sizeIncrement) {
        return new Fun.Function1<Volume, String>() {
            @Override
            public Volume run(String file) {
                return volumeForFile(new File(file), useRandomAccessFile,
                        readOnly,  sliceShift, sizeIncrement);
            }
        };
    }


    public static Fun.Function1<Volume,String> memoryFactory(){
        return memoryFactory(false,CC.VOLUME_PAGE_SHIFT);
    }

    public static Fun.Function1<Volume,String> memoryFactory(
            final boolean useDirectBuffer, final int sliceShift) {
        return new Fun.Function1<Volume,String>() {

            @Override
            public Volume run(String s) {
                return useDirectBuffer?
                        new MemoryVol(true,  sliceShift):
                        new ByteArrayVol(sliceShift);
            }
        };
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
        public void transferInto(long inputOffset, Volume target, long targetOffset, int size) {
            final ByteBuffer b1 = slices[(int)(inputOffset >>> sliceShift)].duplicate();
            final int bufPos = (int) (inputOffset& sliceSizeModMask);

            b1.position(bufPos);
            b1.limit(bufPos+size);
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
        public void clear(long startOffset, long endOffset) {
            if(CC.PARANOID && (startOffset >>> sliceShift) != ((endOffset-1) >>> sliceShift))
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
            return slices.length==0;
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
        protected void unmap(MappedByteBuffer b){
            try{
                if(unmapHackSupported){

                    // need to dispose old direct buffer, see bug
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
                    Method cleanerMethod = b.getClass().getMethod("cleaner", new Class[0]);
                    if(cleanerMethod!=null){
                        cleanerMethod.setAccessible(true);
                        Object cleaner = cleanerMethod.invoke(b);
                        if(cleaner!=null){
                            Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                            if(clearMethod!=null)
                                clearMethod.invoke(cleaner);
                        }
                    }
                }
            }catch(Exception e){
                unmapHackSupported = false;
                //TODO exception handling
                //Utils.LOG.log(Level.WARNING, "ByteBufferVol Unmap failed", e);
            }
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

        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;
        protected final java.io.RandomAccessFile raf;


        public MappedFileVol(File file, boolean readOnly, int sliceShift, int sizeIncrement) {
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
                if(CC.PARANOID && ! ((offset& sliceSizeModMask)==0))
                    throw new AssertionError();
                if(CC.PARANOID && ! (offset>=0))
                    throw new AssertionError();
                ByteBuffer ret = fileChannel.map(mapMode,offset, sliceSize);
                if(mapMode == FileChannel.MapMode.READ_ONLY) {
                    ret = ret.asReadOnlyBuffer();
                }
                return ret;
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }


        @Override
        public void deleteFile() {
            file.delete();
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
            return useDirectBuffer?
                    ByteBuffer.allocateDirect(sliceSize):
                    ByteBuffer.allocate(sliceSize);
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

        @Override public void deleteFile() {}

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

        protected final File file;
        protected final int sliceSize;
        protected RandomAccessFile raf;
        protected FileChannel channel;
        protected final boolean readOnly;

        protected volatile long size;
        protected final Object growLock = new Object();

        public FileChannelVol(File file, boolean readOnly, int sliceShift, int sizeIncrement){
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

            if(offset>size)synchronized (growLock){
                try {
                    channel.truncate(offset);
                    size = offset;
                }catch(ClosedByInterruptException e){
                    throw new DBException.VolumeClosedByInterrupt(e);
                }catch(ClosedChannelException e){
                    throw new DBException.VolumeClosed(e);
                } catch (IOException e) {
                    throw new DBException.VolumeIOError(e);
                }
            }
        }

        @Override
        public void truncate(long size) {
            synchronized (growLock){
                try {
                    this.size = size;
                    channel.truncate(size);
                }catch(ClosedByInterruptException e){
                    throw new DBException.VolumeClosedByInterrupt(e);
                }catch(ClosedChannelException e){
                    throw new DBException.VolumeClosed(e);
                } catch (IOException e) {
                    throw new DBException.VolumeIOError(e);
                }
            }
        }

        protected void writeFully(long offset, ByteBuffer buf) throws IOException {
            int remaining = buf.limit()-buf.position();
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+remaining){
                new IOException("VOL STACK:").printStackTrace();
            }
            while(remaining>0){
                int write = channel.write(buf, offset);
                if(write<0) throw new EOFException();
                remaining-=write;
            }
        }


        @Override
        public void putLong(long offset, long value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+8){
                new IOException("VOL STACK:").printStackTrace();
            }

            try{
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putLong(0, value);
                writeFully(offset, buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void putInt(long offset, int value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+4){
                new IOException("VOL STACK:").printStackTrace();
            }

            try{
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.putInt(0, value);
                writeFully(offset, buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void putByte(long offset, byte value) {
            if(CC.VOLUME_PRINT_STACK_AT_OFFSET!=0 && CC.VOLUME_PRINT_STACK_AT_OFFSET>=offset && CC.VOLUME_PRINT_STACK_AT_OFFSET <= offset+1){
                new IOException("VOL STACK:").printStackTrace();
            }

            try{
                ByteBuffer buf = ByteBuffer.allocate(1);
                buf.put(0, value);
                writeFully(offset, buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            try{
                ByteBuffer buf = ByteBuffer.wrap(src,srcPos, srcSize);
                writeFully(offset, buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            try{
                writeFully(offset,buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        protected void readFully(long offset, ByteBuffer buf) throws IOException {
            int remaining = buf.limit()-buf.position();
            while(remaining>0){
                int read = channel.read(buf, offset);
                if(read<0)
                    throw new EOFException();
                remaining-=read;
            }
        }

        @Override
        public long getLong(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(8);
                readFully(offset,buf);
                return buf.getLong(0);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public int getInt(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(4);
                readFully(offset,buf);
                return buf.getInt(0);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public byte getByte(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(1);
                readFully(offset,buf);
                return buf.get(0);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(size);
                readFully(offset,buf);
                return new DataIO.DataInputByteBuffer(buf,0);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void getData(long offset, byte[] bytes, int bytesPos, int size) {
            try{
                ByteBuffer buf = ByteBuffer.wrap(bytes,bytesPos,size);
                readFully(offset,buf);
            }catch(ClosedByInterruptException e){
                throw new DBException.VolumeClosedByInterrupt(e);
            }catch(ClosedChannelException e){
                throw new DBException.VolumeClosed(e);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }
        }

        @Override
        public void close() {
            try{
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
        public void deleteFile() {
            file.delete();
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
                throw new IOError(e);
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
            try{
                //check second time
                if(slicePos< slices.length)
                    return;

                int oldSize = slices.length;
                byte[][] slices2 = slices;

                slices2 = Arrays.copyOf(slices2, Math.max(slicePos+1, slices2.length + slices2.length/1000));

                for(int pos=oldSize;pos<slices2.length;pos++) {
                    slices2[pos]=new byte[sliceSize];
                }


                slices = slices2;
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
            buf[pos++] = (byte) (0xff & (v >> 56));
            buf[pos++] = (byte) (0xff & (v >> 48));
            buf[pos++] = (byte) (0xff & (v >> 40));
            buf[pos++] = (byte) (0xff & (v >> 32));
            buf[pos++] = (byte) (0xff & (v >> 24));
            buf[pos++] = (byte) (0xff & (v >> 16));
            buf[pos++] = (byte) (0xff & (v >> 8));
            buf[pos] = (byte) (0xff & (v));
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
        public void transferInto(long inputOffset, Volume target, long targetOffset, int size) {
            int pos = (int) (inputOffset & sliceSizeModMask);
            byte[] buf = slices[((int) (inputOffset >>> sliceShift))];

            target.putData(targetOffset,buf,pos, size);
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            if(CC.PARANOID && (startOffset >>> sliceShift) != ((endOffset-1) >>> sliceShift))
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

            final int end = pos + 8;
            long ret = 0;
            for (; pos < end; pos++) {
                ret = (ret << 8) | (buf[pos] & 0xFF);
            }
            return ret;
        }



        @Override
        public int getInt(long offset) {
            int pos = (int) (offset & sliceSizeModMask);
            byte[] buf = slices[((int) (offset >>> sliceShift))];

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
        public void deleteFile() {

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
        public File getFile() {
            return null;
        }

    }


    public static class ReadOnly extends Volume{

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
        public void getData(long offset, byte[] bytes, int bytesPos, int size) {
            vol.getData(offset,bytes,bytesPos,size);
        }

        @Override
        public void close() {
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
        public File getFile() {
            return vol.getFile();
        }

        @Override
        public void clear(long startOffset, long endOffset) {
            throw new IllegalAccessError("read-only");
        }
    }


    public static final class RandomAccessFileVol extends Volume{

        protected final File file;
        protected final RandomAccessFile raf;

        public RandomAccessFileVol(File file, boolean readOnly) {
            this.file = file;
            try {
                this.raf = new RandomAccessFile(file,readOnly?"r":"w");
            } catch (IOException e) {
                throw new IOError(e);
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
                throw new IOError(e);
            }
        }

        @Override
        public synchronized void putLong(long offset, long value) {
            try {
                raf.seek(offset);
                raf.writeLong(value);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }


        @Override
        public synchronized  void putInt(long offset, int value) {
            try {
                raf.seek(offset);
                raf.writeInt(value);
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        public  synchronized void putByte(long offset, byte value) {
            try {
                raf.seek(offset);
                raf.writeByte(value);
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        public  synchronized void putData(long offset, byte[] src, int srcPos, int srcSize) {
            try {
                raf.seek(offset);
                raf.write(src,srcPos,srcSize);
            } catch (IOException e) {
                throw new IOError(e);
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
                throw new IOError(e);
            }
        }

        @Override
        public synchronized int getInt(long offset) {
            try {
                raf.seek(offset);
                return raf.readInt();
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        public synchronized byte getByte(long offset) {
            try {
                raf.seek(offset);
                return raf.readByte();
            } catch (IOException e) {
                throw new IOError(e);
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
                throw new IOError(e);
            }
        }

        @Override
        public synchronized void getData(long offset, byte[] bytes, int bytesPos, int size) {
            try {
                raf.seek(offset);
                raf.read(bytes,bytesPos,size);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void close() {
            try {
                raf.close();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void sync() {
            try {
                raf.getFD().sync();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public int sliceSize() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            try {
                return raf.length()==0;
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void deleteFile() {
            file.delete();
        }

        @Override
        public boolean isSliced() {
            return false;
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
                throw new IOError(e);
            }
        }
    }

    private static final byte[] CLEAR = new byte[1024];
}

