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
     * @throws IOError if Volume can not be expanded beyond given offset
     * @param offset
     */
    public void ensureAvailable(final long offset){
        if(!tryAvailable(offset))
            throw new IOError(new IOException("no free space to expand Volume"));
    }


    abstract public boolean tryAvailable(final long offset);

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

    /**
     * Reads a long from the indicated position
     */
    public long getSixLong(long pos) {
        return
                ((long) (getByte(pos + 0) & 0xff) << 40) |
                        ((long) (getByte(pos + 1) & 0xff) << 32) |
                        ((long) (getByte(pos + 2) & 0xff) << 24) |
                        ((long) (getByte(pos + 3) & 0xff) << 16) |
                        ((long) (getByte(pos + 4) & 0xff) << 8) |
                        ((long) (getByte(pos + 5) & 0xff) << 0);
    }

    /**
     * Writes a long to the indicated position
     */
    public void putSixLong(long pos, long value) {
        if(CC.PARANOID && ! (value>=0 && (value>>>6*8)==0))
            throw new AssertionError("value does not fit");
        //TODO read/write as integer+short, might be faster
        putByte(pos + 0, (byte) (0xff & (value >> 40)));
        putByte(pos + 1, (byte) (0xff & (value >> 32)));
        putByte(pos + 2, (byte) (0xff & (value >> 24)));
        putByte(pos + 3, (byte) (0xff & (value >> 16)));
        putByte(pos + 4, (byte) (0xff & (value >> 8)));
        putByte(pos + 5, (byte) (0xff & (value >> 0)));

    }

    /**
     * Writes packed long at given position and returns number of bytes used.
     */
    public int putPackedLong(long pos, long value) {
        if(CC.PARANOID && ! (value>=0))
            throw new AssertionError("negative value");

        int ret = 0;

        while ((value & ~0x7FL) != 0) {
            putUnsignedByte(pos+(ret++), (((int) value & 0x7F) | 0x80));
            value >>>= 7;
        }
        putUnsignedByte(pos + (ret++), (byte) value);
        return ret;
    }



    /** returns underlying file if it exists */
    abstract public File getFile();

    public long getPackedLong(long pos){
        //TODO unrolled version?
        long result = 0;
        for (int offset = 0; offset < 64; offset += 7) {
            long b = getUnsignedByte(pos++);
            result |= (b & 0x7F) << offset;
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new AssertionError("Malformed long.");
    }

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
            throw new IOError(e);
        }
        target.putData(targetOffset,data,0,size);
    }


    public static Volume volumeForFile(File f, boolean useRandomAccessFile, boolean readOnly, long sizeLimit, int sliceShift, int sizeIncrement) {
        return useRandomAccessFile ?
                new FileChannelVol(f, readOnly,sizeLimit, sliceShift, sizeIncrement):
                new MappedFileVol(f, readOnly,sizeLimit,sliceShift, sizeIncrement);
    }


    public static Fun.Function1<Volume,String> fileFactory(){
        return fileFactory(false,false,0,CC.VOLUME_SLICE_SHIFT,0);
    }

    public static Fun.Function1<Volume,String> fileFactory(
                                      final boolean useRandomAccessFile,
                                      final boolean readOnly,
                                      final long sizeLimit,
                                      final int sliceShift,
                                      final int sizeIncrement) {
        return new Fun.Function1<Volume, String>() {
            @Override
            public Volume run(String file) {
                return volumeForFile(new File(file), useRandomAccessFile,
                        readOnly, sizeLimit, sliceShift, sizeIncrement);
            }
        };
    }


    public static Fun.Function1<Volume,String> memoryFactory(){
        return memoryFactory(false,0L,CC.VOLUME_SLICE_SHIFT);
    }

    public static Fun.Function1<Volume,String> memoryFactory(
            final boolean useDirectBuffer, final long sizeLimit, final int sliceShift) {
        return new Fun.Function1<Volume,String>() {

            @Override
            public Volume run(String s) {
                return useDirectBuffer?
                        new MemoryVol(true, sizeLimit, sliceShift):
                        new ByteArrayVol(sizeLimit, sliceShift);
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

        protected final long sizeLimit;
        protected final boolean hasLimit;
        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected volatile ByteBuffer[] slices = new ByteBuffer[0];
        protected final boolean readOnly;

        protected ByteBufferVol(boolean readOnly, long sizeLimit, int sliceShift) {
            this.readOnly = readOnly;
            this.sizeLimit = sizeLimit;
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;

            this.hasLimit = sizeLimit>0;
        }


        @Override
        public final boolean tryAvailable(long offset) {
            if (hasLimit && offset > sizeLimit) return false;

            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return true;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos< slices.length)
                    return true;

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
            return true;
        }

        protected abstract ByteBuffer makeNewBuffer(long offset);

        @Override public final void putLong(final long offset, final long value) {
            slices[(int)(offset >>> sliceShift)].putLong((int) (offset & sliceSizeModMask), value);
        }

        @Override public final void putInt(final long offset, final int value) {
            slices[(int)(offset >>> sliceShift)].putInt((int) (offset & sliceSizeModMask), value);
        }


        @Override public final void putByte(final long offset, final byte value) {
            slices[(int)(offset >>> sliceShift)].put((int) (offset & sliceSizeModMask), value);
        }



        @Override public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
            final ByteBuffer b1 = slices[(int)(offset >>> sliceShift)].duplicate();
            final int bufPos = (int) (offset& sliceSizeModMask);

            b1.position(bufPos);
            b1.put(src, srcPos, srcSize);
        }

        @Override public final void putData(final long offset, final ByteBuffer buf) {
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


        public MappedFileVol(File file, boolean readOnly, long sizeLimit, int sliceShift, int sizeIncrement) {
            super(readOnly, sizeLimit, sliceShift);
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
                throw new IOError(e);
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
                throw new IOError(e);
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
                throw new IOError(e);
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
                    throw new IOError(e);
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

        public MemoryVol(final boolean useDirectBuffer, final long sizeLimit, final int sliceShift) {
            super(false,sizeLimit, sliceShift);
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
        protected final long sizeLimit;
        protected final boolean hasLimit;

        protected volatile long size;
        protected final Object growLock = new Object();

        public FileChannelVol(File file, boolean readOnly, long sizeLimit, int sliceShift, int sizeIncrement){
            this.file = file;
            this.readOnly = readOnly;
            this.sizeLimit = sizeLimit;
            this.hasLimit = sizeLimit>0;
            this.sliceSize = 1<<sliceShift;
            try {
                checkFolder(file,readOnly);
                if(readOnly && !file.exists()){
                    raf = null;
                    channel = null;
                    size = 0;
                }else {
                    raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
                    channel = raf.getChannel();
                    size = channel.size();
                }
            } catch (IOException e) {
                throw new IOError(e);
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
        public boolean tryAvailable(long offset) {
            if(hasLimit && offset>sizeLimit) return false;
            if(offset% sliceSize !=0)
                offset += sliceSize - offset% sliceSize; //round up to multiply of slice size

            if(offset>size)synchronized (growLock){
                try {
                    channel.truncate(offset);
                    size = offset;
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }
            return true;
        }

        @Override
        public void truncate(long size) {
            synchronized (growLock){
                try {
                    this.size = size;
                    channel.truncate(size);
                } catch (IOException e) {
                    throw new IOError(e);
                }
            }

        }

        protected void writeFully(long offset, ByteBuffer buf) throws IOException {
            int remaining = buf.limit()-buf.position();
            while(remaining>0){
                int write = channel.write(buf, offset);
                if(write<0) throw new EOFException();
                remaining-=write;
            }
        }

        @Override
        public final void putSixLong(long offset, long value) {
            if(CC.PARANOID && ! (value>=0 && (value>>>6*8)==0))
                throw new AssertionError("value does not fit");

            try{

                ByteBuffer buf = ByteBuffer.allocate(6);
                buf.put(0, (byte) (0xff & (value >> 40)));
                buf.put(1, (byte) (0xff & (value >> 32)));
                buf.put(2, (byte) (0xff & (value >> 24)));
                buf.put(3, (byte) (0xff & (value >> 16)));
                buf.put(4, (byte) (0xff & (value >> 8)));
                buf.put(5, (byte) (0xff & (value >> 0)));

                writeFully(offset, buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void putLong(long offset, long value) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(8);
                buf.putLong(0, value);
                writeFully(offset, buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void putInt(long offset, int value) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(4);
                buf.putInt(0, value);
                writeFully(offset, buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void putByte(long offset, byte value) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(1);
                buf.put(0, value);
                writeFully(offset, buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void putData(long offset, byte[] src, int srcPos, int srcSize) {
            try{
                ByteBuffer buf = ByteBuffer.wrap(src,srcPos, srcSize);
                writeFully(offset, buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void putData(long offset, ByteBuffer buf) {
            try{
                writeFully(offset,buf);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        protected void readFully(long offset, ByteBuffer buf) throws IOException {
            int remaining = buf.limit()-buf.position();
            while(remaining>0){
                int read = channel.read(buf, offset);
                if(read<0) throw new EOFException();
                remaining-=read;
            }
        }

        @Override
        public final long getSixLong(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(6);
                readFully(offset,buf);
                return ((long) (buf.get(0) & 0xff) << 40) |
                        ((long) (buf.get(1) & 0xff) << 32) |
                        ((long) (buf.get(2) & 0xff) << 24) |
                        ((long) (buf.get(3) & 0xff) << 16) |
                        ((long) (buf.get(4) & 0xff) << 8) |
                        ((long) (buf.get(5) & 0xff) << 0);

            }catch(IOException e){
                throw new IOError(e);
            }
        }


        @Override
        public long getLong(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(8);
                readFully(offset,buf);
                return buf.getLong(0);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public int getInt(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(4);
                readFully(offset,buf);
                return buf.getInt(0);
            }catch(IOException e){
                throw new IOError(e);
            }

        }

        @Override
        public byte getByte(long offset) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(1);
                readFully(offset,buf);
                return buf.get(0);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public DataIO.DataInputByteBuffer getDataInput(long offset, int size) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(size);
                readFully(offset,buf);
                return new DataIO.DataInputByteBuffer(buf,0);
            }catch(IOException e){
                throw new IOError(e);
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
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void sync() {
            try{
                channel.force(true);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public boolean isEmpty() {
            try {
                return channel==null || channel.size()==0;
            } catch (IOException e) {
                throw new IOError(e);
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

        protected final long sizeLimit;
        protected final boolean hasLimit;
        protected final int sliceShift;
        protected final int sliceSizeModMask;
        protected final int sliceSize;

        protected volatile byte[][] slices = new byte[0][];

        protected ByteArrayVol(long sizeLimit, int sliceShift) {
            this.sizeLimit = sizeLimit;
            this.sliceShift = sliceShift;
            this.sliceSize = 1<< sliceShift;
            this.sliceSizeModMask = sliceSize -1;

            this.hasLimit = sizeLimit>0;
        }


        @Override
        public final boolean tryAvailable(long offset) {
            if (hasLimit && offset > sizeLimit) return false;

            int slicePos = (int) (offset >>> sliceShift);

            //check for most common case, this is already mapped
            if (slicePos < slices.length){
                return true;
            }

            growLock.lock();
            try{
                //check second time
                if(slicePos< slices.length)
                    return true;

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
            return true;
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
            buf[pos++] = (byte) (0xff & (v));
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
        public boolean tryAvailable(long offset) {
            return vol.tryAvailable(offset);
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
    }
}

