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
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

/**
 * MapDB abstraction over raw storage (file, disk partition, memory etc...).
 * <p/>
 * Implementations needs to be thread safe (especially
 'ensureAvailable') operation.
 * However updates do not have to be atomic, it is clients responsibility
 * to ensure two threads are not writing/reading into the same location.
 *
 * @author Jan Kotek
 */
public abstract class Volume {


    public static final int CHUNK_SHIFT = 30;

    public static final int CHUNK_SIZE = 1<< CHUNK_SHIFT;

    public static final int CHUNK_SIZE_MOD_MASK = CHUNK_SIZE -1;

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


    abstract public void putLong(final long offset, final long value);
    abstract public void putInt(long offset, int value);
    abstract public void putByte(final long offset, final byte value);

    abstract public void putData(final long offset, final byte[] src, int srcPos, int srcSize);
    abstract public void putData(final long offset, final ByteBuffer buf);

    abstract public long getLong(final long offset);
    abstract public int getInt(long offset);
    abstract public byte getByte(final long offset);



    abstract public DataInput2 getDataInput(final long offset, final int size);

    abstract public void close();

    abstract public void sync();

    public abstract boolean isEmpty();

    public abstract void deleteFile();

    public abstract boolean isSliced();


    public final void putUnsignedShort(final long offset, final int value){
        putByte(offset, (byte) (value>>8));
        putByte(offset+1, (byte) (value));
    }

    public final int getUnsignedShort(long offset) {
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
        assert(value>=0 && (value>>>6*8)==0): "value does not fit";
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
        assert(value>=0):"negative value";

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
     * Factory which creates two/three volumes used by each MapDB Storage Engine
     */
    public static interface Factory {
        Volume createIndexVolume();
        Volume createPhysVolume();
        Volume createTransLogVolume();
    }

    public static Volume volumeForFile(File f, boolean useRandomAccessFile, boolean readOnly, long sizeLimit, boolean fullChunkAllocation) {
        return useRandomAccessFile ?
                new FileChannelVol(f, readOnly,sizeLimit, fullChunkAllocation):
                new MappedFileVol(f, readOnly,sizeLimit,fullChunkAllocation);
    }


    public static Factory fileFactory(final boolean readOnly, final int rafMode, final File indexFile, final long sizeLimit,
                                      final boolean fullChunkAllocation){
        return fileFactory(readOnly, rafMode, sizeLimit, fullChunkAllocation, indexFile,
                new File(indexFile.getPath() + StoreDirect.DATA_FILE_EXT),
                new File(indexFile.getPath() + StoreWAL.TRANS_LOG_FILE_EXT));
    }

    public static Factory fileFactory(final boolean readOnly,
                                      final int rafMode,
                                      final long sizeLimit,
                                      final boolean fullChunkAllocation,
                                      final File indexFile,
                                      final File physFile,
                                      final File transLogFile) {
        return new Factory() {
            @Override
            public Volume createIndexVolume() {
                return volumeForFile(indexFile, rafMode>1, readOnly, sizeLimit, fullChunkAllocation);
            }

            @Override
            public Volume createPhysVolume() {
                return volumeForFile(physFile, rafMode>0, readOnly, sizeLimit, fullChunkAllocation);
            }

            @Override
            public Volume createTransLogVolume() {
                if(readOnly && !transLogFile.exists())
                    return null;
                return volumeForFile(transLogFile, rafMode>0, readOnly, sizeLimit, fullChunkAllocation);
            }
        };
    }


    public static Factory memoryFactory(final boolean useDirectBuffer, final long sizeLimit, final boolean fullChunkAllocation) {
        return new Factory() {

            @Override public synchronized  Volume createIndexVolume() {
                return new MemoryVol(useDirectBuffer, sizeLimit,fullChunkAllocation);
            }

            @Override public synchronized Volume createPhysVolume() {
                return new MemoryVol(useDirectBuffer, sizeLimit,fullChunkAllocation);
            }

            @Override public synchronized Volume createTransLogVolume() {
                return new MemoryVol(useDirectBuffer, sizeLimit,fullChunkAllocation);
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
        protected final boolean fullChunkAllocation;

        protected volatile ByteBuffer[] chunks;
        protected final boolean readOnly;

        protected ByteBufferVol(boolean readOnly, long sizeLimit, boolean fullChunkAllocation) {
            this.readOnly = readOnly;
            this.sizeLimit = sizeLimit;
            this.fullChunkAllocation = fullChunkAllocation;
            this.hasLimit = sizeLimit>0;
        }


        @Override
        public final boolean tryAvailable(long offset) {
            if(hasLimit&&offset>sizeLimit) return false;

            int chunkPos = (int) (offset >>> CHUNK_SHIFT);

            //check for most common case, this is already mapped
            if(chunkPos< chunks.length && chunks[chunkPos]!=null &&
                    chunks[chunkPos].capacity()>=(offset&Volume.CHUNK_SIZE_MOD_MASK)){
                return true;
            }

            growLock.lock();
            try{
                //check second time
                if(chunkPos< chunks.length && chunks[chunkPos]!=null &&
                        chunks[chunkPos].capacity()>=(offset&Volume.CHUNK_SIZE_MOD_MASK))
                    return true;

                ByteBuffer[] chunks2 = chunks;

                //grow array if necessary
                if(chunkPos>=chunks2.length){
                    chunks2 = Arrays.copyOf(chunks2, Math.max(chunkPos+1, chunks2.length * 2));
                }


                //just remap file buffer
                if( chunks2[chunkPos] == null){
                    //make sure previous buffer is fully expanded
                    if(chunkPos>0){
                        ByteBuffer oldPrev = chunks2[chunkPos-1];
                        if(oldPrev == null || oldPrev.capacity()!= CHUNK_SIZE){
                            chunks2[chunkPos-1]  = makeNewBuffer(1L*chunkPos* CHUNK_SIZE -1,chunks2);
                        }
                    }
                }


                ByteBuffer newChunk = makeNewBuffer(offset, chunks2);
                if(readOnly)
                    newChunk = newChunk.asReadOnlyBuffer();

                chunks2[chunkPos] = newChunk;

                chunks = chunks2;
            }finally{
                growLock.unlock();
            }
            return true;
        }

        protected abstract ByteBuffer makeNewBuffer(long offset, ByteBuffer[] buffers2);

        @Override public final void putLong(final long offset, final long value) {
            chunks[(int)(offset >>> CHUNK_SHIFT)].putLong((int) (offset & Volume.CHUNK_SIZE_MOD_MASK), value);
        }

        @Override public final void putInt(final long offset, final int value) {
            chunks[(int)(offset >>> CHUNK_SHIFT)].putInt((int) (offset & Volume.CHUNK_SIZE_MOD_MASK), value);
        }


        @Override public final void putByte(final long offset, final byte value) {
            chunks[(int)(offset >>> CHUNK_SHIFT)].put((int) (offset & Volume.CHUNK_SIZE_MOD_MASK), value);
        }



        @Override public void putData(final long offset, final byte[] src, int srcPos, int srcSize){
            final ByteBuffer b1 = chunks[(int)(offset >>> CHUNK_SHIFT)].duplicate();
            final int bufPos = (int) (offset&Volume.CHUNK_SIZE_MOD_MASK);

            b1.position(bufPos);
            b1.put(src, srcPos, srcSize);
        }

        @Override public final void putData(final long offset, final ByteBuffer buf) {
            final ByteBuffer b1 = chunks[(int)(offset >>> CHUNK_SHIFT)].duplicate();
            final int bufPos = (int) (offset&Volume.CHUNK_SIZE_MOD_MASK);
            //no overlap, so just write the value
            b1.position(bufPos);
            b1.put(buf);
        }

        @Override final public long getLong(long offset) {
            return chunks[(int)(offset >>> CHUNK_SHIFT)].getLong((int) (offset&Volume.CHUNK_SIZE_MOD_MASK));
        }

        @Override final public int getInt(long offset) {
            return chunks[(int)(offset >>> CHUNK_SHIFT)].getInt((int) (offset&Volume.CHUNK_SIZE_MOD_MASK));
        }


        @Override public final byte getByte(long offset) {
            return chunks[(int)(offset >>> CHUNK_SHIFT)].get((int) (offset&Volume.CHUNK_SIZE_MOD_MASK));
        }


        @Override
        public final DataInput2 getDataInput(long offset, int size) {
            return new DataInput2(chunks[(int)(offset >>> CHUNK_SHIFT)], (int) (offset&Volume.CHUNK_SIZE_MOD_MASK));
        }

        @Override
        public boolean isEmpty() {
            return chunks[0]==null || chunks[0].capacity()==0;
        }

        @Override
        public boolean isSliced(){
            return true;
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
                        Object cleaner = cleanerMethod.invoke(b, new Object[0]);
                        if(cleaner!=null){
                            Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
                            if(cleanerMethod!=null)
                                clearMethod.invoke(cleaner, new Object[0]);
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


    }

    public static final class MappedFileVol extends ByteBufferVol {

        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;
        protected final java.io.RandomAccessFile raf;


        protected ReferenceQueue<MappedByteBuffer> unreleasedQueue = new ReferenceQueue<MappedByteBuffer>();
        protected Set<Reference<MappedByteBuffer>> unreleasedChunks = new LinkedHashSet<Reference<MappedByteBuffer>>();

        static final int BUF_SIZE_INC = 1024*1024;

        public MappedFileVol(File file, boolean readOnly, long sizeLimit, boolean fullChunkAllocation) {
            super(readOnly, sizeLimit, fullChunkAllocation);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                this.raf = new java.io.RandomAccessFile(file, readOnly?"r":"rw");
                this.fileChannel = raf.getChannel();

                final long fileSize = fileChannel.size();
                if(fileSize>0){
                    //map existing data
                    chunks = new ByteBuffer[(int) (1+(fileSize>>> CHUNK_SHIFT))];
                    for(int i=0;i<=fileSize>>> CHUNK_SHIFT;i++){
                        final long offset = 1L* CHUNK_SIZE *i;
                        chunks[i] = fileChannel.map(mapMode, offset, Math.min(CHUNK_SIZE, fileSize-offset));
                        if(mapMode == FileChannel.MapMode.READ_ONLY)
                            chunks[i] = chunks[i].asReadOnlyBuffer();
                        //TODO what if 'fileSize % 8 != 0'?
                    }
                }else{
                    chunks = new ByteBuffer[1];
//                    chunks[0] = fileChannel.map(mapMode, 0, INITIAL_SIZE);
//                    if(mapMode == FileChannel.MapMode.READ_ONLY)
//                        chunks[0] = chunks[0].asReadOnlyBuffer();

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
                if(!readOnly)
                    sync();

                for(Reference<MappedByteBuffer> rb: unreleasedChunks){
                    MappedByteBuffer b = rb.get();
                    if(b==null) continue;
                    unmap(b);
                }

                for(ByteBuffer b: chunks){
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        unmap((MappedByteBuffer) b);
                    }
                }

                unreleasedChunks = null;
                unreleasedQueue = null;

                chunks = null;

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
                //clear GC references
                for(Reference ref=unreleasedQueue.poll();ref!=null; ref=unreleasedQueue.poll()){
                    unreleasedChunks.remove(ref);
                }

                for(Reference<MappedByteBuffer> rb: unreleasedChunks){
                    MappedByteBuffer b = rb.get();
                    if(b==null) continue;
                    b.force();
                }
                for(ByteBuffer b: chunks){
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        ((MappedByteBuffer) b).force();
                    }
                }

            }finally{
                growLock.unlock();
            }

        }

        @Override
        public boolean isEmpty() {
            return chunks[0]==null || chunks[0].capacity()==0;
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
        protected ByteBuffer makeNewBuffer(long offset, ByteBuffer[] buffers2) {
            assert (growLock.isHeldByCurrentThread());
            try {
                //create new chunk
                long newChunkSize =  offset&Volume.CHUNK_SIZE_MOD_MASK;
                int round = offset<BUF_SIZE_INC? BUF_SIZE_INC/16 : BUF_SIZE_INC;
                if(fullChunkAllocation) round = CHUNK_SIZE;

                //round newBufSize to multiple of BUF_SIZE_INC
                long rest = newChunkSize%round;
                if(rest!=0)
                    newChunkSize += round-rest;


                final MappedByteBuffer buf =  fileChannel.map( mapMode, offset - (offset&Volume.CHUNK_SIZE_MOD_MASK), newChunkSize );

                unreleasedChunks.add(new WeakReference<MappedByteBuffer>(buf, unreleasedQueue));

                for(Reference ref=unreleasedQueue.poll();ref!=null; ref=unreleasedQueue.poll()){
                    unreleasedChunks.remove(ref);
                }

                return buf;
            } catch (IOException e) {
                if(e.getCause()!=null && e.getCause() instanceof OutOfMemoryError){
                    throw new RuntimeException("File could not be mapped to memory, common problem on 32bit JVM. Use `DBMaker.newRandomAccessFileDB()` as workaround",e);
                }

                throw new IOError(e);
            }
        }
    }

    public static final class MemoryVol extends ByteBufferVol {
        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString()+",direct="+useDirectBuffer;
        }

        public MemoryVol(final boolean useDirectBuffer, final long sizeLimit, final boolean fullChunkAllocation) {
            super(false,sizeLimit, fullChunkAllocation);
            this.useDirectBuffer = useDirectBuffer;
//            ByteBuffer b0 = useDirectBuffer?
//                    ByteBuffer.allocateDirect(INITIAL_SIZE) :
//                    ByteBuffer.allocate(INITIAL_SIZE);
//            chunks = new ByteBuffer[]{b0};
            chunks =new ByteBuffer[1];
        }

        @Override protected ByteBuffer makeNewBuffer(long offset, ByteBuffer[] buffers2) {
            int curSize = (int) (offset & Volume.CHUNK_SIZE_MOD_MASK);
            int newBufSize = 1 << (32 - Integer.numberOfLeadingZeros(curSize - 1)); //next pow of two
            if(fullChunkAllocation && newBufSize% CHUNK_SIZE !=0){
                //round newBufSize to multiple of BUF_SIZE_INC
                newBufSize += CHUNK_SIZE -newBufSize% CHUNK_SIZE;
            }
            //double size of existing in-memory-buffer
            ByteBuffer newBuf = useDirectBuffer?
                    ByteBuffer.allocateDirect(newBufSize):
                    ByteBuffer.allocate(newBufSize);
            final int buffersPos = (int) (offset >>> CHUNK_SHIFT);
            ByteBuffer oldBuffer = buffers2[buffersPos];
            if(oldBuffer!=null){
                //copy old buffer if it exists
                oldBuffer = oldBuffer.duplicate();
                oldBuffer.rewind();
                newBuf.put(oldBuffer);
            }
            return newBuf;
        }

        @Override public void close() {
            growLock.lock();
            try{
                for(ByteBuffer b: chunks){
                    if(b!=null && (b instanceof MappedByteBuffer)){
                        unmap((MappedByteBuffer)b);
                    }
                }
                chunks = null;
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
        protected FileChannel channel;
        protected final boolean readOnly;
        protected final long sizeLimit;
        protected final boolean hasLimit;
        protected final boolean fullChunkAllocation;

        protected volatile long size;
        protected Object growLock = new Object();

        public FileChannelVol(File file, boolean readOnly, long sizeLimit, boolean fullChunkAllocation){
            this.file = file;
            this.readOnly = readOnly;
            this.sizeLimit = sizeLimit;
            this.fullChunkAllocation = fullChunkAllocation;
            this.hasLimit = sizeLimit>0;
            try {
                channel = new RandomAccessFile(file, readOnly?"r":"rw").getChannel();
                size = channel.size();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public boolean tryAvailable(long offset) {
            if(hasLimit && offset>sizeLimit) return false;
            if(fullChunkAllocation && offset% CHUNK_SIZE !=0)
                offset += CHUNK_SIZE - offset% CHUNK_SIZE;

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
            assert(value>=0 && (value>>>6*8)==0): "value does not fit";

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
        public DataInput2 getDataInput(long offset, int size) {
            try{
                ByteBuffer buf = ByteBuffer.allocate(size);
                readFully(offset,buf);
                return new DataInput2(buf,0);
            }catch(IOException e){
                throw new IOError(e);
            }
        }

        @Override
        public void close() {
            try{
                channel.close();
                channel = null;
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
        int bufSize = 1024*64;

        for(long offset=0;offset<size;offset+=bufSize){
            int bb = (int) Math.min(bufSize, size-offset);
            DataInput2 input = from.getDataInput(offset, bb);
            ByteBuffer buf = input.buf.duplicate();
            buf.position(input.pos);
            buf.limit(input.pos+bb);
            to.ensureAvailable(offset+bb);
            to.putData(offset,buf);
        }
    }
}

