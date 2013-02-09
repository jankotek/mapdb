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

import java.io.EOFException;
import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.logging.Level;

/**
 * MapDB abstraction over raw storage (file, disk partition, memory etc...)
 *
 * @author Jan Kotek
 */
public abstract class Volume {

    public static final int BUF_SIZE = 1<<30;
    public static final int INITIAL_SIZE = 1024*32;

    abstract public void ensureAvailable(final long offset);

    abstract public void putLong(final long offset, final long value);

    abstract public void putByte(final long offset, final byte value);

    abstract public void putData(final long offset, final byte[] value, int size);

    abstract public void putData(final long offset, final java.nio.ByteBuffer buf, final int size);

    abstract public long getLong(final long offset);

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

    /** returns underlying file if it exists */
    abstract public File getFile();


    /**
     * Factory which creates two/three volumes used by each MapDB Storage Engine
     */
    public static interface Factory {
        Volume createIndexVolume();
        Volume createPhysVolume();
        Volume createTransLogVolume();
    }

    public static Volume volumeForFile(File f, boolean useRandomAccessFile, boolean readOnly) {
        return useRandomAccessFile ?
                new Volume.RandomAccessFile(f, readOnly):
                new Volume.MappedFile(f, readOnly);
    }


    public static Factory fileFactory(final boolean readOnly, final boolean RAF, final File indexFile){
        return fileFactory(readOnly, RAF, indexFile,
                new File(indexFile.getPath() + Storage.DATA_FILE_EXT),
                new File(indexFile.getPath() + StorageJournaled.TRANS_LOG_FILE_EXT));
    }

    public static Factory fileFactory(final boolean readOnly,
                                      final boolean RAF,
                                      final File indexFile,
                                      final File physFile,
                                      final File transLogFile) {
        return new Factory() {
            @Override
            public Volume createIndexVolume() {
                return volumeForFile(indexFile, RAF, readOnly);
            }

            @Override
            public Volume createPhysVolume() {
                return volumeForFile(physFile, RAF, readOnly);
            }

            @Override
            public Volume createTransLogVolume() {
                return volumeForFile(transLogFile, RAF, readOnly);
            }
        };
    }

    public abstract int getInt(long offset);
    public abstract void putInt(long offset, int value);

    public static Factory memoryFactory(final boolean useDirectBuffer) {
        return new Factory() {

            @Override public Volume createIndexVolume() {
                return new Memory(useDirectBuffer);
            }

            @Override public Volume createPhysVolume() {
                return new Memory(useDirectBuffer);
            }

            @Override public Volume createTransLogVolume() {
                return new Memory(useDirectBuffer);
            }
        };
    }


    /**
     * Abstract Volume over bunch of ByteBuffers
     * It leaves ByteBuffer details (allocation, disposal) on subclasses.
     * Most methods are final for better performance (JIT compiler can inline those).
     */
    abstract static public class ByteBuffer extends Volume{



        protected java.nio.ByteBuffer[] buffers;
        protected final boolean readOnly;

        protected ByteBuffer(boolean readOnly) {
            this.readOnly = readOnly;
        }


        @Override
        public final void ensureAvailable(long offset) {
            int buffersPos = (int) (offset/ BUF_SIZE);

            //check for most common case, this is already mapped
            if(buffersPos<buffers.length && buffers[buffersPos]!=null &&
                    buffers[buffersPos].capacity()>=offset% BUF_SIZE)
                return;

            //grow array if necessary
            if(buffersPos>=buffers.length){
                buffers = Arrays.copyOf(buffers, Math.max(buffersPos, buffers.length * 2));
            }

            //just remap file buffer

            java.nio.ByteBuffer newBuf = makeNewBuffer(offset);
            if(readOnly)
                newBuf = newBuf.asReadOnlyBuffer();

            buffers[buffersPos] = newBuf;
        }

        protected abstract java.nio.ByteBuffer makeNewBuffer(long offset);

        protected final java.nio.ByteBuffer internalByteBuffer(long offset) {
            final int pos = ((int) (offset / BUF_SIZE));
            if(pos>=buffers.length) throw new IOError(new EOFException());
            return buffers[pos];
        }



        @Override public final void putLong(final long offset, final long value) {
            internalByteBuffer(offset).putLong((int) (offset% BUF_SIZE), value);
        }

        @Override public final void putInt(final long offset, final int value) {
            internalByteBuffer(offset).putInt((int) (offset% BUF_SIZE), value);
        }


        @Override public final void putByte(final long offset, final byte value) {
            internalByteBuffer(offset).put((int) (offset % BUF_SIZE), value);
        }



        @Override public final void putData(final long offset, final byte[] value, final int size) {
            final java.nio.ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);

            synchronized (b1){
                b1.position(bufPos);
                b1.put(value, 0, size);
            }
        }

        @Override public final void putData(final long offset, final java.nio.ByteBuffer buf, final int size) {
            final java.nio.ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);
            //no overlap, so just write the value
            synchronized (b1){
                b1.position(bufPos);
                b1.put(buf);
            }
        }

        @Override final public long getLong(long offset) {
            try{
                return internalByteBuffer(offset).getLong((int) (offset% BUF_SIZE));
            }catch(IndexOutOfBoundsException e){
                throw new IOError(new EOFException());
            }
        }

        @Override final public int getInt(long offset) {
            try{
                return internalByteBuffer(offset).getInt((int) (offset% BUF_SIZE));
            }catch(IndexOutOfBoundsException e){
                throw new IOError(new EOFException());
            }
        }


        @Override public final byte getByte(long offset) {
            try{
                return internalByteBuffer(offset).get((int) (offset% BUF_SIZE));
            }catch(IndexOutOfBoundsException e){
                throw new IOError(new EOFException());
            }
        }


        @Override
        public final DataInput2 getDataInput(long offset, int size) {
            final java.nio.ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);
            return new DataInput2(b1, bufPos);
        }

        @Override
        public boolean isEmpty() {
            return buffers[0]==null || buffers[0].capacity()==0;
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
                Utils.LOG.log(Level.WARNING, "ByteBuffer Unmap failed", e);
            }
        }

        private static boolean unmapHackSupported = true;
        static{
            try{
                unmapHackSupported =
                        Class.forName("sun.nio.ch.DirectBuffer")!=null;
            }catch(Exception e){
                unmapHackSupported = false;
            }
        }


    }

    public static final class MappedFile extends ByteBuffer {

        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;
        protected final java.io.RandomAccessFile raf;

        static final int BUF_SIZE_INC = 1024*1024;

        public MappedFile(File file, boolean readOnly) {
            super(readOnly);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                this.raf = new java.io.RandomAccessFile(file, readOnly?"r":"rw");
                this.fileChannel = raf.getChannel();

                final long fileSize = fileChannel.size();
                if(fileSize>0){
                    //map existing data
                    buffers = new java.nio.ByteBuffer[(int) (1+fileSize/BUF_SIZE)];
                    for(int i=0;i<=fileSize/BUF_SIZE;i++){
                        final long offset = 1L*BUF_SIZE*i;
                        buffers[i] = fileChannel.map(mapMode, offset, Math.min(BUF_SIZE, fileSize-offset));
                        if(mapMode == FileChannel.MapMode.READ_ONLY)
                            buffers[i] = buffers[i].asReadOnlyBuffer();
                        //TODO what if 'fileSize % 8 != 0'?
                    }
                }else{
                    buffers = new java.nio.ByteBuffer[1];
                    buffers[0] = fileChannel.map(mapMode, 0, INITIAL_SIZE);
                    if(mapMode == FileChannel.MapMode.READ_ONLY)
                        buffers[0] = buffers[0].asReadOnlyBuffer();

                }
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public void close() {
            try {
                fileChannel.close();
                raf.close();
            } catch (IOException e) {
                throw new IOError(e);
            }
            if(!readOnly)
                sync();
            for(java.nio.ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    unmap((MappedByteBuffer) b);
                }
            }
            buffers = null;
        }

        @Override
        public void sync() {
            if(readOnly) return;
            for(java.nio.ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    ((MappedByteBuffer)b).force();
                }
            }
        }

        @Override
        public boolean isEmpty() {
            return buffers[0]==null || buffers[0].capacity()==0;
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
        protected java.nio.ByteBuffer makeNewBuffer(long offset) {
            try {
                long newBufSize =  offset% BUF_SIZE;
                newBufSize = newBufSize + newBufSize%BUF_SIZE_INC; //round to BUF_SIZE_INC
                return fileChannel.map(
                        mapMode,
                        offset - offset% BUF_SIZE, newBufSize );
            } catch (IOException e) {
                if(e.getCause()!=null && e.getCause() instanceof OutOfMemoryError){
                    throw new RuntimeException("File could not be mapped to memory, common problem on 32bit JVM. Use `DBMaker.newRandomAccessFileDB()` as workaround",e);
                }

                throw new IOError(e);
            }
        }
    }

    public static final class Memory extends ByteBuffer {
        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString()+",direct="+useDirectBuffer;
        }

        public Memory(boolean useDirectBuffer) {
            super(false);
            this.useDirectBuffer = useDirectBuffer;
            java.nio.ByteBuffer b0 = useDirectBuffer?
                    java.nio.ByteBuffer.allocateDirect(INITIAL_SIZE) :
                    java.nio.ByteBuffer.allocate(INITIAL_SIZE);
            buffers = new java.nio.ByteBuffer[]{b0};
        }

        @Override protected java.nio.ByteBuffer makeNewBuffer(long offset) {
            final int newBufSize = Utils.nextPowTwo((int) (offset % BUF_SIZE));
            //double size of existing in-memory-buffer
            java.nio.ByteBuffer newBuf = useDirectBuffer?
                    java.nio.ByteBuffer.allocateDirect(newBufSize):
                    java.nio.ByteBuffer.allocate(newBufSize);
            final int buffersPos = (int) (offset/ BUF_SIZE);
            final java.nio.ByteBuffer oldBuffer = buffers[buffersPos];
            if(oldBuffer!=null){
                //copy old buffer if it exists
                synchronized (oldBuffer){
                    oldBuffer.rewind();
                    newBuf.put(oldBuffer);
                }
            }
            return newBuf;
        }

        @Override public void close() {
            for(java.nio.ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    unmap((MappedByteBuffer)b);
                }
            }
            buffers = null;
        }

        @Override public void sync() {}

        @Override public void deleteFile() {}

        @Override
        public File getFile() {
            return null;
        }
    }


    public static final class RandomAccessFile extends Volume{

        protected final File file;
        protected final boolean readOnly;

        protected java.io.RandomAccessFile raf;
        protected long pos;

        public RandomAccessFile(File file, boolean readOnly) {
            this.file = file;
            this.readOnly = readOnly;

            try {
                this.raf = new java.io.RandomAccessFile(file, readOnly? "r":"rw");
                this.raf.seek(0);
                pos = 0;
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        synchronized public void ensureAvailable(long offset) {
            //we do not have a list of ByteBuffers, so ensure size does not have to do anything
        }

        @Override
        synchronized public void putLong(long offset, long value) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+8;
                raf.writeLong(value);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        synchronized public void putInt(long offset, int value) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+4;
                raf.writeInt(value);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }


        @Override
        synchronized public void putByte(long offset, byte value) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+1;
                raf.writeByte(0xFF & value);
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        synchronized public void putData(long offset, byte[] value, int size) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+size;
                raf.write(value,0,size);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        synchronized public void putData(long offset, java.nio.ByteBuffer buf, int size) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+size;
                byte[] b = new byte[size];
                buf.get(b);
                putData(offset, b, size);
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        synchronized public long getLong(long offset) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+8;
                return raf.readLong();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        synchronized public int getInt(long offset) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+4;
                return raf.readInt();
            } catch (IOException e) {
                throw new IOError(e);
            }

        }


        @Override
        synchronized public byte getByte(long offset) {

            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+1;
                return raf.readByte();
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        synchronized public DataInput2 getDataInput(long offset, int size) {
            try {
                if(pos!=offset){
                    raf.seek(offset);
                }
                pos=offset+size;
                byte[] b = new byte[size];
                raf.read(b);
                return new DataInput2(b);
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        synchronized public void close() {
            try {
                raf.close();
                raf = null;
            } catch (IOException e) {
                throw new IOError(e);
            }

        }

        @Override
        synchronized public void sync() {
            try {
                raf.getFD().sync();
            } catch (IOException e) {
                throw new IOError(e);
            }
        }

        @Override
        public boolean isEmpty() {
            return file.length()==0;
        }

        @Override
        synchronized public void deleteFile() {
            file.delete();
        }

        @Override
        public boolean isSliced(){
            return false;
        }

        @Override
        public File getFile() {
            return file;
        }
    }


}

