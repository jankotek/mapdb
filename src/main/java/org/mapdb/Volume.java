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
import java.nio.ByteBuffer;
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

    abstract public void putData(final long offset, final ByteBuffer buf, final int size);

    abstract public long getLong(final long offset);

    abstract public byte getByte(final long offset);



    abstract public DataInput2 getDataInput(final long offset, final int size);

    abstract public void close();

    abstract public void sync();

    public abstract void deleteFile();



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
     * Factory which creates two/three volumes used by each MapDB Storage Engine
     */
    public static interface VolumeFactory{
        Volume createIndexVolume();
        Volume createPhysVolume();
        Volume createTransLogVolume();
    }

    public static class FileVolumeFactory implements VolumeFactory{

        protected boolean readOnly;
        protected final File indexFile;
        protected final File physFile;
        protected final File transLogFile;

        public FileVolumeFactory(boolean readOnly, File indexFile){
            this(readOnly, indexFile,
                    new File(indexFile.getPath()+Storage.DATA_FILE_EXT),
                    new File(indexFile.getPath()+StorageTrans.TRANS_LOG_FILE_EXT));
        }

        public FileVolumeFactory(boolean readOnly, File indexFile, File physFile, File transLogFile) {
            this.readOnly = readOnly;
            this.indexFile = indexFile;
            this.physFile = physFile;
            this.transLogFile = transLogFile;
        }

        @Override
        public Volume createIndexVolume() {
            return new MappedFileVolume(indexFile, readOnly);
        }

        @Override
        public Volume createPhysVolume() {
            return new MappedFileVolume(physFile, readOnly);
        }

        @Override
        public Volume createTransLogVolume() {
            return new MappedFileVolume(transLogFile, readOnly);
        }

    }

    public static class MemoryVolumeFactory implements VolumeFactory{
        protected final boolean useDirectBuffer;

        public MemoryVolumeFactory(boolean useDirectBuffer) {
            this.useDirectBuffer = useDirectBuffer;
        }

        @Override public Volume createIndexVolume() {
            return new MemoryVolume(useDirectBuffer);
        }

        @Override public Volume createPhysVolume() {
            return new MemoryVolume(useDirectBuffer);
        }

        @Override public Volume createTransLogVolume() {
            return new MemoryVolume(useDirectBuffer);
        }
    }


    /**
     * Abstract Volume over bunch of ByteBuffers
     * It leaves ByteBuffer details (allocation, disposal) on subclasses.
     * Most methods are final for better performance (JIT compiler can inline those).
     */
    abstract static public class ByteBufferVolume extends Volume{



        protected ByteBuffer[] buffers;
        protected final boolean readOnly;

        protected ByteBufferVolume(boolean readOnly) {
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
            
            ByteBuffer newBuf = makeNewBuffer(offset);
            if(readOnly)
                newBuf = newBuf.asReadOnlyBuffer();

            buffers[buffersPos] = newBuf;
        }

        protected abstract ByteBuffer makeNewBuffer(long offset);

        protected final ByteBuffer internalByteBuffer(long offset) {
            return buffers[((int) (offset / BUF_SIZE))];
        }



        @Override public final void putLong(final long offset, final long value) {
            internalByteBuffer(offset).putLong((int) (offset% BUF_SIZE), value);
        }

        @Override public final void putByte(final long offset, final byte value) {
            internalByteBuffer(offset).put((int) (offset % BUF_SIZE), value);
        }

        @Override public final void putData(final long offset, final byte[] value, final int size) {
            final ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);

            synchronized (b1){
                b1.position(bufPos);
                b1.put(value, 0, size);
            }
        }

        @Override public final void putData(final long offset, final ByteBuffer buf, final int size) {
            final ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);
            //no overlap, so just write the value
            synchronized (b1){
               b1.position(bufPos);
               b1.put(buf);
            }
        }

        @Override final public long getLong(long offset) {
            return internalByteBuffer(offset).getLong((int) (offset% BUF_SIZE));
        }

        @Override public final byte getByte(long offset) {
            return internalByteBuffer(offset).get((int) (offset% BUF_SIZE));
        }


        @Override
        public final DataInput2 getDataInput(long offset, int size) {
            final ByteBuffer b1 = internalByteBuffer(offset);
            final int bufPos = (int) (offset% BUF_SIZE);
            return new DataInput2(b1, bufPos);
        }

        /**
         * Hack to unmap MappedByteBuffer.
         * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
         * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
         * Any error is silently ignored (for example SUN API does not exist on Android).
         */
        public static final void unmap(MappedByteBuffer b){
            try{
                if(unmapHackSupported){
                    sun.misc.Cleaner cleaner = ((sun.nio.ch.DirectBuffer) b).cleaner();
                    if(cleaner!=null)
                        cleaner.clean();
                }
            }catch(Exception e){
                Utils.LOG.log(Level.FINE, "ByteBuffer Unmap failed", e);
            }
        }


        private static boolean unmapHackSupported = false;
        static{
            //TODO check how this works on Android
            try{
                unmapHackSupported =
                        Class.forName("sun.nio.ch.DirectBuffer")!=null;
            }catch(Exception e){
                unmapHackSupported = false;
            }
        }
    }

    public static final class MappedFileVolume extends ByteBufferVolume{

        protected final File file;
        protected final FileChannel fileChannel;
        protected final FileChannel.MapMode mapMode;

        static final int BUF_SIZE_INC = 1024*1024;

        public MappedFileVolume(File file, boolean readOnly) {
            super(readOnly);
            this.file = file;
            this.mapMode = readOnly? FileChannel.MapMode.READ_ONLY: FileChannel.MapMode.READ_WRITE;
            try {
                this.fileChannel = new RandomAccessFile(file, readOnly?"r":"rw")
                        .getChannel();

                final long fileSize = fileChannel.size();
                if(fileSize>0){
                    //map existing data
                    buffers = new ByteBuffer[(int) (1+fileSize/BUF_SIZE)];
                    for(int i=0;i<=fileSize/BUF_SIZE;i++){
                        final long offset = 1L*BUF_SIZE*i;
                        buffers[i] = fileChannel.map(mapMode, offset, Math.min(BUF_SIZE, fileSize-offset));
                        if(mapMode == FileChannel.MapMode.READ_ONLY)
                            buffers[i] = buffers[i].asReadOnlyBuffer();
                        //TODO what if 'fileSize % 8 != 0'?
                    }
                }else{
                    buffers = new ByteBuffer[1];
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
            } catch (IOException e) {
                throw new IOError(e);
            }
            if(!readOnly)
                sync();
            for(ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    unmap((MappedByteBuffer) b);
                }
            }
            buffers = null;
        }

        @Override
        public void sync() {
            if(readOnly) return;
            for(ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    ((MappedByteBuffer)b).force();
                }
            }
        }

        @Override
        public void deleteFile() {
            file.delete();
        }

        @Override
        protected ByteBuffer makeNewBuffer(long offset) {
            try {
                long newBufSize =  offset% BUF_SIZE;
                newBufSize = newBufSize + newBufSize%BUF_SIZE_INC; //round to BUF_SIZE_INC
                return fileChannel.map(
                        mapMode,
                        offset - offset% BUF_SIZE, newBufSize );
            } catch (IOException e) {
                throw new IOError(e);
            }
        }
    }

    public static final class MemoryVolume extends ByteBufferVolume{
        protected final boolean useDirectBuffer;

        @Override
        public String toString() {
            return super.toString()+",direct="+useDirectBuffer;
        }

        public MemoryVolume(boolean useDirectBuffer) {
            super(false);
            this.useDirectBuffer = useDirectBuffer;
            ByteBuffer b0 = useDirectBuffer?
                    ByteBuffer.allocateDirect(INITIAL_SIZE) :
                    ByteBuffer.allocate(INITIAL_SIZE);
            buffers = new ByteBuffer[]{b0};
        }

        @Override protected ByteBuffer makeNewBuffer(long offset) {
            final int newBufSize = Utils.nextPowTwo((int) (offset % BUF_SIZE));
            //double size of existing in-memory-buffer
            ByteBuffer newBuf = useDirectBuffer?
                    ByteBuffer.allocateDirect(newBufSize):
                    ByteBuffer.allocate(newBufSize);
            final int buffersPos = (int) (offset/ BUF_SIZE);
            final ByteBuffer oldBuffer = buffers[buffersPos];
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
            for(ByteBuffer b:buffers){
                if(b!=null && (b instanceof MappedByteBuffer)){
                    unmap((MappedByteBuffer)b);
                }
            }
            buffers = null;
        }

        @Override public void sync() {}

        @Override public void deleteFile() {}

    }

}

