package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DBUtil;
import org.mapdb.DataInput2;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Volume which uses FileChannel.
 * Uses global lock and does not use mapped memory.
 */
public final class FileChannelVol extends Volume {

    public static final VolumeFactory FACTORY = new VolumeFactory() {

        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            return new org.mapdb.volume.FileChannelVol(new File(file),readOnly, fileLockDisabled, sliceShift,initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return new File(file).exists();
        }

    };

    protected final File file;
    protected final int sliceSize;
    protected RandomAccessFile raf;
    protected FileChannel channel;
    protected final boolean readOnly;
    protected final FileLock fileLock;

    protected volatile long size;
    protected final Lock growLock = new ReentrantLock();

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
        this(file, false, false, CC.PAGE_SHIFT,0L);
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
        offset= DBUtil.roundUp(offset,sliceSize);

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
    public DataInput2.ByteBuffer getDataInput(long offset, int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);
        readFully(offset,buf);
        return new DataInput2.ByteBuffer(buf,0);
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
