package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * Created by jan on 2/29/16.
 */
public final class MappedFileVolSingle extends ByteBufferVolSingle {


    protected final static VolumeFactory FACTORY = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            if (initSize > Integer.MAX_VALUE)
                throw new IllegalArgumentException("startSize larger 2GB");
            return new org.mapdb.volume.MappedFileVolSingle(
                    new File(file),
                    readOnly,
                    fileLockDisabled,
                    initSize,
                    false);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return new File(file).exists();
        }

    };

    protected final static VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            if (initSize > Integer.MAX_VALUE)
                throw new IllegalArgumentException("startSize larger 2GB");
            return new org.mapdb.volume.MappedFileVolSingle(
                    new File(file),
                    readOnly,
                    fileLockDisabled,
                    initSize,
                    true);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return new File(file).exists();
        }

    };


    protected final File file;
    protected final FileChannel.MapMode mapMode;
    protected final RandomAccessFile raf;
    protected final FileLock fileLock;

    public MappedFileVolSingle(File file, boolean readOnly, boolean fileLockDisabled, long maxSize,
                               boolean cleanerHackEnabled) {
        super(readOnly, maxSize, cleanerHackEnabled);
        this.file = file;
        this.mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
        try {
            FileChannelVol.checkFolder(file, readOnly);
            raf = new RandomAccessFile(file, readOnly ? "r" : "rw");

            fileLock = Volume.lockFile(file, raf, readOnly, fileLockDisabled);


            final long fileSize = raf.length();
            if (readOnly) {
                maxSize = Math.min(maxSize, fileSize);
            } else if (fileSize < maxSize) {
                //zero out data between fileSize and maxSize, so mmap file operation does not expand file
                raf.seek(fileSize);
                long offset = fileSize;
                do {
                    raf.write(CLEAR, 0, (int) Math.min(CLEAR.length, maxSize - offset));
                    offset += CLEAR.length;
                } while (offset < maxSize);
            }
            buffer = raf.getChannel().map(mapMode, 0, maxSize);

            if (readOnly)
                buffer = buffer.asReadOnlyBuffer();
            //TODO assert endianess
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    synchronized public void close() {
        if (closed) {
            return;
        }
        closed = true;
        //TODO not sure if no sync causes problems while unlocking files
        //however if it is here, it causes slow commits, sync is called on write-ahead-log just before it is deleted and closed
//                if(!readOnly)
//                    sync();

        try {
            if (fileLock != null && fileLock.isValid()) {
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
        if (readOnly)
            return;
        if (buffer instanceof MappedByteBuffer)
            ((MappedByteBuffer) buffer).force();
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
        return fileLock != null && fileLock.isValid();
    }

    @Override
    public void truncate(long size) {
        //TODO truncate
    }

    @Override
    public boolean fileLoad() {
        ((MappedByteBuffer) buffer).load();
        return true;
    }
}
