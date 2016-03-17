package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DataIO;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Arrays;

/**
 * Created by jan on 2/29/16.
 */
public final class MappedFileVol extends ByteBufferVol {

    public static final VolumeFactory FACTORY = new MappedFileFactory(false, false);

    public static class MappedFileFactory extends VolumeFactory {

        final boolean cleanerHackEnabled;
        final boolean preclearDisabled;

        public MappedFileFactory(boolean cleanerHackEnabled, boolean preclearDisabled) {
            this.cleanerHackEnabled = cleanerHackEnabled;
            this.preclearDisabled = preclearDisabled;
        }

        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            return factory(file, readOnly, fileLockDisabled, sliceShift, cleanerHackEnabled, initSize, preclearDisabled);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return new File(file).exists();
        }

        private static Volume factory(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift,
                                      boolean cleanerHackEnabled, long initSize, boolean preclearDisabled) {
            File f = new File(file);
            if (readOnly) {
                long flen = f.length();
                if (flen <= Integer.MAX_VALUE) {
                    return new MappedFileVolSingle(f, readOnly, fileLockDisabled,
                            Math.max(flen, initSize),
                            cleanerHackEnabled);
                }
            }
            //TODO prealocate initsize
            return new org.mapdb.volume.MappedFileVol(f, readOnly, fileLockDisabled, sliceShift, cleanerHackEnabled, initSize, preclearDisabled);
        }

    }

    protected final File file;
    protected final FileChannel fileChannel;
    protected final FileChannel.MapMode mapMode;
    protected final RandomAccessFile raf;
    protected final FileLock fileLock;
    protected final boolean preclearDisabled;

    public MappedFileVol(File file, boolean readOnly, boolean fileLockDisable,
                         int sliceShift, boolean cleanerHackEnabled, long initSize,
                         boolean preclearDisabled) {
        super(readOnly, sliceShift, cleanerHackEnabled);
        this.file = file;
        this.mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
        this.preclearDisabled = preclearDisabled;
        try {
            FileChannelVol.checkFolder(file, readOnly);
            this.raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
            this.fileChannel = raf.getChannel();

            fileLock = Volume.lockFile(file, raf, readOnly, fileLockDisable);

            final long fileSize = fileChannel.size();
            long endSize = fileSize;
            if (initSize > fileSize && !readOnly)
                endSize = initSize; //allocate more data

            if (endSize > 0) {
                //map data
                int chunksSize = (int) ((DataIO.roundUp(endSize, sliceSize) >>> sliceShift));
                if (endSize > fileSize && !readOnly) {
                    RandomAccessFileVol.clearRAF(raf, fileSize, endSize);
                    raf.getFD().sync();
                }

                slices = new ByteBuffer[chunksSize];
                for (int i = 0; i < slices.length; i++) {
                    ByteBuffer b = fileChannel.map(mapMode, 1L * sliceSize * i, sliceSize);
                    if (CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                        throw new AssertionError("Little-endian");
                    slices[i] = b;
                }
            } else {
                slices = new ByteBuffer[0];
            }
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        }
    }

    @Override
    public final void ensureAvailable(long offset) {
        offset = DataIO.roundUp(offset, 1L << sliceShift);
        int slicePos = (int) (offset >>> sliceShift);

        //check for most common case, this is already mapped
        if (slicePos < slices.length) {
            return;
        }

        growLock.lock();
        try {
            //check second time
            if (slicePos <= slices.length)
                return;

            int oldSize = slices.length;

            if (!preclearDisabled) {
                // fill with zeroes from  old size to new size
                // this will prevent file from growing via mmap operation
                RandomAccessFileVol.clearRAF(raf, 1L * oldSize * sliceSize, offset);
                raf.getFD().sync();
            }

            //grow slices
            ByteBuffer[] slices2 = slices;

            slices2 = Arrays.copyOf(slices2, slicePos);

            for (int pos = oldSize; pos < slices2.length; pos++) {
                ByteBuffer b = fileChannel.map(mapMode, 1L * sliceSize * pos, sliceSize);
                if (CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                    throw new AssertionError("Little-endian");
                slices2[pos] = b;
            }

            slices = slices2;
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        } finally {
            growLock.unlock();
        }
    }


    @Override
    public void close() {
        growLock.lock();
        try {
            if (closed)
                return;

            closed = true;
            if (fileLock != null && fileLock.isValid()) {
                fileLock.release();
            }
            fileChannel.close();
            raf.close();
            //TODO not sure if no sync causes problems while unlocking files
            //however if it is here, it causes slow commits, sync is called on write-ahead-log just before it is deleted and closed
//                if(!readOnly)
//                    sync();

            if (cleanerHackEnabled) {
                for (ByteBuffer b : slices) {
                    if (b != null && (b instanceof MappedByteBuffer)) {
                        unmap((MappedByteBuffer) b);
                    }
                }
            }
            Arrays.fill(slices, null);
            slices = null;

        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        } finally {
            growLock.unlock();
        }

    }

    @Override
    public void sync() {
        if (readOnly)
            return;
        growLock.lock();
        try {
            ByteBuffer[] slices = this.slices;
            if (slices == null)
                return;

            // Iterate in reverse order.
            // In some cases if JVM crashes during iteration,
            // first part of the file would be synchronized,
            // while part of file would be missing.
            // It is better if end of file is synchronized first, since it has less sensitive data,
            // and it increases chance to detect file corruption.
            for (int i = slices.length - 1; i >= 0; i--) {
                ByteBuffer b = slices[i];
                if (b != null && (b instanceof MappedByteBuffer)) {
                    MappedByteBuffer bb = ((MappedByteBuffer) b);
                    bb.force();
                }
            }
        } finally {
            growLock.unlock();
        }

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
        final int maxSize = 1 + (int) (size >>> sliceShift);
        if (maxSize == slices.length)
            return;
        if (maxSize > slices.length) {
            ensureAvailable(size);
            return;
        }
        growLock.lock();
        try {
            if (maxSize >= slices.length)
                return;
            ByteBuffer[] old = slices;
            slices = Arrays.copyOf(slices, maxSize);

            //unmap remaining buffers
            for (int i = maxSize; i < old.length; i++) {
                if (cleanerHackEnabled) {
                    unmap((MappedByteBuffer) old[i]);
                }
                old[i] = null;
            }

            if (ByteBufferVol.windowsWorkaround) {
                for (int i = 0; i < maxSize; i++) {
                    if (cleanerHackEnabled) {
                        unmap((MappedByteBuffer) old[i]);
                    }
                    old[i] = null;
                }
            }

            try {
                fileChannel.truncate(1L * sliceSize * maxSize);
            } catch (IOException e) {
                throw new DBException.VolumeIOError(e);
            }

            if (ByteBufferVol.windowsWorkaround) {
                for (int pos = 0; pos < maxSize; pos++) {
                    ByteBuffer b = fileChannel.map(mapMode, 1L * sliceSize * pos, sliceSize);
                    if (CC.ASSERT && b.order() != ByteOrder.BIG_ENDIAN)
                        throw new AssertionError("Little-endian");
                    slices[pos] = b;
                }
            }
        } catch (IOException e) {
            throw new DBException.VolumeIOError(e);
        } finally {
            growLock.unlock();
        }
    }

    @Override
    public boolean fileLoad() {
        ByteBuffer[] slices = this.slices;
        for (ByteBuffer b : slices) {
            if (b instanceof MappedByteBuffer) {
                ((MappedByteBuffer) b).load();
            }
        }
        return true;
    }
}
