package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.util.DataIO;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

/**
 * Created by jan on 3/13/16.
 */
public final class ByteBufferMemoryVol extends ByteBufferVol {

    /**
     * factory for DirectByteBuffer storage
     */
    public static final VolumeFactory FACTORY = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, long fileLockWait, int sliceShift, long initSize, boolean fixedSize) {
            //TODO optimize for fixedSize smaller than 2GB
            return new ByteBufferMemoryVol(true, sliceShift, false, initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
        }

        @Override
        public boolean handlesReadonly() {
            return false;
        }
    };


    /**
     * factory for DirectByteBuffer storage
     */
    public static final VolumeFactory FACTORY_WITH_CLEANER_HACK = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, long fileLockWait, int sliceShift, long initSize, boolean fixedSize) {//TODO prealocate initSize
            //TODO optimize for fixedSize smaller than 2GB
            return new ByteBufferMemoryVol(true, sliceShift, true, initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
        }

        @Override
        public boolean handlesReadonly() {
            return false;
        }
    };

    protected final boolean useDirectBuffer;

    @Override
    public String toString() {
        return super.toString() + ",direct=" + useDirectBuffer;
    }

    public ByteBufferMemoryVol(final boolean useDirectBuffer, final int sliceShift, boolean cleanerHackEnabled, long initSize) {
        super(false, sliceShift, cleanerHackEnabled);
        this.useDirectBuffer = useDirectBuffer;
        if (initSize != 0)
            ensureAvailable(initSize);
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
            ByteBuffer[] slices2 = slices;

            slices2 = Arrays.copyOf(slices2, slicePos);

            for (int pos = oldSize; pos < slices2.length; pos++) {
                ByteBuffer b = useDirectBuffer ?
                        ByteBuffer.allocateDirect(sliceSize) :
                        ByteBuffer.allocate(sliceSize);
                if (CC.PARANOID && b.order() != ByteOrder.BIG_ENDIAN)
                    throw new AssertionError("little-endian");
                slices2[pos] = b;
            }

            slices = slices2;
        } catch (OutOfMemoryError e) {
            throw new DBException.OutOfMemory(e);
        } finally {
            growLock.unlock();
        }
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
                if (cleanerHackEnabled && old[i] instanceof MappedByteBuffer)
                    unmap((MappedByteBuffer) old[i]);
                old[i] = null;
            }

        } finally {
            growLock.unlock();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false,true))
            return;

        growLock.lock();
        try {
            if (cleanerHackEnabled) {
                for (ByteBuffer b : slices) {
                    if (b != null && (b instanceof MappedByteBuffer)) {
                        unmap((MappedByteBuffer) b);
                    }
                }
            }
            Arrays.fill(slices, null);
            slices = null;
        } finally {
            growLock.unlock();
        }
    }

    @Override
    public void sync() {
    }

    @Override
    public long length() {
        return ((long) slices.length) * sliceSize;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public File getFile() {
        return null;
    }

    @Override
    public boolean getFileLocked() {
        return false;
    }
}
