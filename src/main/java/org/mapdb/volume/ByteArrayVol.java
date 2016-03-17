package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.DataIO;
import org.mapdb.DataInput2;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by jan on 2/29/16.
 */
public final class ByteArrayVol extends Volume {

    public static final VolumeFactory FACTORY = new VolumeFactory() {

        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisable, int sliceShift, long initSize, boolean fixedSize) {
            //TODO optimize for fixedSize if bellow 2GB
            return new org.mapdb.volume.ByteArrayVol(sliceShift, initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
        }
    };

    protected final ReentrantLock growLock = new ReentrantLock();

    protected final int sliceShift;
    protected final int sliceSizeModMask;
    protected final int sliceSize;

    protected volatile byte[][] slices = new byte[0][];

    public ByteArrayVol() {
        this(CC.PAGE_SHIFT, 0L);
    }

    public ByteArrayVol(int sliceShift, long initSize) {
        this.sliceShift = sliceShift;
        this.sliceSize = 1 << sliceShift;
        this.sliceSizeModMask = sliceSize - 1;

        if (initSize != 0) {
            ensureAvailable(initSize);
        }
    }

    protected final byte[] getSlice(long offset) {
        byte[][] slices = this.slices;
        int pos = ((int) (offset >>> sliceShift));
        if (pos >= slices.length)
            throw new DBException.VolumeEOF("offset points beyond slices");
        return slices[pos];
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
            byte[][] slices2 = slices;

            slices2 = Arrays.copyOf(slices2, slicePos);

            for (int pos = oldSize; pos < slices2.length; pos++) {
                slices2[pos] = new byte[sliceSize];
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
            slices = Arrays.copyOf(slices, maxSize);
        } finally {
            growLock.unlock();
        }
    }

    @Override
    public void putLong(long offset, long v) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        DataIO.putLong(buf, pos, v);
    }


    @Override
    public void putInt(long offset, int value) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        buf[pos++] = (byte) (0xff & (value >> 24));
        buf[pos++] = (byte) (0xff & (value >> 16));
        buf[pos++] = (byte) (0xff & (value >> 8));
        buf[pos++] = (byte) (0xff & (value));
    }

    @Override
    public void putByte(long offset, byte value) {
        final byte[] b = getSlice(offset);
        b[((int) (offset & sliceSizeModMask))] = value;
    }

    @Override
    public void putData(long offset, byte[] src, int srcPos, int srcSize) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        System.arraycopy(src, srcPos, buf, pos, srcSize);
    }

    @Override
    public void putData(long offset, ByteBuffer buf) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] dst = getSlice(offset);
        buf.get(dst, pos, buf.remaining());
    }


    @Override
    public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
        int pos = (int) (inputOffset & sliceSizeModMask);
        byte[] buf = getSlice(inputOffset);

        //TODO size>Integer.MAX_VALUE
        target.putData(targetOffset, buf, pos, (int) size);
    }


    @Override
    public void putDataOverlap(long offset, byte[] data, int pos, int len) {
        boolean overlap = (offset >>> sliceShift != (offset + len) >>> sliceShift);

        if (overlap) {
            while (len > 0) {
                byte[] b = getSlice(offset);
                int pos2 = (int) (offset & sliceSizeModMask);

                int toPut = Math.min(len, sliceSize - pos2);

                System.arraycopy(data, pos, b, pos2, toPut);

                pos += toPut;
                len -= toPut;
                offset += toPut;
            }
        } else {
            putData(offset, data, pos, len);
        }
    }

    @Override
    public DataInput2 getDataInputOverlap(long offset, int size) {
        boolean overlap = (offset >>> sliceShift != (offset + size) >>> sliceShift);
        if (overlap) {
            byte[] bb = new byte[size];
            final int origLen = size;
            while (size > 0) {
                byte[] b = getSlice(offset);
                int pos = (int) (offset & sliceSizeModMask);

                int toPut = Math.min(size, sliceSize - pos);

                System.arraycopy(b, pos, bb, origLen - size, toPut);

                size -= toPut;
                offset += toPut;
            }
            return new DataInput2.ByteArray(bb);
        } else {
            //return mapped buffer
            return getDataInput(offset, size);
        }
    }

    @Override
    public void clear(long startOffset, long endOffset) {
        if (CC.ASSERT && (startOffset >>> sliceShift) != ((endOffset - 1) >>> sliceShift))
            throw new AssertionError();
        byte[] buf = getSlice(startOffset);
        int start = (int) (startOffset & sliceSizeModMask);
        int end = (int) (start + (endOffset - startOffset));

        int pos = start;
        while (pos < end) {
            System.arraycopy(CLEAR, 0, buf, pos, Math.min(CLEAR.length, end - pos));
            pos += CLEAR.length;
        }
    }

    @Override
    public long getLong(long offset) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        return DataIO.getLong(buf, pos);
    }

    @Override
    public int getInt(long offset) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);

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
        final byte[] b = getSlice(offset);
        return b[((int) (offset & sliceSizeModMask))];
    }

    @Override
    public DataInput2 getDataInput(long offset, int size) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        return new DataInput2.ByteArray(buf, pos);
    }

    @Override
    public void getData(long offset, byte[] bytes, int bytesPos, int length) {
        int pos = (int) (offset & sliceSizeModMask);
        byte[] buf = getSlice(offset);
        System.arraycopy(buf, pos, bytes, bytesPos, length);
    }

    @Override
    public void close() {
        closed = true;
        slices = null;
    }

    @Override
    public void sync() {

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
        return ((long) slices.length) * sliceSize;
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
