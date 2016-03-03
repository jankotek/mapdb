package org.mapdb.volume;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mapdb.DBException;
import org.mapdb.DBUtil;
import org.mapdb.DataInput2;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Volume backed by on-heap byte[] with maximal fixed size 2GB.
 * For thread-safety it can not be grown
  */
public final class SingleByteArrayVol extends Volume {

    protected final static VolumeFactory FACTORY = new VolumeFactory() {
        @Override
        public Volume makeVolume(String file, boolean readOnly, boolean fileLockDisabled, int sliceShift, long initSize, boolean fixedSize) {
            if(initSize>Integer.MAX_VALUE)
                throw new IllegalArgumentException("startSize larger 2GB");
            return new org.mapdb.volume.SingleByteArrayVol((int) initSize);
        }

        @NotNull
        @Override
        public boolean exists(@Nullable String file) {
            return false;
        }
    };

    protected final byte[] data;

    public SingleByteArrayVol(int size) {
        this(new byte[size]);
    }

    public SingleByteArrayVol(byte[] data){
        this.data = data;
    }


    @Override
    public void ensureAvailable(long offset) {
        if(offset >= data.length){
            throw new DBException.VolumeMaxSizeExceeded(data.length, offset);
        }
    }

    @Override
    public void truncate(long size) {
        //unsupported
        //TODO throw an exception?
    }

    @Override
    public void putLong(long offset, long v) {
        DBUtil.putLong(data, (int) offset, v);
    }


    @Override
    public void putInt(long offset, int value) {
        int pos = (int) offset;
        data[pos++] = (byte) (0xff & (value >> 24));
        data[pos++] = (byte) (0xff & (value >> 16));
        data[pos++] = (byte) (0xff & (value >> 8));
        data[pos++] = (byte) (0xff & (value));
    }

    @Override
    public void putByte(long offset, byte value) {
        data[(int) offset] = value;
    }

    @Override
    public void putData(long offset, byte[] src, int srcPos, int srcSize) {
        System.arraycopy(src, srcPos, data, (int) offset, srcSize);
    }

    @Override
    public void putData(long offset, ByteBuffer buf) {
         buf.get(data, (int) offset, buf.remaining());
    }


    @Override
    public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
        //TODO size>Integer.MAX_VALUE
        target.putData(targetOffset,data, (int) inputOffset, (int) size);
    }

    @Override
    public void clear(long startOffset, long endOffset) {
        int start = (int) startOffset;
        int end = (int) endOffset;

        int pos = start;
        while(pos<end){
            System.arraycopy(CLEAR,0,data,pos, Math.min(CLEAR.length, end-pos));
            pos+=CLEAR.length;
        }
    }

    @Override
    public long getLong(long offset) {
        return DBUtil.getLong(data, (int) offset);
    }



    @Override
    public int getInt(long offset) {
        int pos = (int) offset;
        //TODO verify loop
        final int end = pos + 4;
        int ret = 0;
        for (; pos < end; pos++) {
            ret = (ret << 8) | (data[pos] & 0xFF);
        }
        return ret;
    }

    @Override
    public byte getByte(long offset) {
        return data[((int) offset)];
    }

    @Override
    public DataInput2 getDataInput(long offset, int size) {
         return new DataInput2.ByteArray(data, (int) offset);
    }

    @Override
    public void getData(long offset, byte[] bytes, int bytesPos, int length) {
        System.arraycopy(data, (int) offset,bytes,bytesPos,length);
    }

    @Override
    public void close() {
        closed = true;
        //TODO perhaps set `data` to null? what are performance implications for non-final fieldd?
    }

    @Override
    public void sync() {
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
        return data.length;
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
