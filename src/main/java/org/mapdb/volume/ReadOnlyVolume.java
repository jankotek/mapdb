package org.mapdb.volume;

import org.mapdb.DataInput2;

import java.io.File;
import java.nio.ByteBuffer;

/**
 * Created by jan on 2/29/16.
 */
public final class ReadOnlyVolume extends Volume {

    protected final Volume vol;

    public ReadOnlyVolume(Volume vol) {
        this.vol = vol;
    }

    @Override
    public void ensureAvailable(long offset) {
        //TODO some error handling here?
        return;
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
    public void putDataOverlap(long offset, byte[] src, int srcPos, int srcSize) {
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
    public DataInput2 getDataInput(long offset, int size) {
        return vol.getDataInput(offset, size);
    }

    @Override
    public DataInput2 getDataInputOverlap(long offset, int size) {
        return vol.getDataInputOverlap(offset, size);
    }

    @Override
    public void getData(long offset, byte[] bytes, int bytesPos, int size) {
        vol.getData(offset, bytes, bytesPos, size);
    }

    @Override
    public void close() {
        closed = true;
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
    public void deleteFile() {
        throw new IllegalAccessError("read-only");
    }

    @Override
    public boolean isSliced() {
        return vol.isSliced();
    }

    @Override
    public long length() {
        return vol.length();
    }

    @Override
    public void putUnsignedShort(long offset, int value) {
        throw new IllegalAccessError("read-only");
    }

    @Override
    public int getUnsignedShort(long offset) {
        return vol.getUnsignedShort(offset);
    }

    @Override
    public int getUnsignedByte(long offset) {
        return vol.getUnsignedByte(offset);
    }

    @Override
    public void putUnsignedByte(long offset, int b) {
        throw new IllegalAccessError("read-only");
    }


    @Override
    public long getSixLong(long pos) {
        return vol.getSixLong(pos);
    }

    @Override
    public void putSixLong(long pos, long value) {
        throw new IllegalAccessError("read-only");
    }

    @Override
    public File getFile() {
        return vol.getFile();
    }

    @Override
    public boolean getFileLocked() {
        return vol.getFileLocked();
    }

    @Override
    public void transferInto(long inputOffset, Volume target, long targetOffset, long size) {
        vol.transferInto(inputOffset, target, targetOffset, size);
    }

    @Override
    public void clear(long startOffset, long endOffset) {
        throw new IllegalAccessError("read-only");
    }
}
