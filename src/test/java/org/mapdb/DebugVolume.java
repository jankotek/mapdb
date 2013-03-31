package org.mapdb;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * Wraps existing volume and logs all operations
 */
public class DebugVolume extends Volume{


    protected PrintStream out = System.out;

    protected final Volume vol;

    public DebugVolume(Volume vol) {
        this.vol = vol;
    }

    @Override
    public void ensureAvailable(long offset) {
        out.println("ensureAvailable: "+offset);
        vol.ensureAvailable(offset);
    }

    @Override
    public void putLong(long offset, long value) {
        out.println("putLong: "+offset+ " - "+value);
        vol.putLong(offset, value);
    }

    @Override
    public void putInt(long offset, int value) {
        out.println("putInt: "+offset+ " - "+value);
        vol.putInt(offset, value);
    }


    @Override
    public void putByte(long offset, byte value) {
        out.println("putByte: "+offset+ " - "+value);
        vol.putByte(offset, value);
    }

    @Override
    public void putData(long offset, byte[] value, int pos, int size) {
        out.println("putData: "+offset+ " - "+pos+ " - "+size);
        out.println("  "+ Arrays.toString(value));
        vol.putData(offset,value,pos, size);
    }

    @Override
    public void putData(long offset, java.nio.ByteBuffer buf) {
        int size = buf.limit()-buf.position();
        out.println("putDataBuf: "+offset+ " - "+size);
        out.println("  "+ Arrays.toString(buf.array()));
        vol.putData(offset,buf);
    }

    @Override
    public long getLong(long offset) {
        long ret = vol.getLong(offset);
        out.println("getLong: "+offset+" - "+ret);
        return ret;
    }

    @Override
    public int getInt(long offset) {
        int ret = vol.getInt(offset);
        out.println("getInt: "+offset+" - "+ret);
        return ret;
    }


    @Override
    public byte getByte(long offset) {
        byte ret = vol.getByte(offset);
        out.println("getByte: "+offset+" - "+ret);
        return ret;

    }

    @Override
    public DataInput2 getDataInput(long offset, int size) {
        DataInput2 ret = vol.getDataInput(offset, size);
        out.println("getDataInput: "+offset+" - "+size);
        byte[] bb = new byte[size];
        try {
            ret.readFully(bb);
        } catch (IOException e) {
            throw new IOError(e);
        }
        ret.pos=0;
        out.println("   "+Arrays.toString(bb));
        return ret;
    }

    @Override
    public void close() {
        out.println("close");
        vol.close();
    }

    @Override
    public void sync() {
        out.println("sync");
        vol.sync();
    }

    @Override
    public boolean isEmpty() {
        out.println("isEmpty()");
        return vol.isEmpty();
    }

    @Override
    public void deleteFile() {
        out.println("deleteFile");
        vol.deleteFile();
    }

    @Override
    public boolean isSliced() {
        out.println("isSliced");
        return vol.isSliced();
    }

    @Override
    public File getFile() {
        return vol.getFile();
    }
}
