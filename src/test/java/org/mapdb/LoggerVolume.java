package org.mapdb;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;


/**
 * Logs write operations performed on Volume.
 * Useful for debugging storage problems.
 */
public class LoggerVolume extends Volume{

    public static class Factory implements VolumeFactory{

        final VolumeFactory loggedFac;
        final VolumeFactory logFac;

        public Factory(VolumeFactory loggedFac, VolumeFactory logFac) {
            this.loggedFac = loggedFac;
            this.logFac = logFac;
        }

        @Override
        public Volume createIndexVolume() {
            return new LoggerVolume(loggedFac.createIndexVolume(), logFac.createIndexVolume());
        }

        @Override
        public Volume createPhysVolume() {
            return new LoggerVolume(loggedFac.createPhysVolume(), logFac.createPhysVolume());
        }

        @Override
        public Volume createTransLogVolume() {
            return new LoggerVolume(loggedFac.createTransLogVolume(), logFac.createTransLogVolume());
        }
    }


    protected static final byte LONG = 1;
    protected static final byte BYTE = 2;
    protected static final byte BYTE_ARRAY = 3;

    protected final Volume logged;
    protected final Volume log;
    protected long pos = 0;

    public LoggerVolume(Volume logged, Volume log) {
        this.logged = logged;
        this.log = log;
    }


    protected void logStackTrace(){
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new Exception().printStackTrace(new PrintStream(out));
        byte[] b = out.toByteArray();
        log.ensureAvailable(pos +8 + b.length);
        log.putLong(pos, b.length);
        pos +=8;
        log.putData(pos, b, b.length);
        pos += b.length;

    }

    @Override
    synchronized public void ensureAvailable(long offset) {
        logged.ensureAvailable(offset);
    }

    @Override
    synchronized public void putLong(long offset, long value) {
        logged.putLong(offset, value);

        log.ensureAvailable(pos+1+8+8);
        log.putByte(pos, LONG);
        pos+=1;
        log.putLong(pos, offset);
        pos+=8;
        log.putLong(pos, value);
        pos+=8;

        logStackTrace();
    }

    @Override
    synchronized public void putByte(long offset, byte value) {
        logged.putByte(offset, value);

        log.ensureAvailable(pos+1+8+1);
        log.putByte(pos, BYTE);
        pos+=1;
        log.putLong(pos, offset);
        pos+=8;
        log.putByte(pos, value);
        pos+=1;

        logStackTrace();
    }

    @Override
    synchronized public void putData(long offset, byte[] value, int size) {
        logged.putData(offset, value, size);

        log.ensureAvailable(pos+1+8+size);
        log.putByte(pos, BYTE_ARRAY);
        pos+=1;
        log.putLong(pos, offset);
        pos+=8;
        log.putData(pos, value, size);
        pos+=size;

        logStackTrace();
    }

    @Override
    synchronized public void putData(long offset, ByteBuffer buf, int size) {
        byte[] b = new byte[size];
        buf.get(b);
        putData(offset, b, size);
    }

    @Override
    public long getLong(long offset) {
        return logged.getLong(offset);
    }

    @Override
    public byte getByte(long offset) {
        return logged.getByte(offset);
    }

    @Override
    public DataInput2 getDataInput(long offset, int size) {
        return logged.getDataInput(offset, size);
    }

    @Override
    public void close() {
        logged.close();
    }

    @Override
    public void sync() {
        logged.sync();
        log.sync();
    }

    @Override
    public boolean isEmpty() {
        return logged.isEmpty();
    }

    @Override
    public void deleteFile() {
        logged.deleteFile();
    }

    @Override
    public boolean isSliced() {
        return logged.isSliced();
    }
}
