package org.mapdb.volume;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * Created by jan on 3/13/16.
 */
public final class ByteBufferMemoryVolSingle extends ByteBufferVolSingle {

    protected final boolean useDirectBuffer;

    @Override
    public String toString() {
        return super.toString() + ",direct=" + useDirectBuffer;
    }

    public ByteBufferMemoryVolSingle(final boolean useDirectBuffer, final long maxSize, boolean cleanerHackEnabled) {
        super(false, maxSize, cleanerHackEnabled);
        this.useDirectBuffer = useDirectBuffer;
        this.buffer = useDirectBuffer ?
                ByteBuffer.allocateDirect((int) maxSize) :
                ByteBuffer.allocate((int) maxSize);
    }

    @Override
    public void truncate(long size) {
        //TODO truncate
    }

    @Override
    synchronized public void close() {
        if (!closed.compareAndSet(false,true))
            return;

        if (cleanerHackEnabled && buffer instanceof MappedByteBuffer) {
            ByteBufferVol.unmap((MappedByteBuffer) buffer);
        }
        buffer = null;
    }

    @Override
    public void sync() {
    }

    @Override
    public long length() {
        return maxSize;
    }

    @Override
    public boolean isReadOnly() {
        return false;
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
