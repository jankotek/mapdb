package org.mapdb.io;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;

/**
 * Wraps {@code DataInput} into {@code InputStream}
 */
public final class DataInputToStream extends InputStream {

    protected final DataInput2 in;

    public DataInputToStream(DataInput2 in) {
        this.in = in;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        in.readFully(b,off,len);
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        n = Math.min(n, Integer.MAX_VALUE);
        //$DELAY$
        return in.skipBytes((int) n);
    }

    @Override
    public void close() throws IOException {
        if(in instanceof Closeable)
            ((Closeable) in).close();
    }

    @Override
    public int read() throws IOException {
        return in.readUnsignedByte();
    }
}
