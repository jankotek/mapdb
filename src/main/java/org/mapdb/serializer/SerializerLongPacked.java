package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerLongPacked extends SerializerLong {
    @Override
    public void serialize(DataOutput2 out, Long value) throws IOException {
        out.packLong(value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) throws IOException {
        return new Long(in.unpackLong());
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for (long o : (long[])vals) {
            out.packLong(o);
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        long[] ret = new long[size];
        in.unpackLongArray(ret, 0, size);
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
