package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerLong extends SerializerEightByte<Long> {

    @Override
    public void serialize(DataOutput2 out, Long value) throws IOException {
        out.writeLong(value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) throws IOException {
        return new Long(in.readLong());
    }


    @Override
    protected Long unpack(long l) {
        return new Long(l);
    }

    @Override
    protected long pack(Long l) {
        return l.longValue();
    }

    @Override
    public int valueArraySearch(Object keys, Long key) {
        return Arrays.binarySearch((long[])keys, key);
    }


}
