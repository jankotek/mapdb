package org.mapdb.serializer;

import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerLongDelta extends SerializerLong {
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
        long[] keys = (long[]) vals;
        long prev = keys[0];
        out.packLong(prev);
        for (int i = 1; i < keys.length; i++) {
            long curr = keys[i];
            //$DELAY$
            out.packLong(curr - prev);
            if (CC.ASSERT && curr < prev)
                throw new AssertionError("not sorted");
            prev = curr;
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        return in.unpackLongArrayDeltaCompression(size);
    }


    @Override
    public Long valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        long a = 0;
        while (pos-- >= 0) {
            a += input.unpackLong();
        }
        return a;
    }


    @Override
    public int fixedSize() {
        return -1;
    }
}
