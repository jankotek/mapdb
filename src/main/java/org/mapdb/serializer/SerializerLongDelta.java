package org.mapdb.serializer;

import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

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
            if (CC.PARANOID && curr < prev)
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
    public int valueArrayBinarySearch(Long key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        long key2 = key;
        long from = 0;
        for (int pos = 0; pos < keysLen; pos++) {
            from += input.unpackLong();

            if (key2 <= from) {
                input.unpackLongSkip(keysLen-pos-1);
                return (key2 == from) ? pos : -(pos + 1);
            }
        }

        //not found
        return -(keysLen + 1);
    }


    @Override
    public int fixedSize() {
        return -1;
    }
}
