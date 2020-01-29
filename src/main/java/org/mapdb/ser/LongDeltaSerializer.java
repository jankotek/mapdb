package org.mapdb.ser;

import org.mapdb.CC;
import org.mapdb.DBException;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class LongDeltaSerializer extends LongSerializer {
    @Override
    public void serialize(DataOutput2 out, Long value) {
        out.writePackedLong(value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) {
        return new Long(in.readPackedLong());
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, long[] vals) {
        long[] keys = (long[]) vals;
        long prev = keys[0];
        out.writePackedLong(prev);
        for (int i = 1; i < keys.length; i++) {
            long curr = keys[i];
            //$DELAY$
            out.writePackedLong(curr - prev);
            if (CC.ASSERT && curr < prev)
                throw new AssertionError("not sorted");
            prev = curr;
        }
    }

    @Override
    public long[] valueArrayDeserialize(DataInput2 in, int size) {
        throw new DBException.TODO("delta packing");
//        return in.unpackLongArrayDeltaCompression(size);
    }


    @Override
    public Long valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        long a = 0;
        while (pos-- >= 0) {
            a += input.readPackedLong();
        }
        return a;
    }

    @Override
    public int valueArrayBinarySearch(Long key, DataInput2 input, int keysLen, Comparator comparator) {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        long key2 = key;
        long from = 0;
        for (int pos = 0; pos < keysLen; pos++) {
            from += input.readPackedLong();

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
