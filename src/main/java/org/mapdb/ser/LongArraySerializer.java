package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class LongArraySerializer extends DefaultGroupSerializer<long[]> {

    @Override
    public void serialize(DataOutput2 out, long[] value) {
        out.packInt(value.length);
        for (long c : value) {
            out.writeLong(c);
        }
    }

    @Override
    public long[] deserialize(DataInput2 in) {
        final int size = in.unpackInt();
        long[] ret = new long[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readLong();
        }
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return long[].class;
    }


    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(long[] a1, long[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(long[] bytes, int seed) {
        for (long element : bytes) {
            int elementHash = (int) (element ^ (element >>> 32));
            seed = (-1640531527) * seed + elementHash;
        }
        return seed;
    }

    @Override
    public int compare(long[] o1, long[] o2) {
        if (o1 == o2) return 0;
        final int len = Math.min(o1.length, o2.length);
        for (int i = 0; i < len; i++) {
            if (o1[i] == o2[i])
                continue;
            if (o1[i] > o2[i])
                return 1;
            return -1;
        }
        return SerializerUtils.compareInt(o1.length, o2.length);
    }

    @Override
    public long[] nextValue(long[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            long b1 = value[i];
            if(b1==Long.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Long.MIN_VALUE;
                continue;
            }
            value[i] = b1+1L;
            return value;
        }
    }
}
