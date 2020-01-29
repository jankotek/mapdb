package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class ShortArraySerializer extends DefaultGroupSerializer<short[]> {
    @Override
    public void serialize(DataOutput2 out, short[] value) {
        out.packInt(value.length);
        for (short v : value) {
            out.writeShort(v);
        }
    }

    @Override
    public short[] deserialize(DataInput2 in) {
        short[] ret = new short[in.unpackInt()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = in.readShort();
        }
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return short[].class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(short[] a1, short[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(short[] shorts, int seed) {
        for (short element : shorts)
            seed = (-1640531527) * seed + element;
        return seed;
    }

    @Override
    public int compare(short[] o1, short[] o2) {
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
    public short[] nextValue(short[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            short b1 = value[i];
            if(b1==Short.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Short.MIN_VALUE;
                continue;
            }
            value[i] = (short) (b1+1);
            return value;
        }
    }
}
