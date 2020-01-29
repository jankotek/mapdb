package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class IntArraySerializer extends DefaultGroupSerializer<int[]> {

    @Override
    public void serialize(DataOutput2 out, int[] value) {
        out.packInt(value.length);
        for (int c : value) {
            out.writeInt(c);
        }
    }

    @Override
    public int[] deserialize(DataInput2 in) {
        final int size = in.unpackInt();
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readInt();
        }
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return int[].class;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(int[] a1, int[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(int[] bytes, int seed) {
        for (int i : bytes) {
            seed = (-1640531527) * seed + i;
        }
        return seed;
    }

    @Override
    public int compare(int[] o1, int[] o2) {
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
    public int[] nextValue(int[] value) {
        value = value.clone();

        for (int i = value.length-1; ;i--) {
            int b1 = value[i];
            if(b1==Integer.MAX_VALUE){
                if(i==0)
                    return null;
                value[i]=Integer.MIN_VALUE;
                continue;
            }
            value[i] = b1+1;
            return value;
        }
    }

}
