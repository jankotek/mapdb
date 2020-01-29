package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class DoubleArraySerializer extends DefaultGroupSerializer<double[]> {

    @Override
    public void serialize(DataOutput2 out, double[] value) {
        out.packInt(value.length);
        for (double c : value) {
            out.writeDouble(c);
        }
    }

    @Override
    public double[] deserialize(DataInput2 in) {
        final int size = in.unpackInt();
        double[] ret = new double[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readDouble();
        }
        return ret;
    }

    @Nullable
    @Override
    public Class serializedType() {
        return double[].class;
    }


    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public boolean equals(double[] a1, double[] a2) {
        return Arrays.equals(a1, a2);
    }

    @Override
    public int hashCode(double[] bytes, int seed) {
        for (double element : bytes) {
            long bits = Double.doubleToLongBits(element);
            seed = (-1640531527) * seed + (int) (bits ^ (bits >>> 32));
        }
        return seed;
    }

    @Override
    public int compare(double[] o1, double[] o2) {
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


}
