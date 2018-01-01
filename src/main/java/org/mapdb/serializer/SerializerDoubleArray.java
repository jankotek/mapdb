package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.hasher.Hasher;
import org.mapdb.hasher.Hashers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerDoubleArray extends GroupSerializerObjectArray<double[]> {

    @Override
    public void serialize(DataOutput2 out, double[] value) throws IOException {
        out.packInt(value.length);
        for (double c : value) {
            out.writeDouble(c);
        }
    }

    @Override
    public double[] deserialize(DataInput2 in, int available) throws IOException {
        final int size = in.unpackInt();
        double[] ret = new double[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readDouble();
        }
        return ret;
    }


    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public Hasher<double[]> defaultHasher() {
        return Hashers.DOUBLE_ARRAY;
    }
}
