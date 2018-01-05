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
public class SerializerFloatArray implements Serializer<float[]> {
    @Override
    public void serialize(DataOutput2 out, float[] value) throws IOException {
        out.packInt(value.length);
        for (float v : value) {
            out.writeFloat(v);
        }
    }

    @Override
    public float[] deserialize(DataInput2 in, int available) throws IOException {
        float[] ret = new float[in.unpackInt()];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = in.readFloat();
        }
        return ret;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public Hasher<float[]> defaultHasher() {
        return Hashers.FLOAT_ARRAY;
    }
}
