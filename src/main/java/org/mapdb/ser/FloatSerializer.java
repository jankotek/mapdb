package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class FloatSerializer extends FourByteSerializer<Float> {

    @Override
    protected Float unpack(int l) {
        return new Float(Float.intBitsToFloat(l));
    }

    @Override
    protected int pack(Float l) {
        return Float.floatToIntBits(l);
    }


    @Override
    public void serialize(DataOutput2 out, Float value) {
        out.writeFloat(value);
    }

    @Override
    public Float deserialize(DataInput2 in) {
        return new Float(in.readFloat());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Float.class;
    }


    @Override
    public int valueArraySearch(int[] keys, Float key) {
        //TODO PERF this can be optimized, but must take care of NaN
        return Arrays.binarySearch(valueArrayToArray(keys), key);
    }


}
