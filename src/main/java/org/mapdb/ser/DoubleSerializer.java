package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class DoubleSerializer extends EightByteSerializer<Double> {
    @Override
    protected Double unpack(long l) {
        return new Double(Double.longBitsToDouble(l));
    }

    @Override
    protected long pack(Double l) {
        return Double.doubleToLongBits(l);
    }

    @Override
    public int valueArraySearch(long[] keys, Double key) {
        //TODO PERF this can be optimized, but must take care of NaN
        return Arrays.binarySearch(valueArrayToArray(keys), key);
    }

    @Override
    public void serialize(DataOutput2 out, Double value) {
        out.writeDouble(value);
    }

    @Override
    public Double deserialize(DataInput2 in) {
        return new Double(in.readDouble());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Double.class;
    }

}
