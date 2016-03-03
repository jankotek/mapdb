package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerDouble extends SerializerEightByte<Double> {
    @Override
    protected Double unpack(long l) {
        return new Double(Double.longBitsToDouble(l));
    }

    @Override
    protected long pack(Double l) {
        return Double.doubleToLongBits(l);
    }

    @Override
    public int valueArraySearch(Object keys, Double key) {
        //TODO PERF this can be optimized, but must take care of NaN
        return Arrays.binarySearch(valueArrayToArray(keys), key);
    }

    @Override
    public void serialize(DataOutput2 out, Double value) throws IOException {
        out.writeDouble(value);
    }

    @Override
    public Double deserialize(DataInput2 in, int available) throws IOException {
        return new Double(in.readDouble());
    }

}
