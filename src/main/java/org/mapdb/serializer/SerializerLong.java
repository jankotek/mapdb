package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerLong extends SerializerEightByte<Long> {

    @Override
    public void serialize(DataOutput2 out, Long value) throws IOException {
        out.writeLong(value);
    }

    @Override
    public Long deserialize(DataInput2 in, int available) throws IOException {
        return new Long(in.readLong());
    }


    @Override
    protected Long unpack(long l) {
        return new Long(l);
    }

    @Override
    protected long pack(Long l) {
        return l.longValue();
    }

    @Override
    public int valueArraySearch(Object keys, Long key) {
        return Arrays.binarySearch((long[])keys, key);
    }


    @Override
    public int valueArrayBinarySearch(Long key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        long key2 = key;
        for (int pos = 0; pos < keysLen; pos++) {
            long from = input.readLong();

            if (key2 <= from) {
                input.skipBytes((keysLen-pos-1)*8);
                return (key2 == from) ? pos : -(pos + 1);
            }
        }

        //not found
        return -(keysLen + 1);
    }

}
