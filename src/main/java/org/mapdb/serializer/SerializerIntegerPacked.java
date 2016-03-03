package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerIntegerPacked extends SerializerInteger {
    @Override
    public void serialize(DataOutput2 out, Integer value) throws IOException {
        out.packInt(value);
    }

    @Override
    public Integer deserialize(DataInput2 in, int available) throws IOException {
        return new Integer(in.unpackInt());
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for (int o : (int[])vals) {
            out.packIntBigger(o);
        }
    }

    @Override
    public int[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        int[] ret = new int[size];
        in.unpackIntArray(ret, 0, size);
        return ret;
    }

    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        int key2 = key;
        boolean notFound = true;
        for (int pos = 0; pos < keysLen; pos++) {
            int from = input.unpackInt();

            if (notFound && key2 <= from) {
                key2 = (key2 == from) ? pos : -(pos + 1);
                notFound = false;
            }
        }

        return notFound ? -(keysLen + 1) : key2;
    }


    @Override
    public int fixedSize() {
        return -1;
    }

}
