package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class SerializerInteger extends SerializerFourByte<Integer> {


    @Override
    public void serialize(DataOutput2 out, Integer value) throws IOException {
        out.writeInt(value);
    }

    @Override
    public Integer deserialize(DataInput2 in, int available) throws IOException {
        return new Integer(in.readInt());
    }
    @Override
    protected Integer unpack(int l) {
        return new Integer(l);
    }

    @Override
    protected int pack(Integer l) {
        return l;
    }

    @Override
    public int valueArraySearch(Object keys, Integer key) {
        return Arrays.binarySearch((int[]) keys, key);
    }

    @Override
    public Integer valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        int a = -Integer.MIN_VALUE;
        while (pos-- >= 0) {
            a = deserialize(input, -1);
        }
        return a;
    }

    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        int key2 = key;
        boolean notFound = true;
        for (int pos = 0; pos < keysLen; pos++) {
            int from = input.readInt();

            if (notFound && key2 <= from) {
                key2 = (key2 == from) ? pos : -(pos + 1);
                notFound = false;
            }
        }

        return notFound ? -(keysLen + 1) : key2;
    }
}
