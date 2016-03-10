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
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        final int key2 = key;
        for (int pos = 0; pos < keysLen; pos++) {
            int from = input.readInt();

            if (key2 <= from) {
                input.skipBytes((keysLen-pos-1)*4);
                return (key2 == from) ? pos : -(pos + 1);
            }
        }

        //not found
        return -(keysLen + 1);
    }
}
