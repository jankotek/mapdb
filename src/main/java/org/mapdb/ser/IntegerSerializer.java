package org.mapdb.ser;

import org.jetbrains.annotations.Nullable;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

public class IntegerSerializer extends FourByteSerializer<Integer> {


    @Override
    public void serialize(DataOutput2 out, Integer value) {
        out.writeInt(value);
    }

    @Override
    public Integer deserialize(DataInput2 in) {
        return new Integer(in.readInt());
    }

    @Nullable
    @Override
    public Class serializedType() {
        return Integer.class;
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
    public int valueArraySearch(int[] keys, Integer key) {
        return Arrays.binarySearch((int[]) keys, key);
    }


    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) {
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
