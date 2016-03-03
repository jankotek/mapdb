package org.mapdb.serializer;

import org.mapdb.CC;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class SerializerIntegerDelta extends SerializerInteger {
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
        int[] keys = (int[]) vals;
        int prev = keys[0];
        out.packInt(prev);
        for (int i = 1; i < keys.length; i++) {
            int curr = keys[i];
            //$DELAY$
            out.packInt(curr - prev);
            if (CC.ASSERT && curr < prev)
                throw new AssertionError("not sorted");
            prev = curr;
        }
    }

    @Override
    public int[] valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        int[] ret = new int[size];
        int prev = 0;
        for (int i = 0; i < size; i++) {
            //$DELAY$
            prev += in.unpackInt();
            ret[i] = prev;
        }
        return ret;
    }


    @Override
    public Integer valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) throws IOException {
        int a = 0;
        while (pos-- >= 0) {
            a += input.unpackInt();
        }
        return a;
    }

    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) throws IOException {
        int[] keys = valueArrayDeserialize(input, keysLen);
        return valueArraySearch(keys, key, comparator);
    }


    @Override
    public int fixedSize() {
        return -1;
    }

}
