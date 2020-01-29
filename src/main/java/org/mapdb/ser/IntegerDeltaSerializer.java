package org.mapdb.ser;

import org.mapdb.CC;
import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class IntegerDeltaSerializer extends IntegerSerializer {
    @Override
    public void serialize(DataOutput2 out, Integer value) {
        out.packInt(value);
    }

    @Override
    public Integer deserialize(DataInput2 in, int available) {
        return new Integer(in.unpackInt());
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, int[] vals) {
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
    public int[] valueArrayDeserialize(DataInput2 in, int size) {
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
    public Integer valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        int a = 0;
        while (pos-- >= 0) {
            a += input.unpackInt();
        }
        return a;
    }


    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        int key2 = key;
        int from = 0;
        for (int pos = 0; pos < keysLen; pos++) {
            from += input.unpackInt();

            if (key2 <= from) {
                input.unpackLongSkip(keysLen-pos-1);
                return (key2 == from) ? pos : -(pos + 1);
            }
        }

        //not found
        return -(keysLen + 1);
    }

    @Override
    public int fixedSize() {
        return -1;
    }

}
