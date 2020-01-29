package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public class IntegerPackedSerializer extends IntegerSerializer {
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
        for (int o : (int[])vals) {
            out.packInt(o); //TODO packIntBigger()
        }
    }

    @Override
    public int[] valueArrayDeserialize(DataInput2 in, int size) {
        int[] ret = new int[size];
        //TODO int arrays
//        in.unpackIntArray(ret, 0, size);
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }

    @Override
    public int valueArrayBinarySearch(Integer key, DataInput2 input, int keysLen, Comparator comparator) {
        if (comparator != this)
            return super.valueArrayBinarySearch(key, input, keysLen, comparator);
        int key2 = key;
        for (int pos = 0; pos < keysLen; pos++) {
            int from = input.unpackInt();

            if (key2 <= from) {
                input.unpackLongSkip(keysLen-pos-1);
                return (key2 == from) ? pos : -(pos + 1);
            }
        }

        //not found
        return -(keysLen + 1);
    }

    @Override
    public Integer valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        input.unpackLongSkip(pos);
        return input.unpackInt();
    }

}
