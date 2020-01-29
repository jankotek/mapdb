package org.mapdb.ser;

import org.mapdb.io.DataInput2;
import org.mapdb.io.DataOutput2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public abstract class FourByteSerializer<E> implements GroupSerializer<E, int[]> {

    protected abstract E unpack(int l);

    protected abstract int pack(E l);

    @Override
    public boolean isTrusted() {
        return true;
    }

    @Override
    public int fixedSize() {
        return 4;
    }

    @Override
    public E valueArrayGet(int[] vals, int pos) {
        return unpack(((int[]) vals)[pos]);
    }

    @Override
    public int valueArraySize(int[] vals) {
        return ((int[]) vals).length;
    }

    @Override
    public int[] valueArrayEmpty() {
        return new int[0];
    }

    @Override
    public int[] valueArrayPut(int[] vals, int pos, E newValue) {

        final int[] ret = Arrays.copyOf(vals, vals.length + 1);
        if (pos < vals.length) {
            System.arraycopy(vals, pos, ret, pos + 1, vals.length - pos);
        }
        ret[pos] = pack(newValue);
        return ret;
    }

    @Override
    public int[] valueArrayUpdateVal(int[] vals, int pos, E newValue) {
        int[] vals2 = ((int[]) vals).clone();
        vals2[pos] = pack(newValue);
        return vals2;
    }

    @Override
    public int[] valueArrayFromArray(Object[] objects) {
        int[] ret = new int[objects.length];
        int pos = 0;

        for (Object o : objects) {
            ret[pos++] = pack((E) o);
        }

        return ret;
    }

    @Override
    public int[] valueArrayCopyOfRange(int[] vals, int from, int to) {
        return Arrays.copyOfRange((int[]) vals, from, to);
    }

    @Override
    public int[] valueArrayDeleteValue(int[] vals, int pos) {
        int[] valsOrig = (int[]) vals;
        int[] vals2 = new int[valsOrig.length - 1];
        System.arraycopy(vals, 0, vals2, 0, pos - 1);
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.length - (pos - 1));
        return vals2;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out, int[] vals) {
        for (int o : (int[]) vals) {
            out.writeInt(o);
        }
    }

    @Override
    public int[] valueArrayDeserialize(DataInput2 in, int size) {
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readInt();
        }
        return ret;
    }

    @Override
    final public int valueArraySearch(int[] keys, E key, Comparator comparator) {
        if (comparator == this)
            return valueArraySearch(keys, key);

        int lo = 0;
        int hi = keys.length - 1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int compare = comparator.compare(key, unpack(keys[mid]));

            if (compare == 0)
                return mid;
            else if (compare < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -(lo + 1);
    }

    @Override
    public E valueArrayBinaryGet(DataInput2 input, int keysLen, int pos) {
        input.skipBytes(pos*4);
        return unpack(input.readInt());
    }

}
