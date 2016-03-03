package org.mapdb.serializer;

import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by jan on 2/28/16.
 */
public abstract class SerializerFourByte<E> implements GroupSerializer<E> {

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
    public E valueArrayGet(Object vals, int pos) {
        return unpack(((int[]) vals)[pos]);
    }

    @Override
    public int valueArraySize(Object vals) {
        return ((int[]) vals).length;
    }

    @Override
    public Object valueArrayEmpty() {
        return new int[0];
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, E newValue) {

        int[] array = (int[]) vals;
        final int[] ret = Arrays.copyOf(array, array.length + 1);
        if (pos < array.length) {
            System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
        }
        ret[pos] = pack(newValue);
        return ret;
    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, E newValue) {
        int[] vals2 = ((int[]) vals).clone();
        vals2[pos] = pack(newValue);
        return vals2;
    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        int[] ret = new int[objects.length];
        int pos = 0;

        for (Object o : objects) {
            ret[pos++] = pack((E) o);
        }

        return ret;
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((int[]) vals, from, to);
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        int[] valsOrig = (int[]) vals;
        int[] vals2 = new int[valsOrig.length - 1];
        System.arraycopy(vals, 0, vals2, 0, pos - 1);
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.length - (pos - 1));
        return vals2;
    }


    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for (int o : (int[]) vals) {
            out.writeInt(o);
        }
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        int[] ret = new int[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readInt();
        }
        return ret;
    }

    @Override
    final public int valueArraySearch(Object keys, E key, Comparator comparator) {
        if (comparator == this)
            return valueArraySearch(keys, key);
        int[] array = (int[]) keys;

        int lo = 0;
        int hi = array.length - 1;

        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int compare = comparator.compare(key, unpack(array[mid]));

            if (compare == 0)
                return mid;
            else if (compare < 0)
                hi = mid - 1;
            else
                lo = mid + 1;
        }
        return -(lo + 1);
    }
}
