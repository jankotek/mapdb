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
public class SerializerBoolean implements GroupSerializer<Boolean> {

    @Override
    public void serialize(DataOutput2 out, Boolean value) throws IOException {
        out.writeBoolean(value);
    }

    @Override
    public Boolean deserialize(DataInput2 in, int available) throws IOException {
        return in.readBoolean();
    }

    @Override
    public int fixedSize() {
        return 1;
    }

    @Override
    public boolean isTrusted() {
        return true;
    }


    @Override
    public int valueArraySearch(Object keys, Boolean key) {
        return Arrays.binarySearch(valueArrayToArray(keys), key);
    }

    @Override
    public int valueArraySearch(Object keys, Boolean key, Comparator comparator) {
        return Arrays.binarySearch(valueArrayToArray(keys), key, comparator);
    }

    @Override
    public void valueArraySerialize(DataOutput2 out, Object vals) throws IOException {
        for (boolean b : ((boolean[]) vals)) {
            out.writeBoolean(b);
        }
    }

    @Override
    public Object valueArrayDeserialize(DataInput2 in, int size) throws IOException {
        boolean[] ret = new boolean[size];
        for (int i = 0; i < size; i++) {
            ret[i] = in.readBoolean();
        }
        return ret;
    }

    @Override
    public Boolean valueArrayGet(Object vals, int pos) {
        return ((boolean[]) vals)[pos];
    }

    @Override
    public int valueArraySize(Object vals) {
        return ((boolean[]) vals).length;
    }

    @Override
    public Object valueArrayEmpty() {
        return new boolean[0];
    }

    @Override
    public Object valueArrayPut(Object vals, int pos, Boolean newValue) {
        boolean[] array = (boolean[]) vals;
        final boolean[] ret = Arrays.copyOf(array, array.length + 1);
        if (pos < array.length) {
            System.arraycopy(array, pos, ret, pos + 1, array.length - pos);
        }
        ret[pos] = newValue;
        return ret;

    }

    @Override
    public Object valueArrayUpdateVal(Object vals, int pos, Boolean newValue) {
        boolean[] vals2 = ((boolean[]) vals).clone();
        vals2[pos] = newValue;
        return vals2;

    }

    @Override
    public Object valueArrayFromArray(Object[] objects) {
        boolean[] ret = new boolean[objects.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = (Boolean) objects[i];
        }
        return ret;
    }

    @Override
    public Object valueArrayCopyOfRange(Object vals, int from, int to) {
        return Arrays.copyOfRange((boolean[]) vals, from, to);
    }

    @Override
    public Object valueArrayDeleteValue(Object vals, int pos) {
        boolean[] valsOrig = (boolean[]) vals;
        boolean[] vals2 = new boolean[valsOrig.length - 1];
        System.arraycopy(vals, 0, vals2, 0, pos - 1);
        System.arraycopy(vals, pos, vals2, pos - 1, vals2.length - (pos - 1));
        return vals2;

    }
}
